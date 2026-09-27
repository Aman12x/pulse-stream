// Command consume reads normalized events from Kafka and applies them to Postgres
// exactly once.
//
// Offsets live in Postgres, not Kafka: each partition's batch is applied in one
// transaction that also advances that partition's stored offset, so a crash
// either keeps a whole batch or none of it. On assignment a consumer claims the
// partition, which bumps a generation number and fences every earlier owner, and
// resumes from the stored offset. Rebalances are blocked while a batch is being
// applied (BlockRebalanceOnPoll), so a live member never loses a partition
// mid-batch; a frozen or partitioned member that is kicked out is caught by the
// generation check instead.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Aman12x/pulse-stream/internal/codec"
	"github.com/Aman12x/pulse-stream/internal/consume"
)

var (
	recordsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulse_consume_records_total", Help: "Records applied, by outcome."}, []string{"consumer", "outcome"})
	fencedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulse_consume_fenced_total", Help: "Batches rejected because a newer owner claimed the partition."}, []string{"consumer"})
	retriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulse_consume_tx_retries_total", Help: "Transactions retried after a deadlock or serialization failure."}, []string{"consumer"})
	decodedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulse_consume_decoded_total", Help: "Records decoded, by wire format and schema id."}, []string{"consumer", "format", "schema_id"})
	applySeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "pulse_consume_apply_seconds", Help: "Time to apply one partition batch.", Buckets: prometheus.DefBuckets}, []string{"consumer"})
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("consume stopped", "err", err)
		os.Exit(1)
	}
}

type owner struct {
	mu  sync.Mutex
	gen map[int32]int64
}

func (o *owner) get(p int32) (int64, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	g, ok := o.gen[p]
	return g, ok
}

func (o *owner) set(p int32, g int64) { o.mu.Lock(); o.gen[p] = g; o.mu.Unlock() }
func (o *owner) drop(ps []int32) {
	o.mu.Lock()
	for _, p := range ps {
		delete(o.gen, p)
	}
	o.mu.Unlock()
}

func run(log *slog.Logger) error {
	var (
		id       = env("CONSUMER_ID", "c1")
		group    = env("CONSUMER_GROUP", "pulse-consume")
		topic    = env("KAFKA_TOPIC", "bsky.events")
		schema   = env("PG_SCHEMA", "pulse")
		maxRecs  = envInt("MAX_POLL_RECORDS", 2000)
		delay    = time.Duration(envInt("PROCESS_DELAY_MS", 0)) * time.Millisecond // test knob: slows batches so crash tests have time to happen
		lateSec  = envInt("LATE_SECONDS", 300)
		gapSec   = envInt("SESSION_GAP_SECONDS", 1800)
		sessTOms = envInt("SESSION_TIMEOUT_MS", 10000)
	)
	log = log.With("consumer", id)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := consume.Open(ctx, env("PG_DSN", "postgres://pulse:pulse@localhost:5433/pulse?sslmode=disable"), schema,
		consume.Config{LateUS: int64(lateSec) * 1_000_000, GapUS: int64(gapSec) * 1_000_000})
	if err != nil {
		return err
	}
	defer store.Close()

	owned := &owner{gen: map[int32]int64{}}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.SessionTimeout(time.Duration(sessTOms)*time.Millisecond),
		kgo.HeartbeatInterval(time.Second),
		// Claim each newly assigned partition in Postgres and start from the offset
		// stored there. Kafka's committed offsets are never used.
		kgo.AdjustFetchOffsetsFn(func(ctx context.Context, offs map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
			for t, parts := range offs {
				for p := range parts {
					gen, next, err := store.Claim(ctx, t, p)
					if err != nil {
						return nil, err
					}
					owned.set(p, gen)
					parts[p] = kgo.NewOffset().At(next)
					log.Info("claimed", "partition", p, "generation", gen, "next_offset", next)
				}
			}
			return offs, nil
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, m map[string][]int32) { owned.drop(m[topic]) }),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, m map[string][]int32) { owned.drop(m[topic]) }),
	)
	if err != nil {
		return err
	}
	// Plain Close would hang: leaving the group needs a rebalance, and the last
	// poll before shutdown left rebalancing blocked (BlockRebalanceOnPoll).
	defer cl.CloseAllowingRebalance()

	go serveMetrics(log, env("METRICS_ADDR", ":9111"))

	for {
		fs := cl.PollRecords(ctx, maxRecs)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fs.EachError(func(t string, p int32, err error) { log.Warn("fetch error", "partition", p, "err", err) })

		byPart := map[int32][]consume.Record{}
		fs.EachRecord(func(r *kgo.Record) {
			rec := consume.Record{Offset: r.Offset}
			ev, info, err := codec.Decode(r.Value)
			if err != nil {
				rec.Malformed = true
				decodedTotal.WithLabelValues(id, "malformed", "").Inc()
				log.Warn("undecodable record", "partition", r.Partition, "offset", r.Offset, "err", err)
			} else {
				rec.Event = ev
				decodedTotal.WithLabelValues(id, string(info.Format), strconv.Itoa(info.SchemaID)).Inc()
			}
			byPart[r.Partition] = append(byPart[r.Partition], rec)
		})

		parts := make([]int32, 0, len(byPart))
		for p := range byPart {
			parts = append(parts, p)
		}
		slices.Sort(parts)
		for _, p := range parts {
			gen, ok := owned.get(p)
			if !ok {
				continue // revoked since the fetch; the new owner will read these again
			}
			st, err := applyWithRetry(ctx, store, topic, p, gen, byPart[p], id)
			if errors.Is(err, consume.ErrFenced) {
				fencedTotal.WithLabelValues(id).Inc()
				log.Warn("fenced", "partition", p, "generation", gen)
				owned.drop([]int32{p})
				continue
			}
			if err != nil {
				return err
			}
			recordsTotal.WithLabelValues(id, "new").Add(float64(st.New))
			recordsTotal.WithLabelValues(id, "duplicate").Add(float64(st.Duplicates))
			recordsTotal.WithLabelValues(id, "late").Add(float64(st.Late))
			recordsTotal.WithLabelValues(id, "redelivered").Add(float64(st.Skipped))
		}
		if delay > 0 {
			time.Sleep(delay)
		}
		cl.AllowRebalance()
	}
}

func applyWithRetry(ctx context.Context, s *consume.Store, topic string, p int32, gen int64, recs []consume.Record, id string) (consume.ApplyStats, error) {
	for attempt := 1; ; attempt++ {
		start := time.Now()
		st, err := s.Apply(ctx, topic, p, gen, recs)
		applySeconds.WithLabelValues(id).Observe(time.Since(start).Seconds())
		var pgErr *pgconn.PgError
		if err != nil && attempt < 5 && errors.As(err, &pgErr) && (pgErr.Code == "40P01" || pgErr.Code == "40001") {
			retriesTotal.WithLabelValues(id).Inc()
			time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
			continue
		}
		return st, err
	}
}

func serveMetrics(log *slog.Logger, addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Error("metrics server", "err", err)
	}
}
