// Command ingest reads Bluesky's Jetstream feed and produces normalized events to Kafka.
//
// Delivery contract: at-least-once. A cursor is checkpointed only after every event
// up to it has been acknowledged by Kafka, and a restart resumes from that
// checkpoint minus a rewind window. A crash therefore replays events (duplicates,
// which the consumers deduplicate) but never skips any.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Aman12x/pulse-stream/internal/checkpoint"
	"github.com/Aman12x/pulse-stream/internal/codec"
	"github.com/Aman12x/pulse-stream/internal/ingest"
	"github.com/Aman12x/pulse-stream/internal/jetstream"
	protoschema "github.com/Aman12x/pulse-stream/proto"
)

var (
	eventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulse_ingest_events_total", Help: "Events produced to Kafka, by kind."}, []string{"ingester", "kind"})
	malformedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulse_ingest_malformed_total", Help: "Messages that failed to decode or normalize."}, []string{"ingester"})
	reconnectsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulse_ingest_reconnects_total", Help: "Jetstream reconnects."}, []string{"ingester"})
	checkpointUS = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pulse_ingest_checkpoint_time_us", Help: "Last checkpointed Jetstream cursor."}, []string{"ingester"})
	repairsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulse_ingest_repairs_total", Help: "Reconnect handoff windows replayed and produced."}, []string{"ingester"})
	repairFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulse_ingest_repair_failures_total", Help: "Repair replays that failed every attempt; the checkpoint stays held."}, []string{"ingester"})
	repairEventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulse_ingest_repair_events_total", Help: "Events re-read by repair replays (mostly duplicates)."}, []string{"ingester"})
	lagSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pulse_ingest_lag_seconds", Help: "Wall clock minus the newest acknowledged event time."}, []string{"ingester"})
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
		log.Error("ingest stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	var (
		id       = env("INGEST_ID", "main")
		topic    = env("KAFKA_TOPIC", "bsky.events")
		brokers  = strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")
		endpoint = env("JETSTREAM_URL", "wss://jetstream2.us-east.bsky.network/subscribe")
		salt     = os.Getenv("HASH_SALT")
		batchMax = envInt("BATCH_SIZE", 500)
		rewindUS = int64(envInt("REWIND_SECONDS", 5)) * 1_000_000
		repairUS = int64(envInt("REPAIR_WINDOW_SECONDS", 5)) * 1_000_000
		repairIn = time.Duration(envInt("REPAIR_DELAY_SECONDS", 30)) * time.Second
	)
	format := env("FORMAT", "protobuf")
	if salt == "" {
		return errors.New("HASH_SALT is required (account ids are salted hashes)")
	}
	log = log.With("ingester", id)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := checkpoint.Open(ctx, env("PG_DSN", "postgres://pulse:pulse@localhost:5433/pulse?sslmode=disable"))
	if err != nil {
		return err
	}
	defer store.Close()

	kc, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()), // idempotent producer (franz-go default) requires all-ISR acks
		kgo.ProducerLinger(5*time.Millisecond),
	)
	if err != nil {
		return err
	}
	defer kc.Close()
	if err := ensureTopic(ctx, kc, topic); err != nil {
		return err
	}

	// Protobuf events carry the registry id of the schema this binary was built with.
	schemaID := 0
	if format == "protobuf" {
		schemaID, err = codec.Register(ctx, env("SCHEMA_REGISTRY_URL", "http://localhost:8081"), topic+"-value", protoschema.EventProto)
		if err != nil {
			return err
		}
		log.Info("schema registered", "subject", topic+"-value", "schema_id", schemaID)
	} else if format != "json" {
		return errors.New("FORMAT must be protobuf or json")
	}

	saved, err := store.Load(ctx, id)
	if err != nil {
		return err
	}
	var acked atomic.Int64 // newest event time Kafka has acknowledged
	acked.Store(saved)
	log.Info("starting", "checkpoint_us", saved, "resume_cursor", ingest.ResumeCursor(saved, rewindUS))

	go serveMetrics(log, env("METRICS_ADDR", ":9101"))

	msgs := make(chan jetstream.Message, 4096)
	client := &jetstream.Client{
		Endpoint:    endpoint,
		Log:         log,
		OnReconnect: func() { reconnectsTotal.WithLabelValues(id).Inc() },
	}
	streamErr := make(chan error, 1)
	go func() {
		streamErr <- client.Run(ctx, func() int64 { return ingest.ResumeCursor(acked.Load(), rewindUS) }, msgs)
	}()

	// Repair: replay the window around each cursor reconnect once it is history.
	// See ingest.Holds for why the checkpoint waits for it.
	holds := ingest.NewHolds()
	repair := func(connectedUS, hold int64) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(repairIn):
		}
		for attempt := 1; attempt <= 3; attempt++ {
			n, err := client.ReadRange(ctx, connectedUS-repairUS, connectedUS+repairUS, msgs)
			if err == nil {
				repairEventsTotal.WithLabelValues(id).Add(float64(n))
				select {
				case msgs <- jetstream.Message{RepairedUS: hold}:
				case <-ctx.Done():
				}
				return
			}
			if ctx.Err() != nil {
				return
			}
			log.Warn("repair replay failed", "attempt", attempt, "err", err)
			time.Sleep(time.Duration(attempt) * 5 * time.Second)
		}
		repairFailuresTotal.WithLabelValues(id).Inc()
		log.Error("repair gave up; checkpoint stays held until restart", "connected_us", connectedUS)
	}

	hasher := jetstream.NewHasher(salt)
	batcher := ingest.NewBatcher(batchMax)
	kinds := map[string]int{}

	flush := func() error {
		batch := batcher.Take()
		if len(batch.Records) == 0 {
			return nil
		}
		recs := make([]*kgo.Record, len(batch.Records))
		for i, r := range batch.Records {
			recs[i] = &kgo.Record{Key: r.Key, Value: r.Value}
		}
		if err := kc.ProduceSync(ctx, recs...).FirstErr(); err != nil {
			// Exit instead of retrying in place: the supervisor restarts us and we
			// resume from the last checkpoint, which is always safe.
			return err
		}
		if batch.MaxTimeUS > acked.Load() {
			acked.Store(batch.MaxTimeUS)
		}
		for k, n := range kinds {
			eventsTotal.WithLabelValues(id, k).Add(float64(n))
		}
		clear(kinds)
		lagSeconds.WithLabelValues(id).Set(time.Since(time.UnixMicro(acked.Load())).Seconds())
		return nil
	}

	flushTick := time.NewTicker(250 * time.Millisecond)
	defer flushTick.Stop()
	saveTick := time.NewTicker(2 * time.Second)
	defer saveTick.Stop()

	save := func(c context.Context) error {
		cur := holds.Cap(acked.Load())
		if cur == 0 {
			return nil
		}
		if err := store.Save(c, id, cur); err != nil {
			return err
		}
		checkpointUS.WithLabelValues(id).Set(float64(cur))
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			// Graceful stop: flush and checkpoint with a fresh context.
			sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			ctx = sctx
			if err := flush(); err != nil {
				return err
			}
			return save(sctx)
		case err := <-streamErr:
			return err
		case m := <-msgs:
			if m.ConnectedUS > 0 {
				hold := m.ConnectedUS - repairUS
				holds.Add(hold)
				go repair(m.ConnectedUS, hold)
				continue
			}
			if m.RepairedUS > 0 {
				// Every repaired event is already in the channel ahead of this marker;
				// produce them before letting the checkpoint move past the handoff.
				if err := flush(); err != nil {
					return err
				}
				holds.Release(m.RepairedUS)
				repairsTotal.WithLabelValues(id).Inc()
				continue
			}
			var r jetstream.RawEvent
			if err := json.Unmarshal(m.Raw, &r); err != nil {
				malformedTotal.WithLabelValues(id).Inc()
				continue
			}
			ev, err := hasher.Normalize(r)
			if err != nil {
				malformedTotal.WithLabelValues(id).Inc()
				continue
			}
			var val []byte
			if format == "json" {
				val, _ = json.Marshal(ev)
			} else if val, err = codec.Encode(schemaID, ev); err != nil {
				return err
			}
			kinds[ev.Kind]++
			if batcher.Add([]byte(ev.AccountID), val, ev.TimeUS) {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-flushTick.C:
			if err := flush(); err != nil {
				return err
			}
		case <-saveTick.C:
			if err := save(ctx); err != nil {
				log.Warn("checkpoint save failed", "err", err)
			}
		}
	}
}

func ensureTopic(ctx context.Context, kc *kgo.Client, topic string) error {
	adm := kadm.NewClient(kc)
	resp, err := adm.CreateTopic(ctx, 6, 1, map[string]*string{"retention.ms": ptr("604800000")}, topic)
	if err == nil {
		err = resp.Err
	}
	if err != nil && !strings.Contains(err.Error(), "TOPIC_ALREADY_EXISTS") {
		return err
	}
	return nil
}

func ptr(s string) *string { return &s }

func serveMetrics(log *slog.Logger, addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Error("metrics server", "err", err)
	}
}
