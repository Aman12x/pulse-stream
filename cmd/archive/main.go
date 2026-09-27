// Command archive copies events from Kafka into Parquet files, one file per
// partition offset range, committing Kafka offsets only after each file is in
// place (at-least-once; see internal/archive).
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/Aman12x/pulse-stream/internal/archive"
	"github.com/Aman12x/pulse-stream/internal/codec"
)

var (
	rowsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulse_archive_rows_total", Help: "Rows written to Parquet."})
	filesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulse_archive_files_total", Help: "Parquet files written."})
	undecodableTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulse_archive_undecodable_total", Help: "Records skipped because they could not be decoded."})
)

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return d
}

type buffer struct {
	rows    []archive.Row
	started time.Time
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("archive stopped", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	var (
		topic    = env("KAFKA_TOPIC", "bsky.events")
		dir      = env("ARCHIVE_DIR", "archive")
		maxRows  = envInt("FLUSH_ROWS", 200_000)
		maxAge   = time.Duration(envInt("FLUSH_MINUTES", 10)) * time.Minute
		groupID  = env("CONSUMER_GROUP", "pulse-archive")
		pollSize = envInt("MAX_POLL_RECORDS", 5000)
	)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	bufs := map[int32]*buffer{}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ",")...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		// A revoked partition's unflushed rows are dropped; its next owner re-reads
		// them from the last committed offset.
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			for _, p := range m[topic] {
				delete(bufs, p)
			}
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			for _, p := range m[topic] {
				delete(bufs, p)
			}
		}),
	)
	if err != nil {
		return err
	}
	defer cl.CloseAllowingRebalance()
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		_ = http.ListenAndServe(env("METRICS_ADDR", ":9121"), mux)
	}()

	flush := func(c context.Context, p int32) error {
		b := bufs[p]
		if b == nil || len(b.rows) == 0 {
			return nil
		}
		first, last := b.rows[0], b.rows[len(b.rows)-1]
		path := archive.Path(dir, topic, p, first.KafkaOffset, last.KafkaOffset, first.TimeUS)
		if err := archive.Write(path, b.rows); err != nil {
			return err
		}
		var commitErr error
		cl.CommitOffsetsSync(c, map[string]map[int32]kgo.EpochOffset{
			topic: {p: {Epoch: -1, Offset: last.KafkaOffset + 1}},
		}, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				commitErr = err
				return
			}
			for _, t := range resp.Topics {
				for _, pt := range t.Partitions {
					if e := kerr.ErrorForCode(pt.ErrorCode); e != nil {
						commitErr = e
					}
				}
			}
		})
		if commitErr != nil {
			// The file is in place but the offset is not committed: the range will be
			// re-read and rewritten, which the reader's dedupe absorbs.
			return commitErr
		}
		rowsTotal.Add(float64(len(b.rows)))
		filesTotal.Inc()
		log.Info("archived", "partition", p, "rows", len(b.rows), "file", path)
		delete(bufs, p)
		return nil
	}

	for {
		fs := cl.PollRecords(ctx, pollSize)
		if ctx.Err() != nil {
			sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			for p := range bufs {
				if err := flush(sctx, p); err != nil {
					return err
				}
			}
			return ctx.Err()
		}
		fs.EachError(func(_ string, p int32, err error) { log.Warn("fetch error", "partition", p, "err", err) })
		fs.EachRecord(func(r *kgo.Record) {
			ev, _, err := codec.Decode(r.Value)
			if err != nil {
				undecodableTotal.Inc()
				return
			}
			b := bufs[r.Partition]
			if b == nil {
				b = &buffer{started: time.Now()}
				bufs[r.Partition] = b
			}
			b.rows = append(b.rows, archive.FromEvent(ev, r.Partition, r.Offset))
		})
		for p, b := range bufs {
			if len(b.rows) >= maxRows || time.Since(b.started) >= maxAge {
				if err := flush(ctx, p); err != nil {
					return err
				}
			}
		}
		cl.AllowRebalance()
	}
}
