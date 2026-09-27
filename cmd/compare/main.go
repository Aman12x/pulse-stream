// Command compare checks stage 2's exactly-once claim.
//
//	compare wait -schema S -topic T          block until S has applied every offset of T
//	compare diff -a A -b B [-window ...]     row-by-row diff of every output table
//
// diff reports, per table, rows present in one schema and not the other. With
// -ref, it also compares minute aggregates from a reference schema over the
// minutes fully inside a window, which checks deduplication against a topic that
// never had duplicates.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: compare wait|diff [flags]")
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, envOr("PG_DSN", "postgres://pulse:pulse@localhost:5433/pulse?sslmode=disable"))
	must(err)
	defer pool.Close()
	switch os.Args[1] {
	case "wait":
		wait(ctx, pool, os.Args[2:])
	case "diff":
		diff(ctx, pool, os.Args[2:])
	default:
		must(fmt.Errorf("unknown command %q", os.Args[1]))
	}
}

func wait(ctx context.Context, pool *pgxpool.Pool, args []string) {
	fs := flag.NewFlagSet("wait", flag.ExitOnError)
	schema := fs.String("schema", "", "schema")
	topic := fs.String("topic", "", "topic")
	brokers := fs.String("brokers", "localhost:9092", "brokers")
	must(fs.Parse(args))
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...))
	must(err)
	defer cl.Close()
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, *topic)
	must(err)
	var want int64
	ends.Each(func(o kadm.ListedOffset) { want += o.Offset })
	q := fmt.Sprintf("SELECT coalesce(sum(next_offset),0) FROM %s.partition_state WHERE topic = $1", ident(*schema))
	for {
		var got int64
		if err := pool.QueryRow(ctx, q, *topic).Scan(&got); err == nil && got >= want {
			fmt.Printf("%s caught up: %d of %d offsets applied\n", *schema, got, want)
			return
		}
		select {
		case <-ctx.Done():
			must(ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

var tables = map[string]string{
	"engagement_minute": "minute_us, kind, collection, operation, events",
	"late_minute":       "minute_us, events",
	"account_state":     "account_id, start_us, last_us, events",
	"sessions":          "account_id, start_us, end_us, events",
	"session_late":      "topic, partition, events",
	"seen_events":       "event_id, time_us",
	"partition_state":   "topic, partition, next_offset, watermark_us", // generation differs by design: more claims under chaos
}

func diff(ctx context.Context, pool *pgxpool.Pool, args []string) {
	fs := flag.NewFlagSet("diff", flag.ExitOnError)
	a := fs.String("a", "clean", "baseline schema")
	b := fs.String("b", "chaos", "schema under test")
	ref := fs.String("ref", "", "reference schema built from the duplicate-free topic (optional)")
	from := fs.String("window-start", "", "RFC3339, start of the window verified in stage 1")
	to := fs.String("window-end", "", "RFC3339, end of that window")
	out := fs.String("out", "", "write JSON here")
	var extra kv
	fs.Var(&extra, "note", "key=value recorded in the result (repeatable)")
	must(fs.Parse(args))

	res := map[string]any{"measured_at": time.Now().UTC().Format(time.RFC3339), "baseline": *a, "under_test": *b}
	for k, v := range extra {
		res[k] = v
	}
	failed := false
	perTable := map[string]any{}
	for t, cols := range tables {
		onlyA := count(ctx, pool, fmt.Sprintf("SELECT %[1]s FROM %[2]s.%[4]s EXCEPT ALL SELECT %[1]s FROM %[3]s.%[4]s", cols, ident(*a), ident(*b), t))
		onlyB := count(ctx, pool, fmt.Sprintf("SELECT %[1]s FROM %[3]s.%[4]s EXCEPT ALL SELECT %[1]s FROM %[2]s.%[4]s", cols, ident(*a), ident(*b), t))
		rows := count(ctx, pool, fmt.Sprintf("SELECT 1 FROM %s.%s", ident(*a), t))
		perTable[t] = map[string]int64{"rows": rows, "only_in_baseline": onlyA, "only_in_under_test": onlyB}
		failed = failed || onlyA > 0 || onlyB > 0
	}
	res["tables"] = perTable

	var records, unique, late, sessionLate int64
	must(pool.QueryRow(ctx, fmt.Sprintf("SELECT coalesce(sum(next_offset),0) FROM %s.partition_state", ident(*b))).Scan(&records))
	must(pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s.seen_events", ident(*b))).Scan(&unique))
	must(pool.QueryRow(ctx, fmt.Sprintf("SELECT coalesce(sum(events),0) FROM %s.late_minute", ident(*b))).Scan(&late))
	must(pool.QueryRow(ctx, fmt.Sprintf("SELECT coalesce(sum(events),0) FROM %s.session_late", ident(*b))).Scan(&sessionLate))
	res["records_consumed"], res["unique_events"], res["duplicates_removed"] = records, unique, records-unique
	res["late_events"], res["session_late_events"] = late, sessionLate

	if *ref != "" {
		lo, err := time.Parse(time.RFC3339, *from)
		must(err)
		hi, err := time.Parse(time.RFC3339, *to)
		must(err)
		// Whole minutes only: a minute cut by the window edge has events outside the verified range.
		loUS := (lo.UnixMicro() + 59_999_999) / 60_000_000 * 60_000_000
		hiUS := hi.UnixMicro()/60_000_000*60_000_000 - 60_000_000
		cols := "minute_us, kind, collection, operation, events"
		where := fmt.Sprintf("WHERE minute_us BETWEEN %d AND %d", loUS, hiUS)
		onlyRef := count(ctx, pool, fmt.Sprintf("SELECT %[1]s FROM %[2]s.engagement_minute %[4]s EXCEPT ALL SELECT %[1]s FROM %[3]s.engagement_minute %[4]s", cols, ident(*ref), ident(*b), where))
		onlyB := count(ctx, pool, fmt.Sprintf("SELECT %[1]s FROM %[3]s.engagement_minute %[4]s EXCEPT ALL SELECT %[1]s FROM %[2]s.engagement_minute %[4]s", cols, ident(*ref), ident(*b), where))
		var refEvents int64
		must(pool.QueryRow(ctx, fmt.Sprintf("SELECT coalesce(sum(events),0) FROM %s.engagement_minute %s", ident(*ref), where)).Scan(&refEvents))
		res["reference_check"] = map[string]any{
			"reference_schema":        *ref,
			"minutes_from_utc":        time.UnixMicro(loUS).UTC().Format(time.RFC3339),
			"minutes_to_utc":          time.UnixMicro(hiUS + 60_000_000).UTC().Format(time.RFC3339),
			"reference_events":        refEvents,
			"rows_only_in_reference":  onlyRef,
			"rows_only_in_under_test": onlyB,
		}
		// Events the under-test run excluded as late are the only allowed difference:
		// per minute, reference = under_test + late. Rows can differ when the late
		// setting is tight enough to exclude repair events; the totals must not.
		unreconciled := count(ctx, pool, fmt.Sprintf(`
			SELECT r.minute_us FROM
			  (SELECT minute_us, sum(events) n FROM %[1]s.engagement_minute %[3]s GROUP BY 1) r
			LEFT JOIN (SELECT minute_us, sum(events) n FROM %[2]s.engagement_minute %[3]s GROUP BY 1) t USING (minute_us)
			LEFT JOIN (SELECT minute_us, events n FROM %[2]s.late_minute %[3]s) l USING (minute_us)
			WHERE r.n <> coalesce(t.n, 0) + coalesce(l.n, 0)`, ident(*ref), ident(*b), where))
		var late int64
		must(pool.QueryRow(ctx, fmt.Sprintf("SELECT coalesce(sum(events),0) FROM %s.late_minute %s", ident(*b), where)).Scan(&late))
		rc := res["reference_check"].(map[string]any)
		rc["under_test_late_in_window"] = late
		rc["minutes_not_reconciled"] = unreconciled
		failed = failed || unreconciled > 0 || (late == 0 && (onlyRef > 0 || onlyB > 0))
	}
	res["pass"] = !failed

	out2, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(out2))
	if *out != "" {
		must(os.WriteFile(*out, append(out2, '\n'), 0o644))
	}
	if failed {
		os.Exit(2)
	}
}

type kv map[string]string

func (k *kv) String() string { return "" }
func (k *kv) Set(s string) error {
	if *k == nil {
		*k = kv{}
	}
	key, val, ok := strings.Cut(s, "=")
	if !ok {
		return fmt.Errorf("want key=value, got %q", s)
	}
	(*k)[key] = val
	return nil
}

func count(ctx context.Context, pool *pgxpool.Pool, q string) int64 {
	var n int64
	must(pool.QueryRow(ctx, "SELECT count(*) FROM ("+q+") x").Scan(&n))
	return n
}

func ident(s string) string { return pgx.Identifier{s}.Sanitize() }

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "compare:", err)
		os.Exit(1)
	}
}
