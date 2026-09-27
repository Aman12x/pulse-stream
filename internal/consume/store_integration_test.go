//go:build integration

package consume

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Aman12x/pulse-stream/internal/jetstream"
)

func openTest(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		dsn = "postgres://pulse:pulse@localhost:5433/pulse?sslmode=disable"
	}
	schema := fmt.Sprintf("test_%d", time.Now().UnixNano())
	s, err := Open(context.Background(), dsn, schema, Config{LateUS: 300_000_000, GapUS: 1_800_000_000})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		s.pool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		s.Close()
	})
	return s
}

func rec(off int64, id, acct string, t int64) Record {
	return Record{Offset: off, Event: jetstream.Event{EventID: id, AccountID: acct, TimeUS: t,
		Kind: "commit", Collection: "app.bsky.feed.like", Operation: "create"}}
}

const base = int64(1_790_528_400_000_000)

func TestApplyDedupesAndSkipsRedelivery(t *testing.T) {
	ctx := context.Background()
	s := openTest(t)
	gen, next, err := s.Claim(ctx, "t", 0)
	if err != nil || gen != 1 || next != 0 {
		t.Fatalf("claim = %d %d %v", gen, next, err)
	}
	batch := []Record{rec(0, "e1", "a", base), rec(1, "e2", "a", base+1), rec(2, "e1", "a", base)} // e1 twice
	st, err := s.Apply(ctx, "t", 0, gen, batch)
	if err != nil {
		t.Fatal(err)
	}
	if st.New != 2 || st.Duplicates != 1 || st.Applied != 3 {
		t.Fatalf("stats = %+v", st)
	}
	// Redelivery of the same offsets after a crash is skipped, not re-applied.
	st, err = s.Apply(ctx, "t", 0, gen, batch)
	if err != nil || st.Skipped != 3 || st.Applied != 0 {
		t.Fatalf("redelivery stats = %+v, %v", st, err)
	}
	var events int64
	s.pool.QueryRow(ctx, "SELECT sum(events) FROM engagement_minute").Scan(&events)
	if events != 2 {
		t.Fatalf("engagement_minute total = %d, want 2", events)
	}
	// A duplicate arriving later at a new offset is dropped too.
	st, _ = s.Apply(ctx, "t", 0, gen, []Record{rec(3, "e2", "a", base+1)})
	if st.Duplicates != 1 || st.New != 0 {
		t.Fatalf("late duplicate stats = %+v", st)
	}
}

func TestStaleOwnerIsFencedAndWritesNothing(t *testing.T) {
	ctx := context.Background()
	s := openTest(t)
	old, _, _ := s.Claim(ctx, "t", 0)
	newer, next, _ := s.Claim(ctx, "t", 0)
	if newer != old+1 || next != 0 {
		t.Fatalf("second claim = %d, %d", newer, next)
	}
	if _, err := s.Apply(ctx, "t", 0, old, []Record{rec(0, "e1", "a", base)}); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale owner err = %v, want ErrFenced", err)
	}
	var n int64
	s.pool.QueryRow(ctx, "SELECT count(*) FROM seen_events").Scan(&n)
	if n != 0 {
		t.Fatal("a fenced batch must leave no trace")
	}
	if _, err := s.Apply(ctx, "t", 0, newer, []Record{rec(0, "e1", "a", base)}); err != nil {
		t.Fatalf("current owner: %v", err)
	}
}

func TestOffsetGapIsAnError(t *testing.T) {
	ctx := context.Background()
	s := openTest(t)
	gen, _, _ := s.Claim(ctx, "t", 0)
	if _, err := s.Apply(ctx, "t", 0, gen, []Record{rec(5, "e1", "a", base)}); err == nil {
		t.Fatal("a batch starting past the stored offset must fail, never skip records")
	}
}

func TestSessionClosedAcrossBatches(t *testing.T) {
	ctx := context.Background()
	s := openTest(t)
	gen, _, _ := s.Claim(ctx, "t", 0)
	if _, err := s.Apply(ctx, "t", 0, gen, []Record{rec(0, "e1", "a", base), rec(1, "e2", "a", base+60_000_000)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(ctx, "t", 0, gen, []Record{rec(2, "e3", "a", base+60_000_000+1_800_000_001)}); err != nil {
		t.Fatal(err)
	}
	var start, end, events int64
	if err := s.pool.QueryRow(ctx, "SELECT start_us, end_us, events FROM sessions WHERE account_id='a'").Scan(&start, &end, &events); err != nil {
		t.Fatal(err)
	}
	if start != base || end != base+60_000_000 || events != 2 {
		t.Fatalf("session = %d %d %d", start, end, events)
	}
}

func TestPruneSeenDropsOnlyOldEntries(t *testing.T) {
	ctx := context.Background()
	s := openTest(t)
	gen, _, _ := s.Claim(ctx, "t", 0)
	recs := []Record{rec(0, "old1", "a", 100), rec(1, "old2", "a", 200), rec(2, "new", "a", 5_000)}
	if _, err := s.Apply(ctx, "t", 0, gen, recs); err != nil {
		t.Fatal(err)
	}
	n, err := s.PruneSeen(ctx, 1_000, 1) // batch of 1 exercises the loop
	if err != nil || n != 2 {
		t.Fatalf("pruned %d, %v; want 2", n, err)
	}
	var left int64
	s.pool.QueryRow(ctx, "SELECT count(*) FROM seen_events").Scan(&left)
	if left != 1 {
		t.Fatalf("%d entries left, want 1", left)
	}
}
