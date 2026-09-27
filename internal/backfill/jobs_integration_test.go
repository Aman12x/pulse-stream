//go:build integration

package backfill

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func openTest(t *testing.T) *Control {
	t.Helper()
	dsn := os.Getenv("PG_DSN")
	if dsn == "" {
		dsn = "postgres://pulse:pulse@localhost:5433/pulse?sslmode=disable"
	}
	schema := fmt.Sprintf("bftest_%d", time.Now().UnixNano())
	c, err := Open(context.Background(), dsn, schema)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, sch := range []string{schema, schema + "_live", schema + "_v1", schema + "_v2"} {
			c.pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+sch+" CASCADE")
		}
		c.Close()
	})
	return c
}

func TestPlanCreatesOneJobPerPartition(t *testing.T) {
	ctx := context.Background()
	c := openTest(t)
	v, err := c.Plan(ctx, Spec{Topic: "t", SessionGapS: 1800, LateS: 300, Note: "n"}, map[int32]int64{0: 10, 1: 0, 2: 7})
	if err != nil {
		t.Fatal(err)
	}
	st, _ := c.Status(ctx, v)
	if st.Jobs != 2 {
		t.Fatalf("jobs = %d, want 2 (an empty partition needs no job)", st.Jobs)
	}
}

func TestClaimSkipsLeasedJobsAndTakesExpiredOnes(t *testing.T) {
	ctx := context.Background()
	c := openTest(t)
	v, _ := c.Plan(ctx, Spec{Topic: "t", SessionGapS: 1800, LateS: 300}, map[int32]int64{0: 10})
	a, err := c.Claim(ctx, v, "A", 400*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Claim(ctx, v, "B", time.Second); !errors.Is(err, ErrNoJob) {
		t.Fatalf("B claimed a job A still holds: %v", err)
	}
	time.Sleep(600 * time.Millisecond)
	b, err := c.Claim(ctx, v, "B", time.Second)
	if err != nil {
		t.Fatalf("B could not take the expired lease: %v", err)
	}
	if b.ID != a.ID || b.Token != a.Token+1 {
		t.Fatalf("B's claim = %+v, want same job with token %d", b, a.Token+1)
	}
	if err := c.Heartbeat(ctx, a, time.Second); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("A's heartbeat after losing the lease: %v", err)
	}
	if err := c.Complete(ctx, a); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("A completed a job it no longer holds: %v", err)
	}
	if err := c.Complete(ctx, b); err != nil {
		t.Fatal(err)
	}
	st, _ := c.Status(ctx, v)
	if st.Done != 1 || st.State != "ready" {
		t.Fatalf("status = %+v, want 1 done and version ready", st)
	}
}

func TestCutoverRequiresReadyAndDiffed(t *testing.T) {
	ctx := context.Background()
	c := openTest(t)
	v, _ := c.Plan(ctx, Spec{Topic: "t", SessionGapS: 1800, LateS: 300}, map[int32]int64{0: 1})
	if err := c.Cutover(ctx, v); err == nil {
		t.Fatal("cutover of an unfinished version must fail")
	}
	j, _ := c.Claim(ctx, v, "A", time.Second)
	c.Complete(ctx, j)
	if err := c.Cutover(ctx, v); err == nil {
		t.Fatal("cutover without a recorded diff must fail")
	}
	if err := c.MarkDiffed(ctx, v, "results/x.json"); err != nil {
		t.Fatal(err)
	}
	if err := c.Cutover(ctx, v); err != nil {
		t.Fatalf("cutover: %v", err)
	}
	st, _ := c.Status(ctx, v)
	if st.State != "live" {
		t.Fatalf("state = %s, want live", st.State)
	}
}
