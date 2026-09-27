//go:build integration

package checkpoint

import (
	"context"
	"os"
	"testing"
	"time"
)

func dsn() string {
	if v := os.Getenv("PG_DSN"); v != "" {
		return v
	}
	return "postgres://pulse:pulse@localhost:5433/pulse?sslmode=disable"
}

func TestCheckpointRoundTripAndNeverRegresses(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, dsn())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	id := "test-" + time.Now().Format("150405.000000")

	if c, err := s.Load(ctx, id); err != nil || c != 0 {
		t.Fatalf("fresh load = %d, %v; want 0, nil", c, err)
	}
	if err := s.Save(ctx, id, 500); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(ctx, id, 300); err != nil { // a stale writer
		t.Fatal(err)
	}
	if c, _ := s.Load(ctx, id); c != 500 {
		t.Fatalf("checkpoint regressed to %d", c)
	}
	if err := s.Save(ctx, id, 900); err != nil {
		t.Fatal(err)
	}
	if c, _ := s.Load(ctx, id); c != 900 {
		t.Fatalf("checkpoint = %d, want 900", c)
	}
}
