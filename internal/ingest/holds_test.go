package ingest

import "testing"

func TestCapWithNoHoldsIsAcked(t *testing.T) {
	h := NewHolds()
	if got := h.Cap(500); got != 500 {
		t.Fatalf("Cap = %d, want 500", got)
	}
}

func TestCapNeverPassesAnUnrepairedReconnect(t *testing.T) {
	h := NewHolds()
	h.Add(300)
	h.Add(200)
	if got := h.Cap(500); got != 200 {
		t.Fatalf("Cap = %d, want the earliest hold 200", got)
	}
	h.Release(200)
	if got := h.Cap(500); got != 300 {
		t.Fatalf("after releasing 200, Cap = %d, want 300", got)
	}
	h.Release(300)
	if got := h.Cap(500); got != 500 {
		t.Fatalf("with every hold released, Cap = %d, want acked 500", got)
	}
}

func TestCapBelowHoldIsAcked(t *testing.T) {
	h := NewHolds()
	h.Add(900)
	if got := h.Cap(400); got != 400 {
		t.Fatalf("Cap = %d, want 400 (acked is already behind the hold)", got)
	}
}

func TestReleaseUnknownIsNoop(t *testing.T) {
	h := NewHolds()
	h.Add(100)
	h.Release(999)
	if h.Pending() != 1 {
		t.Fatal("releasing a hold that was never added must not drop others")
	}
}
