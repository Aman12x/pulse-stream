package ingest

// Holds caps the checkpoint below reconnects whose gap repair has not finished.
//
// Measured 2026-09-27 (docs/decisions.md): a Jetstream connection opened with a
// cursor drops a few events that arrive in the ~70 ms around its replay-to-live
// handoff. A later replay of that window recovers them. Until that replay has
// been produced, the checkpoint must stay behind the handoff, so that a crash
// before the repair replays the window again instead of skipping it.
type Holds struct{ pending map[int64]struct{} }

func NewHolds() *Holds { return &Holds{pending: map[int64]struct{}{}} }

func (h *Holds) Add(us int64)     { h.pending[us] = struct{}{} }
func (h *Holds) Release(us int64) { delete(h.pending, us) }
func (h *Holds) Pending() int     { return len(h.pending) }

// Cap returns the highest cursor that is safe to checkpoint.
func (h *Holds) Cap(acked int64) int64 {
	c := acked
	for us := range h.pending {
		if us < c {
			c = us
		}
	}
	return c
}
