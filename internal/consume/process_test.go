package consume

import (
	"testing"

	"github.com/Aman12x/pulse-stream/internal/jetstream"
)

const (
	minute = int64(60_000_000)
	t0     = int64(1_790_528_400_000_000) // a whole minute
	gap    = 30 * minute
	lateBy = 5 * minute
)

func like(acct string, t int64) jetstream.Event {
	return jetstream.Event{EventID: acct + "-" + string(rune(t%1000)), AccountID: acct, TimeUS: t,
		Kind: "commit", Collection: "app.bsky.feed.like", Operation: "create"}
}

func TestMinuteCountsByKindCollectionOperation(t *testing.T) {
	evs := []jetstream.Event{
		like("a", t0+1), like("b", t0+2), like("a", t0+minute+5),
		{EventID: "i", AccountID: "c", TimeUS: t0 + 3, Kind: "identity"},
	}
	r := Process(0, lateBy, gap, nil, evs)
	likeKey := MinuteKey{MinuteUS: t0, Kind: "commit", Collection: "app.bsky.feed.like", Operation: "create"}
	if r.Minutes[likeKey] != 2 {
		t.Errorf("likes in first minute = %d, want 2", r.Minutes[likeKey])
	}
	likeKey.MinuteUS = t0 + minute
	if r.Minutes[likeKey] != 1 {
		t.Errorf("likes in second minute = %d, want 1", r.Minutes[likeKey])
	}
	if r.Minutes[MinuteKey{MinuteUS: t0, Kind: "identity"}] != 1 {
		t.Error("identity event not counted under its own key")
	}
	if r.Watermark != t0+minute+5 {
		t.Errorf("watermark = %d, want the newest event time", r.Watermark)
	}
}

func TestLateEventsAreCountedNotAggregated(t *testing.T) {
	wm := t0 + 10*minute
	evs := []jetstream.Event{like("a", wm-lateBy-1), like("a", wm-lateBy+1)}
	r := Process(wm, lateBy, gap, nil, evs)
	var aggregated int64
	for _, n := range r.Minutes {
		aggregated += n
	}
	if aggregated != 1 {
		t.Fatalf("aggregated %d events, want 1 (the other is late)", aggregated)
	}
	lateMinute := (wm - lateBy - 1) / minute * minute
	if r.Late[lateMinute] != 1 {
		t.Fatalf("late[%d] = %d, want 1", lateMinute, r.Late[lateMinute])
	}
	if r.Watermark != wm {
		t.Fatalf("watermark moved backwards to %d", r.Watermark)
	}
	if _, touched := r.Accounts["a"]; !touched {
		t.Fatal("the on-time event should still update the session")
	}
}

func TestSessionClosesAfterGap(t *testing.T) {
	evs := []jetstream.Event{like("a", t0), like("a", t0+10*minute), like("a", t0+10*minute+gap+1)}
	r := Process(0, lateBy, gap, nil, evs)
	if len(r.Closed) != 1 {
		t.Fatalf("closed sessions = %d, want 1", len(r.Closed))
	}
	s := r.Closed[0]
	if s.AccountID != "a" || s.StartUS != t0 || s.EndUS != t0+10*minute || s.Events != 2 {
		t.Fatalf("closed session wrong: %+v", s)
	}
	open := r.Accounts["a"]
	if open.StartUS != t0+10*minute+gap+1 || open.LastUS != open.StartUS || open.Events != 1 {
		t.Fatalf("new open session wrong: %+v", open)
	}
}

func TestSessionContinuesFromStoredState(t *testing.T) {
	prior := map[string]AccountState{"a": {StartUS: t0, LastUS: t0 + 5*minute, Events: 4}}
	r := Process(0, lateBy, gap, prior, []jetstream.Event{like("a", t0+20*minute)})
	if len(r.Closed) != 0 {
		t.Fatal("a 15-minute pause is inside the gap and must not close the session")
	}
	if got := r.Accounts["a"]; got.Events != 5 || got.LastUS != t0+20*minute || got.StartUS != t0 {
		t.Fatalf("state = %+v", got)
	}
	if prior["a"].Events != 4 {
		t.Fatal("Process must not mutate the caller's state map")
	}
}

func TestOutOfOrderEventsInsideTheSession(t *testing.T) {
	prior := map[string]AccountState{"a": {StartUS: t0 + 10*minute, LastUS: t0 + 20*minute, Events: 3}}
	evs := []jetstream.Event{like("a", t0+15*minute), like("a", t0+2*minute)}
	r := Process(0, 100*gap, gap, prior, evs) // wide lateness: this test is about sessions, not the watermark
	got := r.Accounts["a"]
	if got.Events != 5 || got.LastUS != t0+20*minute || got.StartUS != t0+2*minute {
		t.Fatalf("out-of-order events should join and may extend the start back: %+v", got)
	}
	if r.SessionLate != 0 {
		t.Fatalf("SessionLate = %d", r.SessionLate)
	}
}

func TestEventTooOldForTheOpenSessionIsSessionLate(t *testing.T) {
	prior := map[string]AccountState{"a": {StartUS: t0 + 2*gap, LastUS: t0 + 2*gap, Events: 1}}
	r := Process(0, 100*gap, gap, prior, []jetstream.Event{like("a", t0)})
	if r.SessionLate != 1 {
		t.Fatalf("SessionLate = %d, want 1", r.SessionLate)
	}
	if _, touched := r.Accounts["a"]; touched {
		t.Fatal("an event belonging to an already-closed session must not change the open one")
	}
	var aggregated int64
	for _, n := range r.Minutes {
		aggregated += n
	}
	if aggregated != 1 {
		t.Fatal("it is still inside the watermark, so it still counts in the minute aggregates")
	}
}
