// Package consume turns deduplicated events into engagement aggregates and
// sessions. Process is pure; store.go applies its result inside the same Postgres
// transaction that records the partition offset.
package consume

import "github.com/Aman12x/pulse-stream/internal/jetstream"

const minuteUS = int64(60_000_000)

type MinuteKey struct {
	MinuteUS   int64
	Kind       string
	Collection string
	Operation  string
}

// AccountState is an account's open session.
type AccountState struct {
	StartUS, LastUS int64
	Events          int64
}

type Session struct {
	AccountID      string
	StartUS, EndUS int64
	Events         int64
}

type Result struct {
	Minutes     map[MinuteKey]int64
	Late        map[int64]int64 // minute -> events that arrived behind the watermark
	Accounts    map[string]AccountState
	Closed      []Session
	SessionLate int64 // events older than the start of the account's open session by more than the gap
	Watermark   int64
}

// Process applies events, in partition order, on top of the stored watermark and
// account states. Events already seen must be removed by the caller: Process
// counts everything it is given.
//
// An event more than lateUS behind the partition's watermark is late: it is
// counted per minute in Late and not aggregated or sessionized. Sessions end after
// gapUS without activity. An on-time event that arrives out of order joins the
// open session if it is within gapUS of its start (extending the start back if
// needed); if it is older than that, its session has already closed and it is
// counted in SessionLate instead.
func Process(watermark, lateUS, gapUS int64, states map[string]AccountState, events []jetstream.Event) Result {
	r := Result{
		Minutes:   map[MinuteKey]int64{},
		Late:      map[int64]int64{},
		Accounts:  map[string]AccountState{},
		Watermark: watermark,
	}
	for _, ev := range events {
		minute := ev.TimeUS / minuteUS * minuteUS
		if r.Watermark > 0 && ev.TimeUS < r.Watermark-lateUS {
			r.Late[minute]++
			continue
		}
		if ev.TimeUS > r.Watermark {
			r.Watermark = ev.TimeUS
		}
		r.Minutes[MinuteKey{MinuteUS: minute, Kind: ev.Kind, Collection: ev.Collection, Operation: ev.Operation}]++

		st, ok := r.Accounts[ev.AccountID]
		if !ok {
			st, ok = states[ev.AccountID]
		}
		switch {
		case !ok:
			st = AccountState{StartUS: ev.TimeUS, LastUS: ev.TimeUS, Events: 1}
		case ev.TimeUS-st.LastUS > gapUS:
			r.Closed = append(r.Closed, Session{AccountID: ev.AccountID, StartUS: st.StartUS, EndUS: st.LastUS, Events: st.Events})
			st = AccountState{StartUS: ev.TimeUS, LastUS: ev.TimeUS, Events: 1}
		case ev.TimeUS >= st.LastUS:
			st.LastUS = ev.TimeUS
			st.Events++
		case ev.TimeUS >= st.StartUS-gapUS:
			if ev.TimeUS < st.StartUS {
				st.StartUS = ev.TimeUS
			}
			st.Events++
		default:
			r.SessionLate++
			continue
		}
		r.Accounts[ev.AccountID] = st
	}
	return r
}
