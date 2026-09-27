// Command verify compares a reference ingest topic (one process, never killed)
// against a chaos topic (an ingester killed repeatedly) over the window both
// covered, and writes the result as JSON.
//
//	missing    events in the reference window that never reached the chaos topic (must be 0)
//	duplicates extra copies in the chaos topic, from the rewind on restart (expected, deduplicated downstream)
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/Aman12x/pulse-stream/internal/codec"
	"github.com/twmb/franz-go/pkg/kgo"
)

type id [16]byte

type topicScan struct {
	counts  map[id]int32
	times   map[id]int64
	kinds   map[id]string
	firstUS int64
	lastUS  int64
	records int64
}

func scan(ctx context.Context, brokers []string, topic string) (*topicScan, error) {
	adm, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, err
	}
	ends, err := kadm.NewClient(adm).ListEndOffsets(ctx, topic)
	adm.Close()
	if err != nil {
		return nil, err
	}
	remaining := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			remaining[o.Partition] = o.Offset
		}
	})

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	s := &topicScan{counts: map[id]int32{}, times: map[id]int64{}, kinds: map[id]string{}}
	for len(remaining) > 0 {
		fs := cl.PollFetches(ctx)
		if err := fs.Err0(); err != nil {
			return nil, err
		}
		fs.EachRecord(func(r *kgo.Record) {
			ev, _, err := codec.Decode(r.Value)
			if err != nil {
				return
			}
			var k id
			if b, err := hex.DecodeString(ev.EventID); err == nil && len(b) == 16 {
				copy(k[:], b)
			} else {
				return
			}
			s.counts[k]++
			s.times[k] = ev.TimeUS
			s.kinds[k] = ev.Kind + " " + ev.Collection
			s.records++
			if s.firstUS == 0 || ev.TimeUS < s.firstUS {
				s.firstUS = ev.TimeUS
			}
			if ev.TimeUS > s.lastUS {
				s.lastUS = ev.TimeUS
			}
			if end, ok := remaining[r.Partition]; ok && r.Offset+1 >= end {
				delete(remaining, r.Partition)
			}
		})
	}
	return s, nil
}

func main() {
	var (
		brokers  = flag.String("brokers", "localhost:9092", "comma-separated brokers")
		refTopic = flag.String("ref", "bsky.events.ref", "reference topic")
		chaos    = flag.String("chaos", "bsky.events.chaos", "chaos topic")
		marginS  = flag.Int("margin", 10, "seconds trimmed from each end of the shared window")
		kills    = flag.Int("kills", 0, "number of kill -9s in the chaos run (recorded, not measured)")
		out      = flag.String("out", "", "write the JSON result here")
		dump     = flag.String("dump-missing", "", "write time_us and kind of each missing event here")
	)
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	bs := strings.Split(*brokers, ",")

	ref, err := scan(ctx, bs, *refTopic)
	must(err)
	ch, err := scan(ctx, bs, *chaos)
	must(err)

	margin := int64(*marginS) * 1_000_000
	lo, hi := max(ref.firstUS, ch.firstUS)+margin, min(ref.lastUS, ch.lastUS)-margin
	if hi <= lo {
		must(fmt.Errorf("no shared window: ref [%d,%d] chaos [%d,%d]", ref.firstUS, ref.lastUS, ch.firstUS, ch.lastUS))
	}

	var refInWindow, missing, chaosInWindow, chaosUnique, extra int64
	var missingList []string
	for k, t := range ref.times {
		if t < lo || t > hi {
			continue
		}
		refInWindow++
		if ch.counts[k] == 0 {
			missing++
			missingList = append(missingList, fmt.Sprintf("%d %s", t, ref.kinds[k]))
		}
	}
	for k, t := range ch.times {
		if t < lo || t > hi {
			continue
		}
		chaosUnique++
		chaosInWindow += int64(ch.counts[k])
		if ref.counts[k] == 0 {
			extra++
		}
	}

	res := map[string]any{
		"measured_at":                   time.Now().UTC().Format(time.RFC3339),
		"window_start_utc":              time.UnixMicro(lo).UTC().Format(time.RFC3339),
		"window_end_utc":                time.UnixMicro(hi).UTC().Format(time.RFC3339),
		"window_seconds":                (hi - lo) / 1_000_000,
		"kills":                         *kills,
		"reference_events_in_window":    refInWindow,
		"chaos_records_in_window":       chaosInWindow,
		"chaos_unique_events_in_window": chaosUnique,
		"missing_from_chaos":            missing,
		"duplicate_records_in_chaos":    chaosInWindow - chaosUnique,
		"chaos_events_not_in_reference": extra,
		"reference_duplicate_records":   ref.records - int64(len(ref.counts)),
	}
	b, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(b))
	if *out != "" {
		must(os.WriteFile(*out, append(b, '\n'), 0o644))
	}
	if *dump != "" {
		must(os.WriteFile(*dump, []byte(strings.Join(missingList, "\n")+"\n"), 0o644))
	}
	if missing > 0 {
		os.Exit(2)
	}
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "verify:", err)
		os.Exit(1)
	}
}
