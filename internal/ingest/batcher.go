// Package ingest holds the ingester's pure logic: batching and cursor arithmetic.
package ingest

// Record is one Kafka message waiting to be produced.
type Record struct {
	Key   []byte
	Value []byte
}

// Batch is a set of records plus the highest event time in it. The ingester
// checkpoints MaxTimeUS only after the whole batch is acknowledged by Kafka, so a
// crash can replay events but never skip them.
type Batch struct {
	Records   []Record
	MaxTimeUS int64
}

type Batcher struct {
	max int
	buf []Record
	hi  int64
}

func NewBatcher(max int) *Batcher { return &Batcher{max: max, buf: make([]Record, 0, max)} }

// Add appends a record and reports whether the batch has reached its size limit.
func (b *Batcher) Add(key, value []byte, timeUS int64) bool {
	b.buf = append(b.buf, Record{Key: key, Value: value})
	if timeUS > b.hi {
		b.hi = timeUS
	}
	return len(b.buf) >= b.max
}

func (b *Batcher) Len() int { return len(b.buf) }

// Take hands out the current batch and starts a fresh buffer, so the returned
// slice is never overwritten by later Adds.
func (b *Batcher) Take() Batch {
	if len(b.buf) == 0 {
		return Batch{}
	}
	out := Batch{Records: b.buf, MaxTimeUS: b.hi}
	b.buf, b.hi = make([]Record, 0, b.max), 0
	return out
}

// ResumeCursor is where to reconnect after a restart: the last acknowledged time
// minus a rewind window, because Jetstream's ordering near the tip is not strict.
// 0 means "no checkpoint, start at the live tail".
func ResumeCursor(checkpointUS, rewindUS int64) int64 {
	if checkpointUS <= 0 {
		return 0
	}
	if c := checkpointUS - rewindUS; c > 0 {
		return c
	}
	return 1
}
