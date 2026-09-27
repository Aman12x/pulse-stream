package ingest

import "testing"

func TestBatcherReportsFullAtMax(t *testing.T) {
	b := NewBatcher(3)
	if b.Add([]byte("k"), []byte("v1"), 10) {
		t.Fatal("not full after 1 of 3")
	}
	b.Add([]byte("k"), []byte("v2"), 11)
	if !b.Add([]byte("k"), []byte("v3"), 12) {
		t.Fatal("should report full at 3 of 3")
	}
}

func TestTakeReturnsRecordsAndHighestTimeThenResets(t *testing.T) {
	b := NewBatcher(10)
	b.Add([]byte("a"), []byte("1"), 100)
	b.Add([]byte("b"), []byte("2"), 250)
	b.Add([]byte("c"), []byte("3"), 200) // out of order: the checkpoint must still be the max seen
	batch := b.Take()
	if len(batch.Records) != 3 {
		t.Fatalf("records = %d", len(batch.Records))
	}
	if batch.MaxTimeUS != 250 {
		t.Fatalf("MaxTimeUS = %d, want 250", batch.MaxTimeUS)
	}
	if string(batch.Records[1].Key) != "b" || string(batch.Records[1].Value) != "2" {
		t.Fatalf("record order/content wrong: %+v", batch.Records[1])
	}
	if b.Len() != 0 {
		t.Fatal("Take must reset the buffer")
	}
	if next := b.Take(); len(next.Records) != 0 || next.MaxTimeUS != 0 {
		t.Fatalf("empty Take should be zero: %+v", next)
	}
}

func TestTakeDoesNotAliasPreviousBatch(t *testing.T) {
	b := NewBatcher(10)
	b.Add([]byte("a"), []byte("1"), 1)
	first := b.Take()
	b.Add([]byte("z"), []byte("9"), 2)
	if string(first.Records[0].Key) != "a" {
		t.Fatal("a later Add overwrote a batch already handed out")
	}
}

func TestResumeCursorRewinds(t *testing.T) {
	if got := ResumeCursor(0, 5_000_000); got != 0 {
		t.Fatalf("no checkpoint must mean live tail (0), got %d", got)
	}
	if got := ResumeCursor(10_000_000, 5_000_000); got != 5_000_000 {
		t.Fatalf("got %d", got)
	}
	if got := ResumeCursor(3_000_000, 5_000_000); got != 1 {
		t.Fatalf("a rewind past zero must clamp to the earliest valid cursor, got %d", got)
	}
}
