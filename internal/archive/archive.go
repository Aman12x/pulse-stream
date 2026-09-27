// Package archive writes events to Parquet files for the analytics layer.
//
// Delivery is at-least-once. A file covers one partition's contiguous offset range
// and is named by it; it is written to a temporary name and renamed into place,
// and only then is the offset committed. A crash between the rename and the commit
// makes the next owner rewrite an overlapping range, so readers must deduplicate
// by event_id (the dbt staging model does).
package archive

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/Aman12x/pulse-stream/internal/jetstream"
)

type Row struct {
	EventID          string `parquet:"event_id"`
	AccountID        string `parquet:"account_id"`
	TimeUS           int64  `parquet:"time_us"`
	Kind             string `parquet:"kind"`
	Collection       string `parquet:"collection"`
	Operation        string `parquet:"operation"`
	SubjectAccountID string `parquet:"subject_account_id"`
	KafkaPartition   int32  `parquet:"kafka_partition"`
	KafkaOffset      int64  `parquet:"kafka_offset"`
}

func FromEvent(ev jetstream.Event, partition int32, offset int64) Row {
	return Row{EventID: ev.EventID, AccountID: ev.AccountID, TimeUS: ev.TimeUS, Kind: ev.Kind,
		Collection: ev.Collection, Operation: ev.Operation, SubjectAccountID: ev.SubjectAccountID,
		KafkaPartition: partition, KafkaOffset: offset}
}

// Path is where a range lands: dir/topic=T/date=D/part-P-START-END.parquet, with D
// the UTC date of the first event so readers can prune by day.
func Path(dir, topic string, partition int32, first, last int64, firstEventUS int64) string {
	day := time.UnixMicro(firstEventUS).UTC().Format("2006-01-02")
	return filepath.Join(dir, "topic="+topic, "date="+day, fmt.Sprintf("part-%d-%012d-%012d.parquet", partition, first, last))
}

// Write stores rows at path atomically (temp file, fsync, rename).
func Write(path string, rows []Row) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := parquet.NewGenericWriter[Row](f, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write(rows); err != nil {
		f.Close()
		return err
	}
	if err := w.Close(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
