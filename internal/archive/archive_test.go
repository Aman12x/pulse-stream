package archive

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/Aman12x/pulse-stream/internal/jetstream"
)

func TestWriteRoundTripsAndLeavesNoTempFile(t *testing.T) {
	dir := t.TempDir()
	ev := jetstream.Event{EventID: "e1", AccountID: "a", TimeUS: 1790528400000000, Kind: "commit",
		Collection: "app.bsky.feed.like", Operation: "create", SubjectAccountID: "b"}
	p := Path(dir, "bsky.events", 3, 100, 101, ev.TimeUS)
	if !strings.Contains(p, filepath.Join("topic=bsky.events", "date=2026-09-27", "part-3-000000000100-000000000101.parquet")) {
		t.Fatalf("path = %s", p)
	}
	rows := []Row{FromEvent(ev, 3, 100), FromEvent(ev, 3, 101)}
	if err := Write(p, rows); err != nil {
		t.Fatal(err)
	}
	got, err := parquet.ReadFile[Row](p)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[1] != rows[1] {
		t.Fatalf("read back %+v", got)
	}
	if _, err := os.Stat(p + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("temp file left behind")
	}
}
