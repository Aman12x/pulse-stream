package codec

import (
	"testing"

	"github.com/Aman12x/pulse-stream/internal/jetstream"
)

var sample = jetstream.Event{EventID: "e1", AccountID: "a1", TimeUS: 1790528400000000,
	Kind: "commit", Collection: "app.bsky.feed.like", Operation: "create"}

func TestProtobufRoundTripCarriesSchemaID(t *testing.T) {
	b, err := Encode(42, sample)
	if err != nil {
		t.Fatal(err)
	}
	if b[0] != 0 {
		t.Fatalf("Confluent wire format starts with magic byte 0, got %d", b[0])
	}
	got, info, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if got != sample {
		t.Fatalf("round trip = %+v", got)
	}
	if info.Format != FormatProtobuf || info.SchemaID != 42 {
		t.Fatalf("info = %+v", info)
	}
}

func TestSubjectSurvivesRoundTrip(t *testing.T) {
	ev := sample
	ev.SubjectAccountID = "target"
	b, _ := Encode(7, ev)
	got, _, err := Decode(b)
	if err != nil || got != ev {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestDecodesLegacyJSON(t *testing.T) {
	got, info, err := Decode([]byte(`{"event_id":"e1","account_id":"a1","time_us":1790528400000000,"kind":"commit","collection":"app.bsky.feed.like","operation":"create"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got != sample || info.Format != FormatJSON {
		t.Fatalf("got %+v %+v", got, info)
	}
}

func TestRejectsGarbage(t *testing.T) {
	for _, b := range [][]byte{nil, {}, {0}, {0, 0, 0, 0, 1}, []byte("not json"), {7, 1, 2}} {
		if _, _, err := Decode(b); err == nil {
			t.Errorf("Decode(%v) should fail", b)
		}
	}
}
