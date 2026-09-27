// Package codec is the wire format between the ingester and the consumers.
//
// Current: Confluent wire format — magic byte 0, 4-byte big-endian schema id from
// the schema registry, message-index list, then the Protobuf-encoded pulse.v1.Event.
// Legacy: the JSON produced before stage 3. Decode accepts both, so topics written
// before the migration stay readable.
package codec

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"

	pulsev1 "github.com/Aman12x/pulse-stream/gen/pulse/v1"
	"github.com/Aman12x/pulse-stream/internal/jetstream"
)

type Format string

const (
	FormatProtobuf Format = "protobuf"
	FormatJSON     Format = "json"
)

type Info struct {
	Format   Format
	SchemaID int // 0 for JSON
}

var header sr.ConfluentHeader

// Encode frames ev with the registry schema id. Event is the first (and only)
// message in its file, so its index list is [0].
func Encode(schemaID int, ev jetstream.Event) ([]byte, error) {
	b, err := header.AppendEncode(nil, schemaID, []int{0})
	if err != nil {
		return nil, err
	}
	return proto.MarshalOptions{}.MarshalAppend(b, toProto(ev))
}

func Decode(b []byte) (jetstream.Event, Info, error) {
	if len(b) == 0 {
		return jetstream.Event{}, Info{}, errors.New("empty record")
	}
	switch b[0] {
	case 0:
		id, rest, err := header.DecodeID(b)
		if err != nil {
			return jetstream.Event{}, Info{}, err
		}
		if _, rest, err = header.DecodeIndex(rest, 8); err != nil {
			return jetstream.Event{}, Info{}, err
		}
		var pb pulsev1.Event
		if err := proto.Unmarshal(rest, &pb); err != nil {
			return jetstream.Event{}, Info{}, err
		}
		ev := fromProto(&pb)
		if ev.EventID == "" {
			return jetstream.Event{}, Info{}, errors.New("protobuf event without event_id")
		}
		return ev, Info{Format: FormatProtobuf, SchemaID: id}, nil
	case '{':
		var ev jetstream.Event
		if err := json.Unmarshal(b, &ev); err != nil {
			return jetstream.Event{}, Info{}, err
		}
		if ev.EventID == "" {
			return jetstream.Event{}, Info{}, errors.New("json event without event_id")
		}
		return ev, Info{Format: FormatJSON}, nil
	default:
		return jetstream.Event{}, Info{}, fmt.Errorf("unknown record format (first byte %d)", b[0])
	}
}

func toProto(ev jetstream.Event) *pulsev1.Event {
	return &pulsev1.Event{EventId: ev.EventID, AccountId: ev.AccountID, TimeUs: ev.TimeUS,
		Kind: ev.Kind, Collection: ev.Collection, Operation: ev.Operation, SubjectAccountId: ev.SubjectAccountID}
}

func fromProto(pb *pulsev1.Event) jetstream.Event {
	return jetstream.Event{EventID: pb.GetEventId(), AccountID: pb.GetAccountId(), TimeUS: pb.GetTimeUs(),
		Kind: pb.GetKind(), Collection: pb.GetCollection(), Operation: pb.GetOperation(),
		SubjectAccountID: pb.GetSubjectAccountId()}
}
