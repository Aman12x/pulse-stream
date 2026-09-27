// Package jetstream reads Bluesky's Jetstream feed and normalizes its events.
//
// Normalization is where privacy is enforced: of the record body only the
// "subject" reference is decoded (post text, profile fields and everything else are
// skipped), and every DID is replaced by a salted hash before an event leaves this
// package.
package jetstream

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// RawEvent is the subset of a Jetstream message the pipeline reads. From the
// commit's record only "subject" is decoded; text and every other field are skipped
// by encoding/json and never enter the pipeline.
type RawEvent struct {
	DID    string     `json:"did"`
	TimeUS int64      `json:"time_us"`
	Kind   string     `json:"kind"`
	Commit *RawCommit `json:"commit,omitempty"`
}

type RawCommit struct {
	Rev        string `json:"rev"`
	Operation  string `json:"operation"`
	Collection string `json:"collection"`
	RKey       string `json:"rkey"`
	Record     *struct {
		Subject json.RawMessage `json:"subject"`
	} `json:"record,omitempty"`
}

// Event is what goes onto Kafka.
type Event struct {
	EventID    string `json:"event_id"`
	AccountID  string `json:"account_id"`
	TimeUS     int64  `json:"time_us"`
	Kind       string `json:"kind"`
	Collection string `json:"collection,omitempty"`
	Operation  string `json:"operation,omitempty"`
	// SubjectAccountID is the hashed account a like, repost, follow or block points
	// at (schema v2). Same hash as AccountID, so the two join.
	SubjectAccountID string `json:"subject_account_id,omitempty"`
}

// Hasher derives stable, salted identifiers. The same salt must be used by every
// process whose output is compared or deduplicated together.
type Hasher struct{ salt []byte }

func NewHasher(salt string) Hasher { return Hasher{salt: []byte(salt)} }

func (h Hasher) hash(parts ...string) string {
	s := sha256.New()
	s.Write(h.salt)
	for _, p := range parts {
		s.Write([]byte{0})
		s.Write([]byte(p))
	}
	return hex.EncodeToString(s.Sum(nil))[:32]
}

var errMalformed = errors.New("malformed jetstream event")

// Normalize turns a raw message into an Event. The event id identifies the
// underlying repo change, so a replayed message gets the same id as the original.
func (h Hasher) Normalize(r RawEvent) (Event, error) {
	if r.DID == "" || r.TimeUS <= 0 {
		return Event{}, fmt.Errorf("%w: missing did or time_us", errMalformed)
	}
	ev := Event{AccountID: h.hash("account", r.DID), TimeUS: r.TimeUS, Kind: r.Kind}
	switch r.Kind {
	case "commit":
		c := r.Commit
		if c == nil || c.Collection == "" || c.RKey == "" || c.Rev == "" || c.Operation == "" {
			return Event{}, fmt.Errorf("%w: incomplete commit", errMalformed)
		}
		ev.Collection, ev.Operation = c.Collection, c.Operation
		ev.EventID = h.hash("commit", r.DID, c.Collection, c.RKey, c.Rev, c.Operation)
		if did := subjectDID(c); did != "" {
			ev.SubjectAccountID = h.hash("account", did)
		}
	case "identity", "account":
		// These carry no rev; Jetstream's time_us is stable across replays of one instance.
		ev.EventID = h.hash(r.Kind, r.DID, strconv.FormatInt(r.TimeUS, 10))
	default:
		return Event{}, fmt.Errorf("%w: unknown kind %q", errMalformed, r.Kind)
	}
	return ev, nil
}

// subjectDID extracts the target DID from a record's subject: a strong ref
// ({"uri": "at://did/..."}) for likes and reposts, a bare DID for follows and blocks.
func subjectDID(c *RawCommit) string {
	if c.Record == nil || len(c.Record.Subject) == 0 {
		return ""
	}
	var did string
	if json.Unmarshal(c.Record.Subject, &did) != nil {
		var ref struct {
			URI string `json:"uri"`
		}
		if json.Unmarshal(c.Record.Subject, &ref) != nil {
			return ""
		}
		rest, ok := strings.CutPrefix(ref.URI, "at://")
		if !ok {
			return ""
		}
		did, _, _ = strings.Cut(rest, "/")
	}
	if !strings.HasPrefix(did, "did:") {
		return ""
	}
	return did
}
