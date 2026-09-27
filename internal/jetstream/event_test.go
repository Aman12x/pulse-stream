package jetstream

import (
	"encoding/json"
	"strings"
	"testing"
)

const postJSON = `{
  "did": "did:plc:abc123",
  "time_us": 1725911162329308,
  "kind": "commit",
  "commit": {
    "rev": "3l3qo2vutsw2b",
    "operation": "create",
    "collection": "app.bsky.feed.post",
    "rkey": "3l3qo2vuowo2b",
    "record": {"$type": "app.bsky.feed.post", "text": "a secret thought", "createdAt": "2024-09-09T19:46:02.102Z"},
    "cid": "bafyreidwaivazkwu67xztlmuobx35hs2lnfh3kolmgfmucldvhd3sgzcqi"
  }
}`

func decode(t *testing.T, s string) RawEvent {
	t.Helper()
	var r RawEvent
	if err := json.Unmarshal([]byte(s), &r); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return r
}

func TestNormalizeCommitDropsRecordAndHashesDID(t *testing.T) {
	h := NewHasher("salt")
	ev, err := h.Normalize(decode(t, postJSON))
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	out, _ := json.Marshal(ev)
	for _, leak := range []string{"secret thought", "did:plc:abc123", "createdAt"} {
		if strings.Contains(string(out), leak) {
			t.Errorf("normalized event leaks %q: %s", leak, out)
		}
	}
	if ev.Kind != "commit" || ev.Collection != "app.bsky.feed.post" || ev.Operation != "create" {
		t.Errorf("commit fields not carried: %+v", ev)
	}
	if ev.TimeUS != 1725911162329308 {
		t.Errorf("time_us = %d", ev.TimeUS)
	}
	if len(ev.EventID) != 32 || len(ev.AccountID) != 32 {
		t.Errorf("ids should be 32 hex chars: %q %q", ev.EventID, ev.AccountID)
	}
}

func TestEventIDIsStableAndSaltDependent(t *testing.T) {
	raw := decode(t, postJSON)
	a, _ := NewHasher("salt").Normalize(raw)
	b, _ := NewHasher("salt").Normalize(raw)
	c, _ := NewHasher("other").Normalize(raw)
	if a.EventID != b.EventID {
		t.Error("same input and salt must give the same event id (dedupe depends on it)")
	}
	if a.EventID == c.EventID || a.AccountID == c.AccountID {
		t.Error("a different salt must give different ids")
	}
}

func TestDifferentCommitsGetDifferentIDs(t *testing.T) {
	h := NewHasher("salt")
	a, _ := h.Normalize(decode(t, postJSON))
	updated := strings.Replace(postJSON, `"operation": "create"`, `"operation": "update"`, 1)
	updated = strings.Replace(updated, `"rev": "3l3qo2vutsw2b"`, `"rev": "3l3qo2vutsw2c"`, 1)
	b, _ := h.Normalize(decode(t, updated))
	if a.EventID == b.EventID {
		t.Error("an update of the same record is a different event")
	}
	if a.AccountID != b.AccountID {
		t.Error("same DID must map to the same account id")
	}
}

func TestNormalizeIdentityAndAccountEvents(t *testing.T) {
	h := NewHasher("salt")
	for _, kind := range []string{"identity", "account"} {
		raw := decode(t, `{"did":"did:plc:x","time_us":1725516665333808,"kind":"`+kind+`","`+kind+`":{"did":"did:plc:x","seq":1}}`)
		ev, err := h.Normalize(raw)
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		if ev.Kind != kind || ev.Collection != "" || ev.EventID == "" {
			t.Errorf("%s normalized wrong: %+v", kind, ev)
		}
	}
}

func TestNormalizeRejectsMalformed(t *testing.T) {
	h := NewHasher("salt")
	cases := map[string]string{
		"no did":         `{"time_us":1,"kind":"identity"}`,
		"no time":        `{"did":"did:plc:x","kind":"identity"}`,
		"commit no body": `{"did":"did:plc:x","time_us":1,"kind":"commit"}`,
		"commit no rkey": `{"did":"did:plc:x","time_us":1,"kind":"commit","commit":{"rev":"r","operation":"create","collection":"c"}}`,
		"unknown kind":   `{"did":"did:plc:x","time_us":1,"kind":"weird"}`,
	}
	for name, s := range cases {
		if _, err := h.Normalize(decode(t, s)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestSubjectAccountIsHashedTargetOfLikeAndFollow(t *testing.T) {
	h := NewHasher("salt")
	target, _ := h.Normalize(decode(t, `{"did":"did:plc:target","time_us":1,"kind":"identity"}`))

	likeJSON := `{"did":"did:plc:liker","time_us":2,"kind":"commit","commit":{"rev":"r1","operation":"create",
		"collection":"app.bsky.feed.like","rkey":"k1","record":{"$type":"app.bsky.feed.like",
		"subject":{"uri":"at://did:plc:target/app.bsky.feed.post/3abc","cid":"bafy"},"createdAt":"x"}}}`
	like, err := h.Normalize(decode(t, likeJSON))
	if err != nil {
		t.Fatal(err)
	}
	if like.SubjectAccountID != target.AccountID {
		t.Fatalf("like subject = %q, want the target's account id %q", like.SubjectAccountID, target.AccountID)
	}

	followJSON := `{"did":"did:plc:follower","time_us":3,"kind":"commit","commit":{"rev":"r2","operation":"create",
		"collection":"app.bsky.graph.follow","rkey":"k2","record":{"$type":"app.bsky.graph.follow","subject":"did:plc:target","createdAt":"x"}}}`
	follow, _ := h.Normalize(decode(t, followJSON))
	if follow.SubjectAccountID != target.AccountID {
		t.Fatalf("follow subject = %q", follow.SubjectAccountID)
	}
}

func TestNoSubjectWhereThereIsNone(t *testing.T) {
	h := NewHasher("salt")
	post, _ := h.Normalize(decode(t, postJSON))
	del, _ := h.Normalize(decode(t, `{"did":"did:plc:x","time_us":1,"kind":"commit","commit":{"rev":"r","operation":"delete","collection":"app.bsky.feed.like","rkey":"k"}}`))
	odd, _ := h.Normalize(decode(t, `{"did":"did:plc:x","time_us":1,"kind":"commit","commit":{"rev":"r","operation":"create","collection":"app.bsky.feed.like","rkey":"k","record":{"subject":{"uri":"https://not-an-at-uri"}}}}`))
	for name, ev := range map[string]Event{"post": post, "delete": del, "non-at uri": odd} {
		if ev.SubjectAccountID != "" {
			t.Errorf("%s: SubjectAccountID = %q, want empty", name, ev.SubjectAccountID)
		}
	}
}
