package jetstream

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"time"

	"github.com/coder/websocket"
)

// Message is one item on the ingest channel: either a raw Jetstream event, or a
// marker that a cursor connection was just opened (ConnectedUS, local wall clock).
// The marker is sent before any event from that connection, so the consumer of the
// channel always registers the handoff before it can checkpoint past it.
type Message struct {
	Raw         []byte
	ConnectedUS int64
	RepairedUS  int64 // set on the marker that ends a repair replay
}

// Client streams raw Jetstream messages, reconnecting on failure. Each dial asks
// cursorFn where to resume, so the caller owns the cursor and decides how far back
// to rewind.
type Client struct {
	Endpoint    string // e.g. wss://jetstream2.us-east.bsky.network/subscribe
	OnReconnect func()
	Log         *slog.Logger
}

// Run blocks until ctx is cancelled, sending messages to out.
func (c *Client) Run(ctx context.Context, cursorFn func() int64, out chan<- Message) error {
	backoff := time.Second
	for {
		started := time.Now()
		err := c.stream(ctx, cursorFn(), out)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Since(started) > time.Minute {
			backoff = time.Second // a long healthy session resets the backoff
		}
		c.Log.Warn("jetstream disconnected", "err", err, "retry_in", backoff)
		if c.OnReconnect != nil {
			c.OnReconnect()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

func (c *Client) dial(ctx context.Context, cursor int64) (*websocket.Conn, error) {
	u, err := url.Parse(c.Endpoint)
	if err != nil {
		return nil, err
	}
	if cursor > 0 {
		q := u.Query()
		q.Set("cursor", strconv.FormatInt(cursor, 10))
		u.RawQuery = q.Encode()
	}
	conn, _, err := websocket.Dial(ctx, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	conn.SetReadLimit(4 << 20)
	return conn, nil
}

func (c *Client) stream(ctx context.Context, cursor int64, out chan<- Message) error {
	conn, err := c.dial(ctx, cursor)
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	c.Log.Info("jetstream connected", "cursor", cursor)
	if cursor > 0 {
		select {
		case out <- Message{ConnectedUS: time.Now().UnixMicro()}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for {
		_, msg, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		select {
		case out <- Message{Raw: msg}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// ReadRange replays [fromUS, toUS] on a separate connection and sends each event
// to out. It returns once an event past toUS arrives. Used to repair the handoff
// gap of a cursor connection after that window has become history.
func (c *Client) ReadRange(ctx context.Context, fromUS, toUS int64, out chan<- Message) (int, error) {
	conn, err := c.dial(ctx, fromUS)
	if err != nil {
		return 0, err
	}
	defer conn.CloseNow()
	n := 0
	for {
		_, msg, err := conn.Read(ctx)
		if err != nil {
			return n, fmt.Errorf("read: %w", err)
		}
		var head struct {
			TimeUS int64 `json:"time_us"`
		}
		if json.Unmarshal(msg, &head) == nil && head.TimeUS > toUS {
			return n, nil
		}
		select {
		case out <- Message{Raw: msg}:
			n++
		case <-ctx.Done():
			return n, ctx.Err()
		}
	}
}
