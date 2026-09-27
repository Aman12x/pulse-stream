package codec

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/sr"
)

// Register publishes a Protobuf schema under subject and returns its id. The
// registry checks it against the subject's compatibility level (BACKWARD in this
// stack) and rejects an incompatible schema with HTTP 409, so an ingester built
// against a breaking schema fails at startup instead of writing unreadable events.
// Registering an identical schema again returns the existing id.
func Register(ctx context.Context, url, subject, schema string) (int, error) {
	cl, err := sr.NewClient(sr.URLs(url))
	if err != nil {
		return 0, err
	}
	ss, err := cl.CreateSchema(ctx, subject, sr.Schema{Schema: schema, Type: sr.TypeProtobuf})
	if err != nil {
		return 0, fmt.Errorf("register %s: %w", subject, err)
	}
	return ss.ID, nil
}
