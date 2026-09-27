// Package protoschema embeds the .proto sources so the ingester can register the
// exact schema it was compiled against with the schema registry.
package protoschema

import _ "embed"

//go:embed pulse/v1/event.proto
var EventProto string
