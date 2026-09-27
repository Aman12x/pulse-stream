# One image with every pulse-stream binary; the Compose service picks the command.
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...
# The archive volume mounts here. An empty named volume copies this directory's
# ownership, so the non-root archiver can write to it.
RUN mkdir -p /archive

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
COPY --from=build --chown=65532:65532 /archive /archive
USER nonroot
