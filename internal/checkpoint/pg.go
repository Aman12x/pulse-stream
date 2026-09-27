// Package checkpoint persists how far each ingester has safely produced to Kafka.
package checkpoint

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schema = `
CREATE TABLE IF NOT EXISTS ingest_checkpoint (
    ingester_id text PRIMARY KEY,
    cursor_us   bigint NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
)`

type Store struct{ pool *pgxpool.Pool }

func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// Load returns the saved cursor, or 0 when this ingester has never checkpointed.
func (s *Store) Load(ctx context.Context, ingesterID string) (int64, error) {
	var c int64
	err := s.pool.QueryRow(ctx, `SELECT cursor_us FROM ingest_checkpoint WHERE ingester_id = $1`, ingesterID).Scan(&c)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return c, err
}

// Save records a cursor. It never moves a checkpoint backwards: a stale write from
// a process that is shutting down cannot undo progress made by its successor.
func (s *Store) Save(ctx context.Context, ingesterID string, cursorUS int64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO ingest_checkpoint (ingester_id, cursor_us) VALUES ($1, $2)
		ON CONFLICT (ingester_id) DO UPDATE
		SET cursor_us = GREATEST(ingest_checkpoint.cursor_us, EXCLUDED.cursor_us), updated_at = now()`,
		ingesterID, cursorUS)
	return err
}
