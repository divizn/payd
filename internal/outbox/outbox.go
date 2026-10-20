package outbox

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const batchSize = 100

var published = promauto.NewCounter(prometheus.CounterOpts{
	Name: "payd_outbox_events_published_total",
	Help: "Outbox events published, including redeliveries after a crash.",
})

type Event struct {
	ID        int64           `json:"id"`
	PaymentID string          `json:"payment_id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
}

func Enqueue(ctx context.Context, tx pgx.Tx, paymentID, typ string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox_events (payment_id, type, payload) VALUES ($1, $2, $3)`, paymentID, typ, b)
	return err
}

// Relay publishes each event at least once, so consumers dedupe by Event.ID
type Relay struct {
	DB       *pgxpool.Pool
	Publish  func(context.Context, Event) error
	Interval time.Duration
	Log      *slog.Logger
}

func (r Relay) Run(ctx context.Context) {
	t := time.NewTicker(r.Interval)
	defer t.Stop()
	for {
		n, err := r.publishBatch(ctx)
		if err != nil && ctx.Err() == nil {
			r.Log.ErrorContext(ctx, "outbox relay", "err", err)
		}
		if err == nil {
			published.Add(float64(n))
		}
		if err == nil && n == batchSize {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (r Relay) publishBatch(ctx context.Context) (int, error) {
	tx, err := r.DB.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id, payment_id::text, type, payload FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, batchSize)
	if err != nil {
		return 0, err
	}
	events, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Event])
	if err != nil {
		return 0, err
	}
	ids := make([]int64, len(events))
	for i, e := range events {
		if err := r.Publish(ctx, e); err != nil {
			return 0, err
		}
		ids[i] = e.ID
	}
	if _, err := tx.Exec(ctx, `UPDATE outbox_events SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return 0, err
	}
	return len(events), tx.Commit(ctx)
}
