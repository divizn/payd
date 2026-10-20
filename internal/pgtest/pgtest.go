package pgtest

import (
	"context"
	"fmt"

	"github.com/divizn/payd/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// Start runs a migrated Postgres container and returns its DSN, a pool on it, and a stop func
func Start(ctx context.Context, maxConns int) (string, *pgxpool.Pool, func(), error) {
	pg, err := postgres.Run(ctx, "postgres:16-alpine", postgres.BasicWaitStrategies())
	if err != nil {
		return "", nil, nil, err
	}
	stop := func() { pg.Terminate(context.Background()) }
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		stop()
		return "", nil, nil, err
	}
	db, err := pgxpool.New(ctx, fmt.Sprintf("%s&pool_max_conns=%d", dsn, maxConns))
	if err != nil {
		stop()
		return "", nil, nil, err
	}
	if err := migrations.Up(ctx, stdlib.OpenDBFromPool(db)); err != nil {
		db.Close()
		stop()
		return "", nil, nil, err
	}
	return dsn, db, func() { db.Close(); stop() }, nil
}
