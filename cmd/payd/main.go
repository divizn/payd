package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/divizn/payd/internal/acquirer"
	"github.com/divizn/payd/internal/outbox"
	"github.com/divizn/payd/internal/server"
	"github.com/divizn/payd/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer db.Close()

	if err := migrations.Up(ctx, stdlib.OpenDBFromPool(db)); err != nil {
		return err
	}

	relay := outbox.Relay{DB: db, Interval: 100 * time.Millisecond, Log: log, Publish: func(ctx context.Context, e outbox.Event) error {
		log.InfoContext(ctx, "event published", "event", e)
		return nil
	}}
	relayDone := make(chan struct{})
	go func() {
		relay.Run(ctx)
		close(relayDone)
	}()
	defer func() {
		stop()
		<-relayDone
	}()

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	acq := &acquirer.Fake{}
	if acq.Latency, err = envDuration("ACQUIRER_LATENCY"); err != nil {
		return err
	}
	if acq.ErrorRate, err = envFloat("ACQUIRER_ERROR_RATE"); err != nil {
		return err
	}
	if acq.TimeoutRate, err = envFloat("ACQUIRER_TIMEOUT_RATE"); err != nil {
		return err
	}
	policy := acquirer.Policy{Attempts: 3, Timeout: 500 * time.Millisecond, Backoff: 50 * time.Millisecond}
	srv := &http.Server{Addr: addr, Handler: server.New(db, log, acq, policy), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()
	log.Info("listening", "addr", addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func envDuration(name string) (time.Duration, error) {
	v := os.Getenv(name)
	if v == "" {
		return 0, nil
	}
	return time.ParseDuration(v)
}

func envFloat(name string) (float64, error) {
	v := os.Getenv(name)
	if v == "" {
		return 0, nil
	}
	return strconv.ParseFloat(v, 64)
}
