package server

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "payd_http_request_duration_seconds",
	Help:    "HTTP request latency by route pattern and status code.",
	Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16),
}, []string{"route", "status"})

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// wraps the mux directly since the mux sets r.Pattern on the request it is handed
func instrument(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{w, http.StatusOK}
		mux.ServeHTTP(sw, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		requestDuration.WithLabelValues(route, strconv.Itoa(sw.status)).Observe(time.Since(start).Seconds())
	})
}

func registerOutboxLag(db *pgxpool.Pool) {
	err := prometheus.Register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "payd_outbox_lag_seconds",
		Help: "Age of the oldest unpublished outbox event, 0 when none are pending.",
	}, func() float64 {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var lag float64
		if err := db.QueryRow(ctx, `SELECT coalesce(extract(epoch FROM now() - min(created_at)), 0) FROM outbox_events WHERE published_at IS NULL`).Scan(&lag); err != nil {
			return -1
		}
		return lag
	}))
	var are prometheus.AlreadyRegisteredError
	if err != nil && !errors.As(err, &are) {
		panic(err)
	}
}

func withMetrics(api http.Handler, db *pgxpool.Pool) http.Handler {
	registerOutboxLag(db)
	top := http.NewServeMux()
	top.Handle("GET /metrics", promhttp.Handler())
	top.Handle("/", api)
	return top
}
