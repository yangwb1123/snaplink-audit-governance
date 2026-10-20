package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/snaplink/audit-governance/internal/kafka"
)

// Metrics is an independent, bounded HTTP surface, not the default process
// mux (which could also contain debug/admin handlers). Network exposure is
// still operator-controlled; timeouts are not authentication or rate limits.
func newMetricsServer(address string, metrics func() kafka.ConsumerMetrics) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, metricsText(metrics()))
	})
	return &http.Server{
		Addr: address, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second,
		MaxHeaderBytes: 16 * 1024,
	}
}
