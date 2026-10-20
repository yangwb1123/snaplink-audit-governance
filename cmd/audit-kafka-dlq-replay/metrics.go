package main

import (
	"net/http"
	"time"

	"github.com/snaplink/audit-governance/internal/kafka"
)

// Keep metrics independent of any default-mux debug/admin registrations.
// Binding/firewall/TLS remain deployment responsibilities, not timeout proofs.
func newMetricsServer(address string, metrics func() kafka.ReplayerMetrics) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", metricsHandler(metrics))
	return &http.Server{
		Addr: address, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second,
		MaxHeaderBytes: 16 * 1024,
	}
}
