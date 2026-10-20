package main

import (
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/kafka"
)

func TestMetricsServerBoundedAndPrivateMux(t *testing.T) {
	server := newMetricsServer("127.0.0.1:0", func() kafka.ConsumerMetrics { return kafka.ConsumerMetrics{} })
	if server.ReadHeaderTimeout != 5*time.Second || server.ReadTimeout != 10*time.Second ||
		server.WriteTimeout != 10*time.Second || server.IdleTimeout != 60*time.Second || server.MaxHeaderBytes != 16*1024 {
		t.Fatal("metrics surface lost its independent timeout/header bounds")
	}
	if server.Handler == nil || server.Handler == http.DefaultServeMux {
		t.Fatal("metrics inherited the process-wide mux")
	}
	for _, test := range []struct {
		method, path string
		status       int
	}{
		{http.MethodGet, "/metrics", http.StatusOK},
		{http.MethodGet, "/debug/pprof/", http.StatusNotFound},
		{http.MethodPost, "/metrics", http.StatusMethodNotAllowed},
	} {
		response := httptest.NewRecorder()
		server.Handler.ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))
		if response.Code != test.status {
			t.Fatalf("%s %s status=%d", test.method, test.path, response.Code)
		}
		if test.status == http.StatusOK && (response.Header().Get("Content-Type") != "text/plain; version=0.0.4" || response.Body.String() != metricsText(kafka.ConsumerMetrics{})) {
			t.Fatal("scrape format changed")
		}
	}
}

func TestMetricsServerClosesIncompleteHeadersAtSocketBoundary(t *testing.T) {
	var handled atomic.Int64
	server := newMetricsServer("127.0.0.1:0", func() kafka.ConsumerMetrics {
		handled.Add(1)
		return kafka.ConsumerMetrics{}
	})
	// Only the test's independently constructed server is accelerated. The
	// production 5s setting is pinned above; preparation is synchronized at
	// the actual accepted/closed socket boundaries, not a startup sleep.
	server.ReadHeaderTimeout = 50 * time.Millisecond
	accepted, closed := make(chan struct{}), make(chan struct{})
	server.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			close(accepted)
		} else if state == http.StateClosed {
			close(closed)
		}
	}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	// Queue incomplete bytes before the server starts its read timer; slow
	// test preparation must not consume the deliberately short header budget.
	connection, err := net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := io.WriteString(connection, "GET /metrics HTTP/1.1\r\nHost: local\r\nX-Incomplete:"); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close metrics server: %v", err)
		}
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve metrics: %v", err)
		}
	})
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("server never accepted the socket")
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("incomplete headers kept a socket open")
	}
	if handled.Load() != 0 {
		t.Fatal("incomplete request reached metrics collection")
	}
}
