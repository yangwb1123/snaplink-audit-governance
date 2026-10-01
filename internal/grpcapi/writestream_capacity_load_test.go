//go:build writestream_load

package grpcapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	auditv1 "github.com/snaplink/audit-governance/api/proto"
	"github.com/snaplink/audit-governance/internal/auth"
	"github.com/snaplink/audit-governance/internal/domain"
	"github.com/snaplink/audit-governance/internal/service"
	"github.com/snaplink/audit-governance/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestGRPCWriteStreamCapacityLoad is an observational load test, not a
// capacity gate. It is opt-in because it opens a real listener, performs
// sustained writes, and deliberately injects a dependency fault. The fixed
// levels below are workload observations; none is a server default or limit.
func TestGRPCWriteStreamCapacityLoad(t *testing.T) {
	publisher := &loadLedgeredPublisher{}
	observer := &loadObserver{}
	st, listener, grpcServer := startWriteStreamLoadServer(t, observer, publisher)
	defer st.Close()
	defer grpcServer.Stop()
	defer listener.Close()

	connections := openLoadConnections(t, listener.Addr().String())
	defer func() {
		for _, connection := range connections {
			_ = connection.Close()
		}
	}()
	clients := make([]auditv1.IngestClient, 0, len(connections))
	for _, connection := range connections {
		clients = append(clients, auditv1.NewIngestClient(connection))
	}

	token := signGRPCJWT(t, map[string]any{
		"sub":       "writestream-capacity-load",
		"tenant_id": "tenant-a",
		"client_id": "crm",
		"roles":     []string{"service"},
		"exp":       time.Now().Add(30 * time.Minute).Unix(),
	})
	var reports []loadLevelReport
	for index, level := range writeStreamLoadLevels {
		report := runWriteStreamLoadLevel(t, clients, token, observer, publisher, level, index == len(writeStreamLoadLevels)-1)
		reports = append(reports, report)
		encoded, err := json.Marshal(report)
		if err != nil {
			t.Fatalf("marshal load report for level %d: %v", level, err)
		}
		t.Logf("WRITESTREAM_LOAD %s", encoded)
	}
	if len(reports) < 2 {
		t.Fatalf("reports=%d, want multiple offered workload levels", len(reports))
	}
	if len(connections) < 2 {
		t.Fatalf("connections=%d, want at least two independent ClientConns", len(connections))
	}
	if reports[len(reports)-1].DependencyFault.Errors == 0 {
		t.Fatal("dependency fault produced no recorded publisher errors")
	}
	if reports[len(reports)-1].DependencyFault.RecoverySuccesses == 0 {
		t.Fatal("dependency fault did not record a post-fault recovery success")
	}
}

const (
	loadDependencyDelay = 120 * time.Millisecond
	loadFaultAfter      = 650 * time.Millisecond
	loadFaultDuration   = 350 * time.Millisecond
	loadLevelDuration   = 1800 * time.Millisecond
	loadSampleInterval  = 100 * time.Millisecond
	loadSlowReaderDelay = 700 * time.Millisecond
	loadSlowMessages    = 24
)

var writeStreamLoadLevels = []int{4, 12, 24}

type loadObserver struct {
	handlers atomic.Int64
	workers  atomic.Int64
}

func (o *loadObserver) handlerStarted()    { o.handlers.Add(1) }
func (o *loadObserver) handlerFinished()   { o.handlers.Add(-1) }
func (o *loadObserver) recvWorkerStarted() { o.workers.Add(1) }
func (o *loadObserver) recvWorkerFinished() {
	o.workers.Add(-1)
}

func (o *loadObserver) snapshot() (int64, int64) {
	return o.handlers.Load(), o.workers.Load()
}

// loadObservedServerStream counts the actual RecvMsg call made by the
// expiry-aware receive goroutine. The production ServerStream is wrapped only
// in this build-tagged load test, so this cannot affect admission or RPC
// behavior in a normal binary.
type loadObservedServerStream struct {
	grpc.ServerStream
	observer *loadObserver
}

func (s *loadObservedServerStream) RecvMsg(message any) error {
	s.observer.recvWorkerStarted()
	defer s.observer.recvWorkerFinished()
	return s.ServerStream.RecvMsg(message)
}

func loadObservationInterceptor(observer *loadObserver) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		observer.handlerStarted()
		defer observer.handlerFinished()
		return handler(srv, &loadObservedServerStream{ServerStream: stream, observer: observer})
	}
}

type loadLedgeredPublisher struct {
	degraded        atomic.Bool
	calls           atomic.Int64
	errors          atomic.Int64
	success         atomic.Int64
	recoverySuccess atomic.Int64
	mu              sync.Mutex
	faultEnd        bool
	recoveredAt     time.Time
}

func (p *loadLedgeredPublisher) SetDegraded(value bool) {
	if !value {
		p.mu.Lock()
		p.faultEnd = true
		p.mu.Unlock()
	}
	p.degraded.Store(value)
}

func (p *loadLedgeredPublisher) Publish(ctx context.Context, _ domain.Event) error {
	p.calls.Add(1)
	if p.degraded.Load() {
		p.errors.Add(1)
		timer := time.NewTimer(loadDependencyDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
			return errors.New("controlled ledgered publisher degradation")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	p.mu.Lock()
	recovered := p.faultEnd
	if recovered && p.recoveredAt.IsZero() {
		p.recoveredAt = time.Now()
	}
	p.mu.Unlock()
	p.success.Add(1)
	if recovered {
		p.recoverySuccess.Add(1)
	}
	return nil
}

type loadPublisherSnapshot struct {
	calls           int64
	errors          int64
	success         int64
	recoverySuccess int64
}

func (p *loadLedgeredPublisher) snapshot() loadPublisherSnapshot {
	return loadPublisherSnapshot{
		calls:           p.calls.Load(),
		errors:          p.errors.Load(),
		success:         p.success.Load(),
		recoverySuccess: p.recoverySuccess.Load(),
	}
}

type loadStream struct {
	stream auditv1.Ingest_WriteStreamClient
	cancel context.CancelFunc
	kind   string
	conn   int
	id     int
}

type loadCollector struct {
	mu        sync.Mutex
	opens     []time.Duration
	sends     []time.Duration
	receipts  []time.Duration
	terminals []time.Duration
	pending   map[string]time.Time
	statuses  map[string]int
}

func newLoadCollector() *loadCollector {
	return &loadCollector{pending: map[string]time.Time{}, statuses: map[string]int{}}
}

func (c *loadCollector) opened(started time.Time, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.addStatusLocked("open:" + loadErrorLabel(err))
		return
	}
	c.opens = append(c.opens, time.Since(started))
}

func (c *loadCollector) sent(eventID string, started time.Time, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.addStatusLocked("send:" + loadErrorLabel(err))
		return
	}
	c.sends = append(c.sends, time.Since(started))
	c.pending[eventID] = started
}

func (c *loadCollector) receipt(eventID string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.addStatusLocked("receipt:" + loadErrorLabel(err))
		return
	}
	started, ok := c.pending[eventID]
	if !ok {
		c.addStatusLocked("receipt:missing_send_timestamp")
		return
	}
	c.receipts = append(c.receipts, time.Since(started))
	delete(c.pending, eventID)
}

func (c *loadCollector) terminal(started time.Time, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.terminals = append(c.terminals, time.Since(started))
	c.addStatusLocked("terminal:" + loadErrorLabel(err))
}

func (c *loadCollector) addStatusLocked(label string) {
	c.statuses[label]++
}

func (c *loadCollector) snapshot() (loadLatencySummary, loadLatencySummary, loadLatencySummary, loadLatencySummary, map[string]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	statuses := make(map[string]int, len(c.statuses))
	for label, count := range c.statuses {
		statuses[label] = count
	}
	return summarizeLoadLatencies(c.opens), summarizeLoadLatencies(c.sends), summarizeLoadLatencies(c.receipts), summarizeLoadLatencies(c.terminals), statuses
}

type loadLatencySummary struct {
	Count int     `json:"count"`
	P50MS float64 `json:"p50_ms"`
	P95MS float64 `json:"p95_ms"`
	MaxMS float64 `json:"max_ms"`
}

func summarizeLoadLatencies(values []time.Duration) loadLatencySummary {
	if len(values) == 0 {
		return loadLatencySummary{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	at := func(percentile int) float64 {
		index := (len(values)*percentile + 99) / 100
		if index < 1 {
			index = 1
		}
		if index > len(values) {
			index = len(values)
		}
		return float64(values[index-1]) / float64(time.Millisecond)
	}
	return loadLatencySummary{Count: len(values), P50MS: at(50), P95MS: at(95), MaxMS: float64(values[len(values)-1]) / float64(time.Millisecond)}
}

type loadResourcePeak struct {
	Samples             int    `json:"samples"`
	PeakActiveHandlers  int64  `json:"peak_active_handlers"`
	PeakRecvWorkers     int64  `json:"peak_expiry_recv_workers"`
	PeakGoroutines      int    `json:"peak_goroutines"`
	PeakHeapAllocBytes  uint64 `json:"peak_heap_alloc_bytes"`
	PeakProcessRSSBytes int64  `json:"peak_process_rss_bytes,omitempty"`
	PeakOpenFileDescs   int    `json:"peak_open_file_descriptors,omitempty"`
}

type loadConnectionReport struct {
	ID                string `json:"id"`
	Offered           int    `json:"offered"`
	Opened            int    `json:"opened"`
	ActiveAtSample    int    `json:"active_at_sample"`
	IdleStreams       int    `json:"idle_streams"`
	NormalStreams     int    `json:"normal_streams"`
	SlowReaderStreams int    `json:"slow_reader_streams"`
}

type loadDependencyReport struct {
	Dependency         string `json:"dependency"`
	Condition          string `json:"condition"`
	Injected           bool   `json:"injected"`
	FaultStartOffsetMS int64  `json:"fault_start_offset_ms"`
	FaultDurationMS    int64  `json:"fault_duration_ms"`
	ObservedDurationMS int64  `json:"observed_duration_ms"`
	Calls              int64  `json:"calls"`
	Errors             int64  `json:"errors"`
	RecoverySuccesses  int64  `json:"recovery_successes"`
}

type loadLevelReport struct {
	OfferedStreams          int                    `json:"offered_streams"`
	OpenedStreams           int                    `json:"opened_streams"`
	ActiveStreamsSample     int                    `json:"active_streams_at_sample"`
	IdleStreams             int                    `json:"idle_streams"`
	NormalStreams           int                    `json:"normal_streams"`
	SlowReaderStreams       int                    `json:"slow_reader_streams"`
	SlowReaderDelayMS       int64                  `json:"slow_reader_delay_ms"`
	AdmissionControlPresent bool                   `json:"admission_control_present"`
	StreamOpenLatency       loadLatencySummary     `json:"stream_open_latency"`
	AdmissionLatency        loadLatencySummary     `json:"admission_latency"`
	SendLatency             loadLatencySummary     `json:"send_latency"`
	ReceiptLatency          loadLatencySummary     `json:"receipt_latency"`
	TerminalLatency         loadLatencySummary     `json:"terminal_latency"`
	Resources               loadResourcePeak       `json:"resources"`
	StatusCodes             map[string]int         `json:"status_codes"`
	Connections             []loadConnectionReport `json:"connections"`
	DependencyFault         loadDependencyReport   `json:"dependency_fault"`
}

func startWriteStreamLoadServer(t *testing.T, observer *loadObserver, publisher *loadLedgeredPublisher) (*store.Store, net.Listener, *grpc.Server) {
	t.Helper()
	st, err := store.Open("")
	if err != nil {
		t.Fatal(err)
	}
	svc, err := service.New(st, service.Config{
		Now:               time.Now,
		AllowDevSecrets:   true,
		SegmentSize:       100000,
		LedgeredPublisher: publisher,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateTenant("load-test", domain.Tenant{ID: "tenant-a", Name: "Tenant A", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AddSource("load-test", domain.SourceSystem{TenantID: "tenant-a", ID: "crm", Name: "CRM", Active: true}); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterSchema("load-test", domain.EventSchema{TenantID: "tenant-a", SchemaID: "audit.event", Version: 1, EventType: "audit.event", Active: true}); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	authenticator := auth.Authenticator{JWTSecret: testJWTSecret, AllowLocalHS256: true}
	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(KeepaliveParams()),
		grpc.KeepaliveEnforcementPolicy(KeepaliveEnforcementPolicy()),
		grpc.MaxRecvMsgSize(MaxRecvBytes),
		grpc.ChainStreamInterceptor(
			RecoveryStreamServerInterceptor(log.New(io.Discard, "", 0)),
			loadObservationInterceptor(observer),
		),
	)
	auditv1.RegisterIngestServer(grpcServer, &Server{Service: svc, Auth: authenticator, Logger: log.New(io.Discard, "", 0)})
	go func() { _ = grpcServer.Serve(listener) }()
	return st, listener, grpcServer
}

func openLoadConnections(t *testing.T, address string) []*grpc.ClientConn {
	t.Helper()
	connections := make([]*grpc.ClientConn, 0, 2)
	for i := 0; i < 2; i++ {
		connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("open independent ClientConn %d: %v", i+1, err)
		}
		connections = append(connections, connection)
	}
	return connections
}

func runWriteStreamLoadLevel(t *testing.T, clients []auditv1.IngestClient, token string, observer *loadObserver, publisher *loadLedgeredPublisher, level int, injectFault bool) loadLevelReport {
	t.Helper()
	collector := newLoadCollector()
	connections := make([]loadConnectionReport, len(clients))
	for index := range connections {
		connections[index].ID = "client-conn-" + strconv.Itoa(index+1)
	}
	idle := level / 3
	if idle < 1 {
		idle = 1
	}
	slow := level / 4
	if slow < 1 {
		slow = 1
	}
	if idle+slow > level {
		slow = level - idle
	}
	report := loadLevelReport{
		OfferedStreams:          level,
		IdleStreams:             idle,
		NormalStreams:           level - idle - slow,
		SlowReaderStreams:       slow,
		SlowReaderDelayMS:       loadSlowReaderDelay.Milliseconds(),
		AdmissionControlPresent: false,
		Connections:             connections,
		DependencyFault: loadDependencyReport{
			Dependency:         "service.Config.LedgeredPublisher",
			Condition:          "Publish returns a controlled error after a bounded delay",
			Injected:           injectFault,
			FaultStartOffsetMS: loadFaultAfter.Milliseconds(),
			FaultDurationMS:    loadFaultDuration.Milliseconds(),
		},
	}
	beforePublisher := publisher.snapshot()
	streams := make([]loadStream, 0, level)
	for index := 0; index < level; index++ {
		kind := "idle"
		switch {
		case index >= idle+slow:
			kind = "normal"
		case index >= idle:
			kind = "slow_reader"
		}
		connIndex := index % len(clients)
		connections[connIndex].Offered++
		base := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
		streamContext, cancel := context.WithCancel(base)
		openStarted := time.Now()
		stream, err := clients[connIndex].WriteStream(streamContext)
		collector.opened(openStarted, err)
		if err != nil {
			cancel()
			continue
		}
		connections[connIndex].Opened++
		streams = append(streams, loadStream{stream: stream, cancel: cancel, kind: kind, conn: connIndex, id: index})
	}
	report.OpenedStreams = len(streams)
	for index := range connections {
		connections[index].ActiveAtSample = connections[index].Opened
	}
	if !waitForLoadHandlers(observer, int64(report.OpenedStreams), 2*time.Second) {
		t.Fatalf("offered level %d opened %d streams but active handler observation did not converge", level, report.OpenedStreams)
	}
	var work sync.WaitGroup
	for _, stream := range streams {
		if stream.kind == "idle" {
			continue
		}
		work.Add(1)
		if stream.kind == "slow_reader" {
			go func(stream loadStream) {
				defer work.Done()
				runSlowLoadStream(stream, collector)
			}(stream)
			continue
		}
		go func(stream loadStream) {
			defer work.Done()
			runNormalLoadStream(stream, collector)
		}(stream)
	}

	resource := loadResourcePeak{}
	levelStart := time.Now()
	deadline := levelStart.Add(loadLevelDuration)
	faultStarted := false
	faultEnded := false
	var faultStartedAt, faultEndedAt time.Time
	for time.Now().Before(deadline) {
		now := time.Now()
		if injectFault && !faultStarted && !now.Before(levelStart.Add(loadFaultAfter)) {
			publisher.SetDegraded(true)
			faultStarted = true
			faultStartedAt = now
		}
		if injectFault && faultStarted && !faultEnded && !now.Before(levelStart.Add(loadFaultAfter+loadFaultDuration)) {
			publisher.SetDegraded(false)
			faultEnded = true
			faultEndedAt = now
		}
		sampleLoadResources(observer, &resource)
		time.Sleep(loadSampleInterval)
	}
	if injectFault && faultStarted && !faultEnded {
		publisher.SetDegraded(false)
		faultEndedAt = time.Now()
	}
	stopAt := time.Now()
	for _, stream := range streams {
		if stream.kind != "idle" {
			stream.cancel()
		}
	}
	work.Wait()
	for _, stream := range streams {
		if stream.kind == "idle" {
			_ = stream.stream.CloseSend()
		}
	}
	for _, stream := range streams {
		_, err := stream.stream.Recv()
		collector.terminal(stopAt, err)
		stream.cancel()
	}
	if !waitForLoadHandlers(observer, 0, 5*time.Second) {
		t.Fatalf("level %d left active handlers=%d workers=%d after stream cleanup", level, observer.handlers.Load(), observer.workers.Load())
	}

	report.ActiveStreamsSample = report.OpenedStreams
	report.Resources = resource
	report.StreamOpenLatency, report.SendLatency, report.ReceiptLatency, report.TerminalLatency, report.StatusCodes = collector.snapshot()
	if injectFault && !faultStartedAt.IsZero() {
		report.DependencyFault.ObservedDurationMS = faultEndedAt.Sub(faultStartedAt).Milliseconds()
	}
	afterPublisher := publisher.snapshot()
	report.DependencyFault.Calls = afterPublisher.calls - beforePublisher.calls
	report.DependencyFault.Errors = afterPublisher.errors - beforePublisher.errors
	report.DependencyFault.RecoverySuccesses = afterPublisher.recoverySuccess - beforePublisher.recoverySuccess
	for index := range connections {
		connections[index].IdleStreams = countLoadStreams(streams, index, "idle")
		connections[index].NormalStreams = countLoadStreams(streams, index, "normal")
		connections[index].SlowReaderStreams = countLoadStreams(streams, index, "slow_reader")
	}
	report.Connections = connections
	return report
}

func countLoadStreams(streams []loadStream, connection int, kind string) int {
	count := 0
	for _, stream := range streams {
		if stream.conn == connection && stream.kind == kind {
			count++
		}
	}
	return count
}

func runNormalLoadStream(stream loadStream, collector *loadCollector) {
	for sequence := 0; ; sequence++ {
		event := testProtoEvent(fmt.Sprintf("capacity-normal-%d-%d", stream.id, sequence), "crm")
		started := time.Now()
		err := stream.stream.Send(&auditv1.WriteRequest{Event: event})
		collector.sent(event.GetEventId(), started, err)
		if err != nil {
			return
		}
		response, err := stream.stream.Recv()
		if err != nil {
			collector.receipt(event.GetEventId(), err)
			return
		}
		collector.receipt(response.GetEventId(), nil)
		time.Sleep(35 * time.Millisecond)
	}
}

func runSlowLoadStream(stream loadStream, collector *loadCollector) {
	var reader sync.WaitGroup
	reader.Add(1)
	go func() {
		defer reader.Done()
		time.Sleep(loadSlowReaderDelay)
		for index := 0; index < loadSlowMessages; index++ {
			response, err := stream.stream.Recv()
			if err != nil {
				collector.receipt("", err)
				return
			}
			collector.receipt(response.GetEventId(), nil)
		}
	}()
	for sequence := 0; sequence < loadSlowMessages; sequence++ {
		event := testProtoEvent(fmt.Sprintf("capacity-slow-%d-%d", stream.id, sequence), "crm")
		started := time.Now()
		err := stream.stream.Send(&auditv1.WriteRequest{Event: event})
		collector.sent(event.GetEventId(), started, err)
		if err != nil {
			break
		}
	}
	reader.Wait()
}

func waitForLoadHandlers(observer *loadObserver, want int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		handlers, workers := observer.snapshot()
		if handlers == want && (want != 0 || workers == 0) {
			return true
		}
		_ = workers
		time.Sleep(10 * time.Millisecond)
	}
	handlers, _ := observer.snapshot()
	return handlers == want
}

func sampleLoadResources(observer *loadObserver, peak *loadResourcePeak) {
	handlers, workers := observer.snapshot()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	peak.Samples++
	if handlers > peak.PeakActiveHandlers {
		peak.PeakActiveHandlers = handlers
	}
	if workers > peak.PeakRecvWorkers {
		peak.PeakRecvWorkers = workers
	}
	if runtime.NumGoroutine() > peak.PeakGoroutines {
		peak.PeakGoroutines = runtime.NumGoroutine()
	}
	if memory.HeapAlloc > peak.PeakHeapAllocBytes {
		peak.PeakHeapAllocBytes = memory.HeapAlloc
	}
	if rss := processRSSBytes(); rss > peak.PeakProcessRSSBytes {
		peak.PeakProcessRSSBytes = rss
	}
	if fds := openFileDescriptorCount(); fds > peak.PeakOpenFileDescs {
		peak.PeakOpenFileDescs = fds
	}
}

func processRSSBytes() int64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		value, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0
		}
		return value * 1024
	}
	return 0
}

func openFileDescriptorCount() int {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0
	}
	return len(entries)
}

func loadErrorLabel(err error) string {
	if errors.Is(err, io.EOF) {
		return "EOF"
	}
	if err == nil {
		return "OK"
	}
	return status.Code(err).String()
}
