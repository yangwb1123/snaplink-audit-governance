package store

// The request-path deadline, proven against a real database/sql driver.
//
// The defect this direction fixes is that a statement on the PostgreSQL
// request path cannot be interrupted: it runs while the process-wide Store.mu
// is held, so one black-holed or lock-blocked connection freezes ingest and
// reads for every tenant in the process. These tests use a hand-written
// database/sql driver (~80 lines, no new module dependency) that blocks on the
// context it is handed, which is the only way to prove the context actually
// reaches the wire — a stub Backend would prove nothing.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snaplink/audit-governance/internal/domain"
)

// probeDriver is the scripted database/sql driver behind the deadline tests.
//
// Scripting is by query substring rather than by a full statement grammar:
// the store issues a bounded, known set of statements, and the discriminator
// only has to separate "the catalog probe" from "the hot-document write"
// because those are the two steps the deadline behaviour depends on.
type probeDriver struct {
	mu sync.Mutex
	// catalog is the hot/cold catalog verdict returned once the scripted
	// catalogFailures budget is exhausted ("complete" selects the split store).
	catalog string
	// catalogFailures makes that many leading catalog probes block until the
	// context ends, modelling a transient probe failure.
	catalogFailures int
	// blockWrite makes the tenant hot-document INSERT block until the context
	// ends, modelling a lock-blocked or black-holed statement.
	blockWrite     bool
	catalogStarted chan struct{}
	writeStarted   chan struct{}
	execs          []string
	// released is closed by the test to unblock a pending write without
	// cancelling, so a test can prove the store releases Store.mu.
	released chan struct{}
}

func newProbeDriver() *probeDriver {
	return &probeDriver{
		catalog:        "complete",
		catalogStarted: make(chan struct{}),
		writeStarted:   make(chan struct{}),
		released:       make(chan struct{}),
	}
}

func (d *probeDriver) Open(string) (driver.Conn, error) { return &probeConn{driver: d}, nil }

func (d *probeDriver) record(query string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.execs = append(d.execs, query)
}

func (d *probeDriver) executed(fragment string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, query := range d.execs {
		if strings.Contains(query, fragment) {
			return true
		}
	}
	return false
}

func (d *probeDriver) takeCatalogFailure() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.catalogFailures > 0 {
		d.catalogFailures--
		return true
	}
	return false
}

type probeConn struct{ driver *probeDriver }

func (c *probeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("probe driver: prepared statements are not supported")
}

func (c *probeConn) Close() error { return nil }

func (c *probeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("probe driver: Begin is not supported; use BeginTx")
}

func (c *probeConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	return &probeTx{conn: c}, nil
}

// QueryContext answers the catalog probe and every read, and blocks on the
// scripted catalog-failure budget so a transient probe can be exercised.
func (c *probeConn) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.driver.record(query)
	if isCatalogProbe(query) {
		if c.driver.takeCatalogFailure() {
			closeOnce(c.driver.catalogStarted)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-c.driver.released:
				return nil, ctx.Err()
			}
		}
		return &singleValueRows{column: "status", value: c.driver.catalog}, nil
	}
	// Every read in these tests resolves to "no row": the control row, the
	// tenant hot document and the ledger are all empty on a fresh database.
	return &singleValueRows{column: "id"}, nil
}

func (c *probeConn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.driver.record(query)
	if c.driver.blockWrite && strings.Contains(query, "INSERT INTO audit_tenant") {
		closeOnce(c.driver.writeStarted)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.driver.released:
			return driver.RowsAffected(1), nil
		}
	}
	return driver.RowsAffected(1), nil
}

type probeTx struct{ conn *probeConn }

func (tx *probeTx) Commit() error { return nil }

func (tx *probeTx) Rollback() error { return nil }

func isCatalogProbe(query string) bool {
	return strings.Contains(query, "to_regclass('audit_tenant')")
}

// singleValueRows is a one-column result: one row carrying value, or zero
// rows when value is empty (the "table is empty" answer every read in these
// tests needs).
type singleValueRows struct {
	column string
	value  string
	done   bool
}

func (r *singleValueRows) Columns() []string { return []string{r.column} }

func (r *singleValueRows) Close() error { return nil }

func (r *singleValueRows) Next(dest []driver.Value) error {
	if r.done || r.value == "" {
		return io.EOF
	}
	r.done = true
	dest[0] = r.value
	return nil
}

// testDeadlineReceipt is the minimum durable fact the tenant closures above
// commit; only its presence matters, not its contents.
func testDeadlineReceipt(tenantID, eventID string) domain.EventReceipt {
	return domain.EventReceipt{EventID: eventID, TenantID: tenantID, Status: domain.StatusLedgered}
}

func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// openProbeStore registers a fresh driver name and opens a PostgreSQL store
// over it. Each test needs its own registration because database/sql's
// registry is process-global and append-only.
func openProbeStore(t *testing.T, d *probeDriver, opts Options) *Store {
	t.Helper()
	name := "probe-" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	sql.Register(name, d)
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	st, err := OpenPostgresWithOptions(db, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// TestUpdateTenantAbortsOnCancelledOperationContext pins A2: the store
// operation context really reaches the statement. Cancelling it aborts the
// in-flight write, the call returns promptly and never reports success, and
// Store.mu is released so the next operation proceeds.
func TestUpdateTenantAbortsOnCancelledOperationContext(t *testing.T) {
	driverUnderTest := newProbeDriver()
	driverUnderTest.blockWrite = true
	st := openProbeStore(t, driverUnderTest, Options{OperationTimeout: 30 * time.Second})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- st.UpdateTenant(ctx, "tenant-a", HotFirst, func(view *TenantView) error {
			view.Ledger.SetReceipt(testDeadlineReceipt("tenant-a", "evt-1"))
			return nil
		})
	}()
	select {
	case <-driverUnderTest.writeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("the store never issued the tenant hot-document write")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a cancelled store operation reported success")
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err=%v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a cancelled store operation did not return promptly")
	}
	// Store.mu must be free again: a read issued after the abort has to
	// complete. Under the old context-less statement this goroutine would
	// still hold the write lock and this read would hang behind it.
	readDone := make(chan error, 1)
	go func() { readDone <- st.Read(testCtx, func(*Snapshot) error { return nil }) }()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("read after an aborted operation: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Store.mu was not released by the aborted operation")
	}
	if st.OperationDeadlineExhaustions() != 0 {
		t.Fatalf("a caller cancellation must not be counted as a deadline exhaustion, got %d", st.OperationDeadlineExhaustions())
	}
}

// TestStoreOperationDeadlineFailsClosed pins A3: an operation that exhausts
// its own deadline returns ErrOperationDeadline (which also matches
// context.DeadlineExceeded), never success, commits nothing, and is counted
// exactly once — including when the caller is a nested store operation.
func TestStoreOperationDeadlineFailsClosed(t *testing.T) {
	driverUnderTest := newProbeDriver()
	driverUnderTest.blockWrite = true
	st := openProbeStore(t, driverUnderTest, Options{OperationTimeout: 50 * time.Millisecond})

	err := st.UpdateTenant(testCtx, "tenant-a", HotFirst, func(view *TenantView) error {
		view.Ledger.SetReceipt(testDeadlineReceipt("tenant-a", "evt-deadline"))
		return nil
	})
	if err == nil {
		t.Fatal("an operation that exhausted its deadline reported success")
	}
	if !errors.Is(err, ErrOperationDeadline) {
		t.Fatalf("err=%v, want errors.Is(err, ErrOperationDeadline)", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, want errors.Is(err, context.DeadlineExceeded)", err)
	}
	if got := st.OperationDeadlineExhaustions(); got != 1 {
		t.Fatalf("OperationDeadlineExhaustions()=%d, want exactly 1 (one exhaustion, one count)", got)
	}
	if driverUnderTest.executed("INSERT INTO audit_ledger") {
		t.Fatal("a timed-out tenant commit appended a cold ledger record")
	}
	if driverUnderTest.executed("INSERT INTO audit_state_snapshot") {
		t.Fatal("a timed-out tenant commit wrote the control plane")
	}
}

// TestStoreOperationDeadlineCountsOnceForNestedOperations pins the nesting
// rule: the legacy tenant fallback calls Store.Update inside UpdateTenant, and
// one expiry must still be counted once and classified once.
func TestStoreOperationDeadlineCountsOnceForNestedOperations(t *testing.T) {
	st := NewWithBackendOptions(&deadlineBackend{}, Options{OperationTimeout: 30 * time.Millisecond})
	if err := st.UpdateTenant(testCtx, "tenant-a", HotFirst, func(view *TenantView) error {
		view.Ledger.SetReceipt(testDeadlineReceipt("tenant-a", "evt-nested"))
		return nil
	}); !errors.Is(err, ErrOperationDeadline) {
		t.Fatalf("err=%v, want ErrOperationDeadline", err)
	}
	if got := st.OperationDeadlineExhaustions(); got != 1 {
		t.Fatalf("OperationDeadlineExhaustions()=%d, want exactly 1 for a nested operation", got)
	}
}

// deadlineBackend blocks on the operation context inside LoadForUpdate, so the
// nested (legacy) tenant path spends its whole budget inside one statement.
type deadlineBackend struct{}

func (b *deadlineBackend) Load(ctx context.Context) (*Snapshot, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *deadlineBackend) LoadForUpdate(ctx context.Context) (*Snapshot, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *deadlineBackend) Save(context.Context, *Snapshot) error { return nil }

// TestCatalogProbeContextFailureIsNotMemoized pins the F-D correctness fix.
//
// The hot/cold catalog verdict is memoized because it is a schema invariant.
// It is now probed under a caller-bounded context, so memoizing a probe that
// ended because that context ended would let one transient deadline on the
// first call fail every store operation in the process, with no retry.
func TestCatalogProbeContextFailureIsNotMemoized(t *testing.T) {
	driverUnderTest := newProbeDriver()
	driverUnderTest.catalogFailures = 1
	st := openProbeStore(t, driverUnderTest, Options{OperationTimeout: 40 * time.Millisecond})

	if err := st.Read(testCtx, func(*Snapshot) error { return nil }); !errors.Is(err, ErrOperationDeadline) {
		t.Fatalf("first read err=%v, want a deadline-classified catalog probe failure", err)
	}
	// The second operation must retry the probe rather than replay the
	// memoized failure.
	if err := st.Read(testCtx, func(*Snapshot) error { return nil }); err != nil {
		t.Fatalf("second read after a transient catalog failure: %v", err)
	}
}

// TestSleepBackoffReturnsOnCancel pins F-B: the optimistic-lock retry backoff
// runs while Store.mu is held, so it must be interruptible rather than a
// time.Sleep that can overrun the deadline by retries x backoff.
func TestSleepBackoffReturnsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	if err := sleepBackoff(ctx, 3); !errors.Is(err, context.Canceled) {
		t.Fatalf("sleepBackoff on a cancelled context: err=%v, want context.Canceled", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("sleepBackoff slept %v on an already-cancelled context", elapsed)
	}
	if err := sleepBackoff(testCtx, 0); err != nil {
		t.Fatalf("sleepBackoff on a live context: %v", err)
	}
}
