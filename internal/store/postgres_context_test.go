package store

// The gate for the request-path context invariant.
//
// Every PostgreSQL statement on the request path runs while the process-wide
// Store.mu is held, so a context-less statement is a statement that a dropped
// connection, a cancelled request or a black-holed connection can never end.
// These tests scan the whole package with go/parser instead of trusting a
// grep, so the invariant is checked structurally (a selector call on a
// *sql.DB / *sql.Tx receiver) and fails on any file added later.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// contextlessStatementCalls are the *sql.DB / *sql.Tx entry points that take
// no context. They are listed individually rather than as a prefix rule
// because their context-carrying twins (QueryContext, QueryRowContext,
// ExecContext, BeginTx) must stay legal.
var contextlessStatementCalls = map[string]bool{
	"Query":    true,
	"QueryRow": true,
	"Exec":     true,
	"Begin":    true,
	"Prepare":  true,
}

// contextVariants must all appear in the package. Without this the scanner
// would pass vacuously if the whole package were ever rewritten to a form it
// does not recognise.
var contextVariants = map[string]bool{
	"QueryContext":    false,
	"QueryRowContext": false,
	"ExecContext":     false,
}

// contextlessContextCalls are the context factories that silently detach a
// request from its caller. The store package must derive its operation
// context from the context it is handed, never manufacture a fresh root.
var contextlessContextCalls = map[string]bool{
	"Background": true,
	"TODO":       true,
}

// scanFile walks one Go source file and reports every violation it finds: a
// context-less database/sql statement call on a *sql.DB / *sql.Tx receiver.
func scanFile(t *testing.T, path string, source []byte) []string {
	t.Helper()
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, path, source, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	handles := sqlHandleNames(parsed)
	var findings []string
	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if variant, tracked := contextVariants[sel.Sel.Name]; tracked {
			contextVariants[sel.Sel.Name] = variant || isSQLReceiver(sel.X, handles)
			return true
		}
		if contextlessStatementCalls[sel.Sel.Name] && isSQLReceiver(sel.X, handles) {
			findings = append(findings, filepath.Base(path)+":"+strconv.Itoa(fileSet.Position(sel.Pos()).Line)+
				": context-less "+sel.Sel.Name+" call: use "+sel.Sel.Name+"Context (or BeginTx)")
		}
		return true
	})
	return findings
}

// sqlHandleNames collects every identifier in the file whose declared type is
// *sql.DB or *sql.Tx — variables, struct fields and function parameters alike —
// so a rename (db -> handle) cannot smuggle a context-less statement past the
// gate.
func sqlHandleNames(parsed *ast.File) map[string]bool {
	handles := map[string]bool{}
	ast.Inspect(parsed, func(node ast.Node) bool {
		field, ok := node.(*ast.Field)
		if !ok || !isSQLType(field.Type) {
			return true
		}
		for _, name := range field.Names {
			handles[name.Name] = true
		}
		return true
	})
	return handles
}

// isSQLReceiver reports whether expr is recognisably a *sql.DB or *sql.Tx:
// a declared handle (including a function parameter), a *sql.DB / *sql.Tx
// typed variable, or a field chain that reaches a handle-shaped field such as
// p.backend.db.
func isSQLReceiver(expr ast.Expr, handles map[string]bool) bool {
	switch typed := expr.(type) {
	case *ast.Ident:
		if handles[typed.Name] {
			return true
		}
		if typed.Obj == nil {
			return false
		}
		declared, _ := typed.Obj.Type.(ast.Expr)
		return isSQLType(declared)
	case *ast.SelectorExpr:
		switch typed.Sel.Name {
		case "db", "tx", "conn", "pool":
			return true
		}
		return isSQLReceiver(typed.X, handles)
	}
	return false
}

func isSQLType(expr ast.Expr) bool {
	pointer, ok := expr.(*ast.StarExpr)
	if !ok {
		return false
	}
	selector, ok := pointer.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	return ok && pkg.Name == "sql" && (selector.Sel.Name == "DB" || selector.Sel.Name == "Tx")
}

// TestStoreStatementsUseContextVariants pins A1: no context-less
// database/sql statement call may exist in any non-test file of package store.
//
// The same scanner is self-tested below against an inline fixture that DOES
// contain a violation, so this test cannot pass by the scanner silently
// matching nothing.
func TestStoreStatementsUseContextVariants(t *testing.T) {
	resetContextVariantProbe()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	scanned := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, readErr := os.ReadFile(name)
		if readErr != nil {
			t.Fatal(readErr)
		}
		scanned++
		if findings := scanFile(t, name, source); len(findings) > 0 {
			t.Errorf("context-less statement calls in %s:\n  %s", name, strings.Join(findings, "\n  "))
		}
	}
	if scanned == 0 {
		t.Fatal("no non-test Go files scanned; the gate would pass vacuously")
	}
	for name, seen := range contextVariants {
		if !seen {
			t.Errorf("scanner never observed a %s call; the context-variant assertion cannot pass vacuously", name)
		}
	}
}

// TestStoreScannerDetectsContextlessStatements self-tests the scanner used by
// TestStoreStatementsUseContextVariants. A gate that cannot fail is not a
// gate, so the same function must reject a fixture with a context-less call
// and accept the ctx-carrying twin.
func TestStoreScannerDetectsContextlessStatements(t *testing.T) {
	bad := `package fixture

import "database/sql"

func bad(db *sql.DB, tx *sql.Tx) error {
	if _, err := db.QueryRow("SELECT 1"); err != nil {
		return err
	}
	if _, err := db.Exec("DELETE FROM t"); err != nil {
		return err
	}
	if _, err := tx.Begin(); err != nil {
		return err
	}
	return nil
}
`
	good := `package fixture

import (
	"context"
	"database/sql"
)

func good(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM t"); err != nil {
		return err
	}
	// Commit/Rollback are deliberately NOT flagged: database/sql has no
	// context variant for them; a transaction is bounded by the context of
	// the statements and of BeginTx.
	if err := tx.Commit(); err != nil {
		return err
	}
	row := db.QueryRowContext(ctx, "SELECT 1")
	return row.Err()
}
`
	resetContextVariantProbe()
	if findings := scanFile(t, "bad_fixture.go", []byte(bad)); len(findings) != 3 {
		t.Fatalf("scanner missed context-less calls: got %v, want 3 findings", findings)
	}
	resetContextVariantProbe()
	if findings := scanFile(t, "good_fixture.go", []byte(good)); len(findings) != 0 {
		t.Fatalf("scanner flagged ctx-carrying calls: %v", findings)
	}
}

// resetContextVariantProbe clears the "did the scanner see a ctx variant"
// bookkeeping. It is called explicitly rather than relying on package var
// initialisation so a test that runs before the scan cannot leave stale
// state behind.
func resetContextVariantProbe() {
	for name := range contextVariants {
		contextVariants[name] = false
	}
}
