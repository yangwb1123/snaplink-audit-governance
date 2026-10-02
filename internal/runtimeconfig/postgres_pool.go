package runtimeconfig

// Operator-configured control-plane knobs that every PostgreSQL-using binary
// must resolve identically: the store operation deadline and the
// database/sql connection-pool parameters.
//
// Two rules govern this file.
//
//  1. No numeric default is invented. An UNSET pool variable applies NO
//     setter call at all, so the pool is byte-for-byte what database/sql
//     would do on its own (unlimited open connections, 2 idle, no lifetime).
//     /metrics then reports MaxOpenConnections 0, which reads as "no operator
//     limit" rather than hiding the un-tuned shape. A SET variable is
//     validated or the binary exits naming it.
//  2. One implementation, four binaries. audit-api, audit-governance-worker,
//     audit-outbox-relay and audit-pg-migrate all call these helpers, so the
//     cutover command cannot run with a different pool than the servers.

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Environment variable names shared by every PostgreSQL-serving binary.
const (
	// EnvStoreOperationTimeout bounds one store operation (Options.OperationTimeout).
	EnvStoreOperationTimeout = "AUDIT_STORE_OPERATION_TIMEOUT"
	// The four connection-pool knobs. Unset means "leave database/sql's own
	// default alone"; set means "validate and apply".
	EnvPostgresMaxOpenConns    = "AUDIT_POSTGRES_MAX_OPEN_CONNS"
	EnvPostgresMaxIdleConns    = "AUDIT_POSTGRES_MAX_IDLE_CONNS"
	EnvPostgresConnMaxLifetime = "AUDIT_POSTGRES_CONN_MAX_LIFETIME"
	EnvPostgresConnMaxIdleTime = "AUDIT_POSTGRES_CONN_MAX_IDLE_TIME"
)

// PoolConfig is the resolved pool configuration. A zero/empty field means the
// operator did not set that variable, and ApplyPostgresPoolConfig must make
// no setter call for it (see rule 1 above).
type PoolConfig struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// Set reports how many setters the configuration will apply. It exists so the
// tests (and an operator reading the code) can assert that "unset" really is
// "no call" rather than "call with a value that happens to match the default".
func (c PoolConfig) Set() int {
	set := 0
	for _, present := range []bool{c.MaxOpenConns > 0, c.MaxIdleConns > 0, c.ConnMaxLifetime > 0, c.ConnMaxIdleTime > 0} {
		if present {
			set++
		}
	}
	return set
}

// ResolveStoreOperationTimeout returns the store operation deadline. An unset
// or blank variable selects fallback; a set value must parse as a Go duration
// and be strictly positive. A non-positive value would silently remove the
// interruptible bound that makes every request-path statement abortable, so it
// is rejected by name rather than accepted.
func ResolveStoreOperationTimeout(fallback time.Duration) (time.Duration, error) {
	raw, ok := os.LookupEnv(EnvStoreOperationTimeout)
	if !ok || raw == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a Go duration such as 10s or 2m: %w", EnvStoreOperationTimeout, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %s", EnvStoreOperationTimeout, raw)
	}
	return parsed, nil
}

// PostgresPoolConfigFromEnv resolves the four pool knobs from the
// environment. An unset variable leaves its field at zero, which Apply skips
// entirely.
func PostgresPoolConfigFromEnv() (PoolConfig, error) {
	var cfg PoolConfig
	var err error
	if cfg.MaxOpenConns, err = positiveEnvInt(EnvPostgresMaxOpenConns); err != nil {
		return PoolConfig{}, err
	}
	if cfg.MaxIdleConns, err = positiveEnvInt(EnvPostgresMaxIdleConns); err != nil {
		return PoolConfig{}, err
	}
	if cfg.ConnMaxLifetime, err = positiveEnvDuration(EnvPostgresConnMaxLifetime); err != nil {
		return PoolConfig{}, err
	}
	if cfg.ConnMaxIdleTime, err = positiveEnvDuration(EnvPostgresConnMaxIdleTime); err != nil {
		return PoolConfig{}, err
	}
	return cfg, nil
}

// ApplyPostgresPoolConfig applies cfg to db, skipping every unset knob. An
// unset pool is therefore identical to the pre-change behaviour, and the
// /metrics saturation signals (audit_store_pg_pool_*) are what make a future
// numeric choice evidence-based instead of guessed.
func ApplyPostgresPoolConfig(db *sql.DB, cfg PoolConfig) error {
	if db == nil {
		return fmt.Errorf("apply PostgreSQL pool configuration: nil database handle")
	}
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}
	if cfg.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	}
	return nil
}

// ApplyPostgresPoolConfigFromEnv resolves and applies in one step; it is the
// call every binary makes right after sql.Open.
func ApplyPostgresPoolConfigFromEnv(db *sql.DB) error {
	cfg, err := PostgresPoolConfigFromEnv()
	if err != nil {
		return err
	}
	return ApplyPostgresPoolConfig(db, cfg)
}

func positiveEnvInt(name string) (int, error) {
	raw, ok := os.LookupEnv(name)
	if !ok || raw == "" {
		return 0, nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", name, raw)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %d", name, parsed)
	}
	return parsed, nil
}

func positiveEnvDuration(name string) (time.Duration, error) {
	raw, ok := os.LookupEnv(name)
	if !ok || raw == "" {
		return 0, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a Go duration such as 30m or 90s: %w", name, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s must be positive, got %s", name, raw)
	}
	return parsed, nil
}

// ValidateStoreOperationTimeoutBelow rejects a store deadline that is not
// strictly below limit.
//
// The HTTP boundary can only write the retryable 503 that a store timeout
// classifies as if the connection is still alive when it does so. audit-api's
// WriteTimeout bounds the socket, so a store deadline at or above it would
// let the connection die before the retryable status reaches the client —
// turning a documented retry into an opaque transport error. Failing at
// startup is the only place this is recoverable.
func ValidateStoreOperationTimeoutBelow(timeout, limit time.Duration) error {
	if timeout <= 0 {
		return fmt.Errorf("%s must be positive", EnvStoreOperationTimeout)
	}
	if timeout >= limit {
		return fmt.Errorf("%s=%s must be shorter than the %s HTTP write timeout, otherwise the retryable status cannot reach the client", EnvStoreOperationTimeout, timeout, limit)
	}
	return nil
}

// postgresStartupPingTimeout bounds the boot reachability probe shared by the
// PostgreSQL binaries. A black-holed DSN must fail the command, not hang it
// forever — the same class of bug as an unbounded statement on the request
// path.
const postgresStartupPingTimeout = 10 * time.Second

// PingPostgresStartup probes db under a bounded context derived from ctx. It
// replaces the unbounded db.Ping() the migration command used, which could
// block that command indefinitely.
func PingPostgresStartup(ctx context.Context, db *sql.DB) error {
	pingCtx, cancel := context.WithTimeout(ctx, postgresStartupPingTimeout)
	defer cancel()
	return db.PingContext(pingCtx)
}
