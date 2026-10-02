package runtimeconfig

// A5: the connection pool is operator-configured and NO numeric default is
// invented.
//
// "Unset means no setter call" is the load-bearing property: it is what makes
// the shipped pool byte-for-byte what database/sql would do on its own
// (MaxOpenConnections 0 = unlimited), instead of quietly serialising the
// process behind a guessed number. An invalid value must fail closed naming the
// variable, because a silently ignored typo is worse than a refusal to start.

import (
	"database/sql"
	"testing"
	"time"
)

func TestPoolConfigFromEnv(t *testing.T) {
	for _, name := range []string{
		EnvPostgresMaxOpenConns,
		EnvPostgresMaxIdleConns,
		EnvPostgresConnMaxLifetime,
		EnvPostgresConnMaxIdleTime,
		EnvStoreOperationTimeout,
	} {
		t.Setenv(name, "")
	}

	t.Run("unset-means-no-setter", func(t *testing.T) {
		cfg, err := PostgresPoolConfigFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Set() != 0 {
			t.Fatalf("Set()=%d, want 0 for a fully unset environment", cfg.Set())
		}
		db := &sql.DB{}
		if err := ApplyPostgresPoolConfig(db, cfg); err != nil {
			t.Fatal(err)
		}
		// A zero-value *sql.DB reports database/sql's own defaults, which is
		// exactly the "no setter was called" evidence this property needs.
		if got := db.Stats().MaxOpenConnections; got != 0 {
			t.Fatalf("MaxOpenConnections=%d, want 0 (unlimited: no setter call)", got)
		}
	})

	t.Run("every-knob-applied", func(t *testing.T) {
		t.Setenv(EnvPostgresMaxOpenConns, "11")
		t.Setenv(EnvPostgresMaxIdleConns, "4")
		t.Setenv(EnvPostgresConnMaxLifetime, "45m")
		t.Setenv(EnvPostgresConnMaxIdleTime, "15s")
		cfg, err := PostgresPoolConfigFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.MaxOpenConns != 11 || cfg.MaxIdleConns != 4 || cfg.ConnMaxLifetime != 45*time.Minute || cfg.ConnMaxIdleTime != 15*time.Second {
			t.Fatalf("resolved config=%+v, want every knob read from the environment", cfg)
		}
		if cfg.Set() != 4 {
			t.Fatalf("Set()=%d, want 4", cfg.Set())
		}
	})

	for _, bad := range []struct{ name, value, want string }{
		{EnvPostgresMaxOpenConns, "zero", EnvPostgresMaxOpenConns},
		{EnvPostgresMaxOpenConns, "0", EnvPostgresMaxOpenConns},
		{EnvPostgresMaxOpenConns, "-3", EnvPostgresMaxOpenConns},
		{EnvPostgresMaxIdleConns, "many", EnvPostgresMaxIdleConns},
		{EnvPostgresConnMaxLifetime, "forever", EnvPostgresConnMaxLifetime},
		{EnvPostgresConnMaxLifetime, "0s", EnvPostgresConnMaxLifetime},
		{EnvPostgresConnMaxIdleTime, "-1s", EnvPostgresConnMaxIdleTime},
	} {
		t.Run("invalid/"+bad.name+"="+bad.value, func(t *testing.T) {
			t.Setenv(bad.name, bad.value)
			if _, err := PostgresPoolConfigFromEnv(); err == nil {
				t.Fatalf("%s=%s was accepted", bad.name, bad.value)
			} else if !contains(err.Error(), bad.want) {
				t.Fatalf("error %q does not name %s", err, bad.want)
			}
		})
	}
}

func TestResolveStoreOperationTimeout(t *testing.T) {
	const fallback = 10 * time.Second
	t.Run("unset-selects-fallback", func(t *testing.T) {
		t.Setenv(EnvStoreOperationTimeout, "")
		got, err := ResolveStoreOperationTimeout(fallback)
		if err != nil || got != fallback {
			t.Fatalf("got %v, err %v; want %v, nil", got, err, fallback)
		}
	})
	t.Run("set-is-parsed", func(t *testing.T) {
		t.Setenv(EnvStoreOperationTimeout, "2500ms")
		got, err := ResolveStoreOperationTimeout(fallback)
		if err != nil || got != 2500*time.Millisecond {
			t.Fatalf("got %v, err %v; want 2.5s, nil", got, err)
		}
	})
	for _, value := range []string{"soon", "0", "-5s"} {
		t.Run("invalid/"+value, func(t *testing.T) {
			t.Setenv(EnvStoreOperationTimeout, value)
			_, err := ResolveStoreOperationTimeout(fallback)
			if err == nil {
				t.Fatalf("%s=%s was accepted", EnvStoreOperationTimeout, value)
			}
			if !contains(err.Error(), EnvStoreOperationTimeout) {
				t.Fatalf("error %q does not name %s", err, EnvStoreOperationTimeout)
			}
		})
	}
}

// TestValidateStoreOperationTimeoutBelow pins the startup guard: the retryable
// 503 a store timeout classifies as can only be written while the HTTP
// connection is alive, so a store deadline at or above the WriteTimeout would
// turn a documented retry into an opaque transport error. That has to be a
// startup failure, not a runtime surprise.
func TestValidateStoreOperationTimeoutBelow(t *testing.T) {
	const writeTimeout = 30 * time.Second
	if err := ValidateStoreOperationTimeoutBelow(10*time.Second, writeTimeout); err != nil {
		t.Fatalf("a 10s store deadline must be accepted under a 30s write timeout: %v", err)
	}
	for _, timeout := range []time.Duration{writeTimeout, 45 * time.Second, 0, -time.Second} {
		if err := ValidateStoreOperationTimeoutBelow(timeout, writeTimeout); err == nil {
			t.Fatalf("store deadline %v was accepted under a %v write timeout", timeout, writeTimeout)
		}
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (haystack == needle || indexOf(haystack, needle) >= 0)
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
