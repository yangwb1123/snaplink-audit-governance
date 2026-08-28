package auth

import (
	"strings"
	"testing"
	"time"
)

// baseClaims returns a minimal valid JWT payload for clock-skew tests.
func baseClaims(expOffset, nbfOffset time.Duration) map[string]any {
	payload := map[string]any{
		"sub":       "service-subject",
		"tenant_id": "tenant-a",
		"exp":       time.Now().Add(expOffset).Unix(),
	}
	if nbfOffset != 0 {
		payload["nbf"] = time.Now().Add(nbfOffset).Unix()
	}
	return payload
}

// AC-1: ClockSkew=30s accepts a token expired 15s ago.
func TestClockSkewAcceptsRecentlyExpiredToken(t *testing.T) {
	a := Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true, ClockSkew: 30 * time.Second}
	token := signJWT(t, baseClaims(-15*time.Second, 0))
	if _, err := a.AuthenticateToken(token); err != nil {
		t.Fatalf("expected within-tolerance token to be accepted: %v", err)
	}
}

// AC-2: ClockSkew=30s rejects a token expired 35s ago.
func TestClockSkewRejectsExpiredBeyondTolerance(t *testing.T) {
	a := Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true, ClockSkew: 30 * time.Second}
	token := signJWT(t, baseClaims(-35*time.Second, 0))
	_, err := a.AuthenticateToken(token)
	if err == nil || !strings.Contains(err.Error(), "token is expired") {
		t.Fatalf("expected beyond-tolerance token to be rejected, err=%v", err)
	}
}

// AC-3: ClockSkew=30s accepts a token with nbf 20s in the future.
func TestClockSkewAcceptsNotYetActiveWithinTolerance(t *testing.T) {
	a := Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true, ClockSkew: 30 * time.Second}
	payload := baseClaims(time.Hour, 20*time.Second)
	token := signJWT(t, payload)
	if _, err := a.AuthenticateToken(token); err != nil {
		t.Fatalf("expected within-tolerance nbf token to be accepted: %v", err)
	}
}

// AC-4: ClockSkew=30s rejects a token with nbf 35s in the future.
func TestClockSkewRejectsNotYetActiveBeyondTolerance(t *testing.T) {
	a := Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true, ClockSkew: 30 * time.Second}
	payload := baseClaims(time.Hour, 35*time.Second)
	token := signJWT(t, payload)
	_, err := a.AuthenticateToken(token)
	if err == nil || !strings.Contains(err.Error(), "token is not active") {
		t.Fatalf("expected beyond-tolerance nbf token to be rejected, err=%v", err)
	}
}

// AC-5: Default ClockSkew=0 preserves strict behaviour.
func TestDefaultClockSkewPreservesStrictBehaviour(t *testing.T) {
	a := Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true}

	// exp = now → must be rejected (strict zero tolerance).
	expiredToken := signJWT(t, baseClaims(0, 0))
	if _, err := a.AuthenticateToken(expiredToken); err == nil || !strings.Contains(err.Error(), "token is expired") {
		t.Fatalf("zero skew must reject exp=now, err=%v", err)
	}

	// nbf = now+1 → must be rejected.
	futurePayload := baseClaims(time.Hour, 1*time.Second)
	futureToken := signJWT(t, futurePayload)
	if _, err := a.AuthenticateToken(futureToken); err == nil || !strings.Contains(err.Error(), "token is not active") {
		t.Fatalf("zero skew must reject nbf=now+1, err=%v", err)
	}
}

// Boundary: exp = now - 30s with ClockSkew=30s → exactly at boundary → rejected.
func TestClockSkewExactBoundaryExp(t *testing.T) {
	a := Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true, ClockSkew: 30 * time.Second}
	token := signJWT(t, baseClaims(-30*time.Second, 0))
	_, err := a.AuthenticateToken(token)
	if err == nil || !strings.Contains(err.Error(), "token is expired") {
		t.Fatalf("exact boundary must reject: now >= exp+skew, err=%v", err)
	}
}

// Negative ClockSkew tightens validation: token expired 5s ago is rejected with -10s skew.
func TestNegativeClockSkewTightensExpValidation(t *testing.T) {
	a := Authenticator{JWTSecret: testHMACSecret, AllowLocalHS256: true, ClockSkew: -10 * time.Second}
	token := signJWT(t, baseClaims(-5*time.Second, 0))
	_, err := a.AuthenticateToken(token)
	if err == nil || !strings.Contains(err.Error(), "token is expired") {
		t.Fatalf("negative skew must tighten exp check, err=%v", err)
	}
}
