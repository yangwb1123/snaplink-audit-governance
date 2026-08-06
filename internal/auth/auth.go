package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type Claims struct {
	Subject     string
	ClientID    string
	TenantID    string
	Roles       []string
	Permissions map[string]bool
	Platform    bool
	Service     bool
}

type Authenticator struct {
	JWTSecret                 string
	JWTPublicKeyPEM           string
	JWTPublicKeyAlgorithm     string
	JWKSURL                   string
	JWKSHTTPClient            *http.Client
	Issuer                    string
	Audience                  string
	AllowDev                  bool
	AllowLocalHS256           bool
	AllowInsecureJWKSLoopback bool
}

func (a Authenticator) Authenticate(r *http.Request) (Claims, error) {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(header, "Bearer ") {
		return Claims{}, fmt.Errorf("authorization header is required")
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
	return a.AuthenticateTokenContext(r.Context(), token)
}

func (a Authenticator) AuthenticateToken(token string) (Claims, error) {
	return a.AuthenticateTokenContext(context.Background(), token)
}

func (a Authenticator) AuthenticateTokenContext(ctx context.Context, token string) (Claims, error) {
	if err := a.ValidateConfiguration(); err != nil {
		return Claims{}, err
	}
	if a.AllowDev && strings.HasPrefix(token, "dev:") {
		return parseDevToken(token)
	}
	return a.parseJWT(ctx, token)
}

func parseDevToken(token string) (Claims, error) {
	parts := strings.Split(token, ":")
	if len(parts) < 3 || len(parts) > 4 || parts[1] == "" || parts[2] == "" {
		return Claims{}, fmt.Errorf("invalid development token")
	}
	roles := strings.Split(parts[2], ",")
	clientID := parts[1]
	if len(parts) == 4 {
		clientID = parts[3]
	}
	if clientID == "" {
		return Claims{}, fmt.Errorf("invalid development token")
	}
	claims := Claims{Subject: parts[1], ClientID: clientID, TenantID: parts[1], Roles: roles, Permissions: map[string]bool{}}
	if parts[1] == "platform" || contains(roles, "platform-admin") {
		claims.Platform = true
	}
	if contains(roles, "service") || contains(roles, "event-writer") {
		claims.Service = true
	}
	claims.Permissions = permissionsForRoles(roles)
	return claims, nil
}

func (a Authenticator) parseJWT(ctx context.Context, token string) (Claims, error) {
	verifiedPayload, err := a.verifyJWT(ctx, token)
	if err != nil {
		return Claims{}, err
	}
	var payload map[string]any
	if err := json.Unmarshal(verifiedPayload, &payload); err != nil {
		return Claims{}, fmt.Errorf("invalid token claims: %w", err)
	}
	claims := Claims{Permissions: map[string]bool{}}
	now := time.Now().Unix()
	if expiry, ok := numericClaim(payload, "exp"); ok && now >= int64(expiry) {
		return Claims{}, fmt.Errorf("token is expired")
	}
	if notBefore, ok := numericClaim(payload, "nbf"); ok && now < int64(notBefore) {
		return Claims{}, fmt.Errorf("token is not active")
	}
	if a.Issuer != "" && stringClaim(payload, "iss") != a.Issuer {
		return Claims{}, fmt.Errorf("token issuer mismatch")
	}
	if a.Audience != "" && !audienceClaim(payload, a.Audience) {
		return Claims{}, fmt.Errorf("token audience mismatch")
	}
	subject, hasSubject, err := strictIdentityClaim(payload, "sub")
	if err != nil {
		return Claims{}, err
	}
	if !hasSubject {
		return Claims{}, fmt.Errorf("token must contain sub")
	}
	claims.Subject = subject
	claims.ClientID, err = clientIdentity(payload)
	if err != nil {
		return Claims{}, err
	}
	claims.TenantID = stringClaim(payload, "tenant_id")
	if claims.TenantID == "" {
		claims.TenantID = stringClaim(payload, "tenant")
	}
	claims.Roles = stringSliceClaim(payload, "roles")
	claims.Permissions = permissionsForRoles(claims.Roles)
	for _, permission := range stringSliceClaim(payload, "permissions") {
		claims.Permissions[permission] = true
	}
	if stringClaim(payload, "scope") != "" {
		for _, permission := range strings.Fields(stringClaim(payload, "scope")) {
			claims.Permissions[permission] = true
		}
	}
	claims.Platform = claims.Permissions["audit:platform:cross_tenant"] || contains(claims.Roles, "platform-admin")
	claims.Service = contains(claims.Roles, "service") || contains(claims.Roles, "event-writer")
	return claims, nil
}

func clientIdentity(payload map[string]any) (string, error) {
	clientID, hasClientID, err := strictIdentityClaim(payload, "client_id")
	if err != nil {
		return "", err
	}
	authorizedParty, hasAuthorizedParty, err := strictIdentityClaim(payload, "azp")
	if err != nil {
		return "", err
	}
	if hasClientID && hasAuthorizedParty && clientID != authorizedParty {
		return "", fmt.Errorf("token client identity mismatch")
	}
	if hasClientID {
		return clientID, nil
	}
	return authorizedParty, nil
}

func strictIdentityClaim(payload map[string]any, name string) (string, bool, error) {
	value, present := payload[name]
	if !present {
		return "", false, nil
	}
	identity, ok := value.(string)
	if !ok || identity == "" || identity != strings.TrimSpace(identity) {
		return "", false, fmt.Errorf("token %s claim is invalid", name)
	}
	return identity, true, nil
}

func (c Claims) Allows(permission string) bool {
	return c.Permissions[permission] || c.Platform
}

func permissionsForRoles(roles []string) map[string]bool {
	permissions := map[string]bool{}
	for _, role := range roles {
		switch role {
		case "service", "event-writer":
			permissions["audit:event:write"] = true
		case "auditor", "tenant-auditor":
			permissions["audit:event:read"] = true
			permissions["audit:operation:read"] = true
		case "compliance":
			permissions["audit:event:read"] = true
			permissions["audit:operation:read"] = true
			permissions["audit:export:create"] = true
			permissions["audit:integrity:verify"] = true
			permissions["audit:legal_hold:manage"] = true
		case "tenant-admin":
			permissions["audit:event:read"] = true
			permissions["audit:operation:read"] = true
			permissions["audit:export:create"] = true
			permissions["audit:integrity:verify"] = true
			permissions["audit:legal_hold:manage"] = true
			permissions["audit:policy:read"] = true
			permissions["audit:policy:write"] = true
		case "platform-admin":
			permissions["audit:platform:cross_tenant"] = true
			permissions["audit:event:read"] = true
			permissions["audit:event:write"] = true
			permissions["audit:export:create"] = true
			permissions["audit:integrity:verify"] = true
			permissions["audit:legal_hold:manage"] = true
			permissions["audit:policy:read"] = true
			permissions["audit:policy:write"] = true
		}
	}
	return permissions
}

func stringClaim(values map[string]any, name string) string {
	value, _ := values[name].(string)
	return value
}

func stringSliceClaim(values map[string]any, name string) []string {
	var result []string
	switch value := values[name].(type) {
	case []any:
		for _, item := range value {
			if text, ok := item.(string); ok {
				result = append(result, text)
			}
		}
	case string:
		result = strings.Fields(value)
	}
	return result
}

func numericClaim(values map[string]any, name string) (float64, bool) {
	value, ok := values[name].(float64)
	return value, ok
}

func audienceClaim(values map[string]any, wanted string) bool {
	switch value := values["aud"].(type) {
	case string:
		return value == wanted
	case []any:
		for _, item := range value {
			if text, ok := item.(string); ok && text == wanted {
				return true
			}
		}
	}
	return false
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
