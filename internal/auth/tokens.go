// Package auth holds the token primitives used by control-panel-service:
// random ID generation, secret-token generation, and SHA-256 hashing for
// at-rest storage.
//
// D1 deliberately uses HMAC-less opaque random tokens — verification is
// "compare incoming hash to stored hash". Stronger schemes (mTLS, signed
// JWT) are deferred per the Phase D1 design Resolved Decisions.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
)

// GenerateRuntimeID returns a 12-byte random hex string used as
// runtime_registry.runtime_id when the caller didn't provide one.
func GenerateRuntimeID() string {
	return "rt-" + randomHex(12)
}

// GenerateRegistrationToken returns a 32-byte random hex string used as the
// registration / session token returned to a runtime. Treat as a secret.
func GenerateRegistrationToken() string {
	return randomHex(32)
}

// GenerateOpaqueToken returns a 24-byte random hex string used for
// short-lived caller tokens. Verification mechanics are wired in section 6.
func GenerateOpaqueToken() string {
	return randomHex(24)
}

// HashToken returns the SHA-256 hex of the supplied token.
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		panic("auth: rand.Read failed: " + err.Error())
	}
	return hex.EncodeToString(buf)
}
