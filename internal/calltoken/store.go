// Package calltoken implements the Phase D1 caller_token issuance and
// validation layer. quant-handler attaches the token to outbound
// strategy-runtime calls as `x-caller-token` metadata; the runtime's
// gRPC interceptor calls ValidateCallerToken on control-panel-service
// to confirm the caller has been authorized for this runtime.
//
// D3 will retire this layer once the control-panel proxy supersedes
// per-call attestation. Documented as D3-removable.
package calltoken

import (
	"sync"
	"time"
)

// Binding is the data associated with a single issued caller_token.
type Binding struct {
	UserID    int64
	RuntimeID string
	ExpiresAt time.Time
}

// ValidationReason explains why a Validate call returned valid=false.
type ValidationReason string

const (
	ReasonOK              ValidationReason = ""
	ReasonExpired         ValidationReason = "expired"
	ReasonUnknown         ValidationReason = "unknown"
	ReasonRuntimeMismatch ValidationReason = "runtime_mismatch"
)

// Store is the process-local in-memory caller_token table. Issue puts a
// (token → binding) entry; Validate looks one up. Expired entries are
// rejected on Validate AND swept opportunistically; the sweep keeps
// memory bounded under sustained traffic without paying a goroutine.
//
// Concurrency: a single mutex covers all access. The expected QPS is
// "one issue + one validate per strategy call" so a fancier sharded
// map is unnecessary in D1.
//
// Memory bound: Issue triggers a sweep every IssueSweepInterval calls
// (default 256). Worst-case the map holds N issues during a sweep
// window even if all are immediately expired; with default 60s TTL
// and reasonable issue rate this stays bounded.
type Store struct {
	mu         sync.Mutex
	entries    map[string]Binding
	now        func() time.Time
	issueCnt   int
	sweepEvery int
}

// NewStore returns an empty caller_token store with the supplied clock.
// Pass time.Now in production; tests inject a deterministic clock.
func NewStore(now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return &Store{
		entries:    make(map[string]Binding),
		now:        now,
		sweepEvery: 256,
	}
}

// Issue records token → binding. The caller (service layer) must
// generate the token via auth.GenerateOpaqueToken so secrecy is not
// this package's concern.
//
// Behavior on duplicate token: overwrites. Tokens are 24 random bytes
// (auth.GenerateOpaqueToken) so collision is astronomical; overwrite
// is the simplest safe behavior.
func (s *Store) Issue(token string, b Binding) {
	if token == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[token] = b
	s.issueCnt++
	if s.issueCnt >= s.sweepEvery {
		s.issueCnt = 0
		s.sweepLocked(s.now())
	}
}

// Validate looks up a token, checks the runtime binding, and rejects
// expired entries. Returns user_id on success.
func (s *Store) Validate(token, runtimeID string) (userID int64, valid bool, reason ValidationReason) {
	if token == "" {
		return 0, false, ReasonUnknown
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.entries[token]
	if !ok {
		return 0, false, ReasonUnknown
	}
	if !s.now().Before(b.ExpiresAt) {
		// Sweep this expired entry while we hold the lock.
		delete(s.entries, token)
		return 0, false, ReasonExpired
	}
	if runtimeID != "" && b.RuntimeID != runtimeID {
		return 0, false, ReasonRuntimeMismatch
	}
	return b.UserID, true, ReasonOK
}

// Len returns the current entry count. Test introspection.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

// sweepLocked removes all expired entries. Caller must hold s.mu.
func (s *Store) sweepLocked(now time.Time) {
	for k, b := range s.entries {
		if !now.Before(b.ExpiresAt) {
			delete(s.entries, k)
		}
	}
}

// Sweep is the explicit GC entrypoint for tests / shutdown.
func (s *Store) Sweep() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweepLocked(s.now())
}
