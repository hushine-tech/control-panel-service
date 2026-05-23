package calltoken

import (
	"testing"
	"time"
)

func TestStore_IssueValidate_HappyPath(t *testing.T) {
	now := time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC)
	s := NewStore(func() time.Time { return now })
	s.Issue("tok_a", Binding{UserID: 42, RuntimeID: "rt_x", ExpiresAt: now.Add(60 * time.Second)})

	uid, valid, reason := s.Validate("tok_a", "rt_x")
	if !valid {
		t.Fatalf("Validate: valid=false reason=%q, want valid", reason)
	}
	if uid != 42 {
		t.Errorf("user_id = %d, want 42", uid)
	}
}

func TestStore_Validate_UnknownToken(t *testing.T) {
	s := NewStore(func() time.Time { return time.Now() })
	_, valid, reason := s.Validate("bogus", "rt_x")
	if valid {
		t.Fatal("valid=true for unknown token")
	}
	if reason != ReasonUnknown {
		t.Errorf("reason = %q, want %q", reason, ReasonUnknown)
	}
}

func TestStore_Validate_EmptyToken(t *testing.T) {
	s := NewStore(func() time.Time { return time.Now() })
	_, valid, reason := s.Validate("", "rt_x")
	if valid {
		t.Fatal("valid=true for empty token")
	}
	if reason != ReasonUnknown {
		t.Errorf("reason = %q, want %q", reason, ReasonUnknown)
	}
}

func TestStore_Validate_Expired(t *testing.T) {
	t0 := time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC)
	clock := t0
	s := NewStore(func() time.Time { return clock })
	s.Issue("tok_a", Binding{UserID: 42, RuntimeID: "rt_x", ExpiresAt: t0.Add(60 * time.Second)})

	// Advance past expiry.
	clock = t0.Add(61 * time.Second)
	_, valid, reason := s.Validate("tok_a", "rt_x")
	if valid {
		t.Fatal("valid=true after expiry")
	}
	if reason != ReasonExpired {
		t.Errorf("reason = %q, want %q", reason, ReasonExpired)
	}
	// Expired entry should have been swept by Validate.
	if s.Len() != 0 {
		t.Errorf("expired entry not swept on Validate; len=%d", s.Len())
	}
}

func TestStore_Validate_RuntimeMismatch(t *testing.T) {
	t0 := time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC)
	s := NewStore(func() time.Time { return t0 })
	s.Issue("tok_a", Binding{UserID: 42, RuntimeID: "rt_x", ExpiresAt: t0.Add(60 * time.Second)})

	_, valid, reason := s.Validate("tok_a", "rt_y")
	if valid {
		t.Fatal("valid=true for wrong runtime_id")
	}
	if reason != ReasonRuntimeMismatch {
		t.Errorf("reason = %q, want %q", reason, ReasonRuntimeMismatch)
	}
	// Token still valid for the right runtime.
	uid, valid, _ := s.Validate("tok_a", "rt_x")
	if !valid || uid != 42 {
		t.Errorf("subsequent Validate(rt_x) failed: valid=%v uid=%d", valid, uid)
	}
}

// runtime_id="" in Validate is a reduced check used by tests / debug
// surfaces; the public RPC always provides runtime_id so the
// permissive path is intentional.
func TestStore_Validate_EmptyRuntimeIDAcceptsAnyBinding(t *testing.T) {
	t0 := time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC)
	s := NewStore(func() time.Time { return t0 })
	s.Issue("tok_a", Binding{UserID: 42, RuntimeID: "rt_x", ExpiresAt: t0.Add(60 * time.Second)})
	uid, valid, _ := s.Validate("tok_a", "")
	if !valid || uid != 42 {
		t.Errorf("Validate with empty runtime_id failed: valid=%v uid=%d", valid, uid)
	}
}

func TestStore_Sweep_RemovesExpiredEntries(t *testing.T) {
	t0 := time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC)
	clock := t0
	s := NewStore(func() time.Time { return clock })
	s.Issue("tok_a", Binding{UserID: 1, RuntimeID: "rt_a", ExpiresAt: t0.Add(60 * time.Second)})
	s.Issue("tok_b", Binding{UserID: 2, RuntimeID: "rt_b", ExpiresAt: t0.Add(120 * time.Second)})
	s.Issue("tok_c", Binding{UserID: 3, RuntimeID: "rt_c", ExpiresAt: t0.Add(10 * time.Second)})

	clock = t0.Add(70 * time.Second)
	s.Sweep()
	if s.Len() != 1 {
		t.Errorf("len = %d, want 1 (only tok_b should remain)", s.Len())
	}
}

func TestStore_Issue_AutoSweepEvery256(t *testing.T) {
	t0 := time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC)
	clock := t0
	s := NewStore(func() time.Time { return clock })
	// 100 entries that expire at t0+10s.
	for i := 0; i < 100; i++ {
		s.Issue(string(rune('a'+i%26))+"-"+string(rune('A'+i/26)),
			Binding{UserID: int64(i + 1), RuntimeID: "rt", ExpiresAt: t0.Add(10 * time.Second)})
	}
	// Advance past expiry.
	clock = t0.Add(20 * time.Second)
	// Issue ~256 more entries to trigger an auto-sweep.
	for i := 0; i < 260; i++ {
		s.Issue("fresh-"+string(rune(i)), Binding{UserID: 999, RuntimeID: "rt", ExpiresAt: clock.Add(60 * time.Second)})
	}
	// All 100 originally-issued entries should be gone after auto-sweep.
	// Remaining ≈ 260 fresh entries.
	if s.Len() < 200 || s.Len() > 270 {
		t.Errorf("len after auto-sweep = %d, want roughly 260", s.Len())
	}
}
