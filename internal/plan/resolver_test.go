package plan

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hushine-tech/control-panel-service/internal/config"
)

type stubLookup struct {
	planCode string
	err      error
}

func (s stubLookup) GetUserPlanCode(_ context.Context, _ int64) (string, error) {
	return s.planCode, s.err
}

func makeResolver(plans map[string]config.RuntimePlan, defaultPlan string, platform config.RuntimePlatformConfig, lookup PlanLookup) *Resolver {
	if platform.DefaultPlanCode == "" {
		platform.DefaultPlanCode = defaultPlan
	}
	return NewResolver(lookup, plans, platform)
}

func TestResolve_PicksUserPlan(t *testing.T) {
	plans := map[string]config.RuntimePlan{
		"free": {MaxHostedRuntimes: 1, MaxConcurrentSessionsTotal: 2},
		"pro":  {MaxHostedRuntimes: 5, MaxConcurrentSessionsTotal: 20},
	}
	// Platform side -1 = unlimited so we isolate the per-plan limit.
	platform := config.RuntimePlatformConfig{
		DefaultPlanCode:            "free",
		MaxTotalHostedRuntimes:     -1,
		MaxTotalSelfHostedRuntimes: -1,
	}
	r := makeResolver(plans, "", platform, stubLookup{planCode: "pro"})
	got, err := r.Resolve(context.Background(), 42)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.PlanCode != "pro" {
		t.Errorf("PlanCode = %q, want pro", got.PlanCode)
	}
	if got.MaxHostedRuntimes != 5 {
		t.Errorf("MaxHostedRuntimes = %d, want 5", got.MaxHostedRuntimes)
	}
}

// TestResolve_LookupNotFoundFailsClosed proves the fail-closed contract.
// Previously this returned the default plan silently; the fix forwards
// the absent-user error so callers (RegisterRuntime / ResolveRuntimeRoute)
// can refuse the request rather than allocate quota to a missing user.
func TestResolve_LookupNotFoundFailsClosed(t *testing.T) {
	plans := map[string]config.RuntimePlan{
		"free": {MaxHostedRuntimes: 1},
	}
	platform := config.RuntimePlatformConfig{DefaultPlanCode: "free"}
	r := makeResolver(plans, "", platform,
		stubLookup{err: status.Error(codes.NotFound, "user not found")})

	_, err := r.Resolve(context.Background(), 42)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, ErrUserNotFound) {
		t.Errorf("err = %v, want ErrUserNotFound", err)
	}
}

// TestResolve_LookupUnavailableFailsClosed: core-service down /
// network blip / Internal error → fail closed. Don't grant default plan
// access during a control-plane outage.
func TestResolve_LookupUnavailableFailsClosed(t *testing.T) {
	plans := map[string]config.RuntimePlan{
		"free": {MaxHostedRuntimes: 1},
	}
	platform := config.RuntimePlatformConfig{DefaultPlanCode: "free"}
	r := makeResolver(plans, "", platform,
		stubLookup{err: status.Error(codes.Unavailable, "core-service down")})

	_, err := r.Resolve(context.Background(), 42)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, ErrPlanLookupUnavailable) {
		t.Errorf("err = %v, want ErrPlanLookupUnavailable", err)
	}
}

// Plain (non-gRPC) errors should also be treated as Unavailable, not as
// "user missing". This guards against accidental misclassification.
func TestResolve_PlainErrorTreatedAsUnavailable(t *testing.T) {
	plans := map[string]config.RuntimePlan{
		"free": {MaxHostedRuntimes: 1},
	}
	platform := config.RuntimePlatformConfig{DefaultPlanCode: "free"}
	r := makeResolver(plans, "", platform, stubLookup{err: errors.New("network blip")})

	_, err := r.Resolve(context.Background(), 42)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !errors.Is(err, ErrPlanLookupUnavailable) {
		t.Errorf("err = %v, want ErrPlanLookupUnavailable", err)
	}
}

// TestResolve_EmptyPlanCodeFallsBackToDefault: the migration window
// scenario. Lookup returns OK but plan_code is empty (legacy row written
// before migration 0011 backfill); resolver falls back to platform
// default. NOT a fail-open path — the lookup itself succeeded, so the
// user genuinely exists.
func TestResolve_EmptyPlanCodeFallsBackToDefault(t *testing.T) {
	plans := map[string]config.RuntimePlan{
		"pro": {MaxHostedRuntimes: 5},
	}
	platform := config.RuntimePlatformConfig{DefaultPlanCode: "pro"}
	r := makeResolver(plans, "", platform, stubLookup{planCode: ""})
	got, err := r.Resolve(context.Background(), 42)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.PlanCode != "pro" {
		t.Errorf("PlanCode = %q, want pro (fallback)", got.PlanCode)
	}
}

func TestResolve_FallsBackOnUnknownPlanCode(t *testing.T) {
	plans := map[string]config.RuntimePlan{
		"pro": {MaxHostedRuntimes: 5},
	}
	platform := config.RuntimePlatformConfig{DefaultPlanCode: "pro"}
	r := makeResolver(plans, "", platform, stubLookup{planCode: "enterprise"})
	got, err := r.Resolve(context.Background(), 42)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.PlanCode != "pro" {
		t.Errorf("PlanCode = %q, want pro (fallback to default)", got.PlanCode)
	}
}

func TestResolve_MissingDefaultPlanIsError(t *testing.T) {
	plans := map[string]config.RuntimePlan{
		"free": {MaxHostedRuntimes: 1},
	}
	platform := config.RuntimePlatformConfig{DefaultPlanCode: "missing"}
	// Lookup returns "missing" plan_code; resolver tries to look up,
	// finds nothing, falls back to platform default ("missing"), still
	// nothing → config error.
	r := makeResolver(plans, "", platform, stubLookup{planCode: "missing"})
	if _, err := r.Resolve(context.Background(), 42); err == nil {
		t.Fatalf("expected error when default plan is missing from config")
	}
}

func TestResolve_RejectsZeroUserID(t *testing.T) {
	r := makeResolver(map[string]config.RuntimePlan{"pro": {}}, "", config.RuntimePlatformConfig{DefaultPlanCode: "pro"}, stubLookup{planCode: "pro"})
	_, err := r.Resolve(context.Background(), 0)
	if err == nil {
		t.Fatalf("expected ErrUserNotFound for user_id=0")
	}
	if !errors.Is(err, ErrUserNotFound) {
		t.Errorf("err = %v, want ErrUserNotFound", err)
	}
}

func TestResolve_NilLookupFailsClosed(t *testing.T) {
	r := NewResolver(nil, map[string]config.RuntimePlan{"pro": {}}, config.RuntimePlatformConfig{DefaultPlanCode: "pro"})
	_, err := r.Resolve(context.Background(), 42)
	if err == nil {
		t.Fatalf("expected error when lookup is nil")
	}
	if !errors.Is(err, ErrPlanLookupUnavailable) {
		t.Errorf("err = %v, want ErrPlanLookupUnavailable", err)
	}
}

func TestResolve_AppliesPlatformCap(t *testing.T) {
	plans := map[string]config.RuntimePlan{
		"pro": {MaxHostedRuntimes: 50},
	}
	platform := config.RuntimePlatformConfig{
		DefaultPlanCode:        "pro",
		MaxTotalHostedRuntimes: 10, // platform-wide cap below plan
	}
	r := makeResolver(plans, "", platform, stubLookup{planCode: "pro"})
	got, err := r.Resolve(context.Background(), 42)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.MaxHostedRuntimes != 10 {
		t.Errorf("MaxHostedRuntimes = %d, want 10 (platform cap)", got.MaxHostedRuntimes)
	}
}

// New "0 = hard cap" semantics: free plan with max_self_hosted_runtimes: 0
// should produce 0, NOT the platform's 100. This was the #3 bug.
func TestApplyPlatformCap_ZeroIsHardCap(t *testing.T) {
	cases := []struct {
		name           string
		plan, platform int
		want           int
	}{
		{"plan zero forbids", 0, 100, 0},
		{"platform zero forbids", 100, 0, 0},
		{"both zero", 0, 0, 0},
		{"plan unlimited", -1, 100, 100},
		{"platform unlimited", 10, -1, 10},
		{"both unlimited", -1, -1, -1},
		{"both positive smaller wins (plan)", 5, 100, 5},
		{"both positive smaller wins (platform)", 100, 5, 5},
		{"equal", 5, 5, 5},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := applyPlatformCap(c.plan, c.platform)
			if got != c.want {
				t.Errorf("applyPlatformCap(%d, %d) = %d, want %d", c.plan, c.platform, got, c.want)
			}
		})
	}
}
