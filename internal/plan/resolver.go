// Package plan resolves the effective per-user runtime plan limits used by
// route resolution and provisioning. Effective limits are
// min(user_plan_limit, platform_limit) per the design doc.
package plan

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hushine-tech/control-panel-service/internal/config"
)

// Sentinel errors. Callers map these to gRPC codes through service-layer
// sentinels so the fail-closed contract is honored.
var (
	// ErrUserNotFound: core-service returned NotFound for the supplied
	// user_id. The control plane MUST refuse the request rather than
	// silently fall back to a default plan.
	ErrUserNotFound = errors.New("plan: user not found in core-service")

	// ErrPlanLookupUnavailable: the call to core-service failed for any
	// non-NotFound reason (Unavailable, DeadlineExceeded, network, etc.).
	// Callers MUST treat this as a hard failure; allowing requests through
	// during core-service downtime would break the plan / quota contract.
	ErrPlanLookupUnavailable = errors.New("plan: core-service lookup unavailable")
)

// PlanLookup retrieves a user's plan_code. The interface lets unit tests
// substitute a deterministic fixture for the real core-service client.
//
// Implementations should return a gRPC status error so the resolver can
// distinguish NotFound (user truly absent) from Unavailable / transient
// failures. Wrapped errors (e.g., status.FromError fallthrough) are still
// treated as Unavailable.
type PlanLookup interface {
	GetUserPlanCode(ctx context.Context, userID int64) (string, error)
}

// EffectiveLimits is the per-user, post-platform-cap view used by the
// route resolver, RuntimeChannel admission, and hosted-runtime provisioning.
type EffectiveLimits struct {
	PlanCode                        string
	MaxHostedRuntimes               int
	MaxSelfHostedRuntimes           int
	MaxRoutingEnabledRuntimes       int
	MaxConcurrentSessionsTotal      int
	MaxConcurrentSessionsPerRuntime int
	AllowedResourceProfiles         []string
	AllowSelfHostedRuntime          bool
	AllowIDEDebug                   bool
}

// Resolver merges three inputs into an EffectiveLimits:
//
//  1. The user's plan_code (looked up via PlanLookup; lookup errors fail
//     closed via ErrUserNotFound / ErrPlanLookupUnavailable).
//  2. The runtime_plans config tier matching that plan_code.
//  3. The platform-wide caps in runtime_platform.
//
// The configured default plan is used ONLY when core-service returns
// the user successfully but the user's plan_code field is empty (legacy
// row written before migration 0011 backfill). It is NOT a fallback for
// missing users or transient failures.
type Resolver struct {
	lookup   PlanLookup
	plans    map[string]config.RuntimePlan
	platform config.RuntimePlatformConfig
}

func NewResolver(lookup PlanLookup, plans map[string]config.RuntimePlan, platform config.RuntimePlatformConfig) *Resolver {
	return &Resolver{lookup: lookup, plans: plans, platform: platform}
}

// Resolve returns the effective limits for the given user_id.
//
// Failure semantics (Phase D1 fail-closed contract):
//
//   - userID <= 0                          → ErrUserNotFound
//   - lookup returns gRPC NotFound         → ErrUserNotFound
//   - lookup returns any other gRPC error  → ErrPlanLookupUnavailable
//   - lookup returns OK with empty code    → use platform default plan
//   - lookup returns OK with unknown code  → use platform default plan
//   - default plan missing from config     → wrapped fmt error (config bug)
//
// The "OK + empty code" branch exists only for the migration window:
// migration 0011 backfills `plan_code='pro'` so production rows always
// have a non-empty plan_code. This branch SHOULD be tightened to also
// fail closed once that backfill is verified.
func (r *Resolver) Resolve(ctx context.Context, userID int64) (EffectiveLimits, error) {
	if userID <= 0 {
		return EffectiveLimits{}, fmt.Errorf("%w: user_id must be > 0 (got %d)", ErrUserNotFound, userID)
	}
	if r.lookup == nil {
		return EffectiveLimits{}, fmt.Errorf("%w: PlanLookup is nil", ErrPlanLookupUnavailable)
	}

	code, err := r.lookup.GetUserPlanCode(ctx, userID)
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
			return EffectiveLimits{}, fmt.Errorf("%w: user_id=%d", ErrUserNotFound, userID)
		}
		// Unavailable / DeadlineExceeded / Internal / network → fail closed.
		return EffectiveLimits{}, fmt.Errorf("%w: %v", ErrPlanLookupUnavailable, err)
	}

	planCode := code
	if planCode == "" {
		// Migration window only: row missing plan_code → use platform default.
		planCode = r.platform.DefaultPlanCode
	}

	plan, ok := r.plans[planCode]
	if !ok {
		// Unknown plan_code → fall back to platform default (config drift, not a security boundary).
		plan, ok = r.plans[r.platform.DefaultPlanCode]
		if !ok {
			return EffectiveLimits{}, fmt.Errorf("default plan %q not found in runtime_plans config", r.platform.DefaultPlanCode)
		}
		planCode = r.platform.DefaultPlanCode
	}

	return EffectiveLimits{
		PlanCode:                        planCode,
		MaxHostedRuntimes:               applyPlatformCap(plan.MaxHostedRuntimes, r.platform.MaxTotalHostedRuntimes),
		MaxSelfHostedRuntimes:           applyPlatformCap(plan.MaxSelfHostedRuntimes, r.platform.MaxTotalSelfHostedRuntimes),
		MaxRoutingEnabledRuntimes:       plan.MaxRoutingEnabledRuntimes,
		MaxConcurrentSessionsTotal:      plan.MaxConcurrentSessionsTotal,
		MaxConcurrentSessionsPerRuntime: plan.MaxConcurrentSessionsPerRuntime,
		AllowedResourceProfiles:         plan.AllowedResourceProfiles,
		AllowSelfHostedRuntime:          plan.AllowSelfHostedRuntime,
		AllowIDEDebug:                   plan.AllowIDEDebug,
	}, nil
}

// applyPlatformCap combines a plan-level limit with a platform-wide cap.
//
// Convention (replaces the old "0 = unlimited" minNonZero behavior, which
// silently turned `max_self_hosted_runtimes: 0` into "unlimited"):
//
//   - -1  → unlimited on this side
//   - 0  → hard cap of 0 (forbidden); takes precedence
//   - >0  → the smaller of the two non-unlimited sides; if the other side
//     is unlimited, this side wins
//
// Examples:
//
//	plan=0,  platform=100 → 0   (free plan forbids; platform cap doesn't relax)
//	plan=-1, platform=100 → 100
//	plan=10, platform=-1  → 10
//	plan=10, platform=100 → 10
func applyPlatformCap(planLimit, platformLimit int) int {
	if planLimit == 0 || platformLimit == 0 {
		return 0
	}
	if planLimit == -1 {
		return platformLimit
	}
	if platformLimit == -1 {
		return planLimit
	}
	if planLimit < platformLimit {
		return planLimit
	}
	return platformLimit
}
