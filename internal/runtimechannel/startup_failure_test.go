package runtimechannel

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	"github.com/hushine-tech/control-panel-service/internal/domain"
	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
)

func signedStartupFailureRequest(t *testing.T, privateKey ed25519.PrivateKey, at time.Time, nonce string) *cpv1.ReportRuntimeStartupFailureRequest {
	t.Helper()
	profile := completeDependencyProfile("normal-build")
	request := &cpv1.ReportRuntimeStartupFailureRequest{
		KeyId:          "key-1",
		RuntimeId:      "runtime-1",
		Source:         domain.RuntimeSourceSelfHosted,
		IssuedAtUnixMs: at.UnixMilli(),
		Nonce:          nonce,
		DependencyError: &strategyv1.RuntimeDependencyError{
			Code:                  runtimeDependencyProfileInvalidCode,
			Module:                "numpy",
			RuntimeProfile:        profile.GetProfileName(),
			RuntimeProfileVersion: profile.GetProfileVersion(),
			ImageBuildId:          profile.GetImageBuildId(),
			Message:               "runtime dependency startup probe failed",
		},
		ActualProfile: profile,
	}
	signStartupFailureRequest(t, privateKey, request)
	return request
}

func signStartupFailureRequest(t *testing.T, privateKey ed25519.PrivateKey, request *cpv1.ReportRuntimeStartupFailureRequest) {
	t.Helper()
	request.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, canonicalRuntimeStartupFailurePayload(request)))
}

func TestStartupFailureCanonicalPayloadIsDomainSeparated(t *testing.T) {
	_, privateKey, now := newAuthFixture(t, domain.CredentialStatusActive)
	request := signedStartupFailureRequest(t, privateKey, now, "nonce-123")
	payload := canonicalRuntimeStartupFailurePayload(request)
	if !bytes.HasPrefix(payload, []byte("runtime-startup-failure-v1\n{")) {
		t.Fatalf("canonical payload is not domain separated: %q", payload)
	}
}

func TestReportRuntimeStartupFailureRecordsOnlyFailureState(t *testing.T) {
	repo, privateKey, now := newAuthFixture(t, domain.CredentialStatusDownloaded)
	svc := NewWithConfig(repo, Config{ExpectedDependencyProfile: expectedDependencyProfile()})
	svc.SetClock(func() time.Time { return now })

	response, err := svc.ReportRuntimeStartupFailure(context.Background(), signedStartupFailureRequest(t, privateKey, now, "nonce-123"))
	if err != nil {
		t.Fatalf("ReportRuntimeStartupFailure: %v", err)
	}
	if response == nil || !response.GetRecorded() {
		t.Fatalf("response = %+v, want recorded", response)
	}
	if repo.createdRuntime != nil || repo.createdLease != nil || repo.rotatedLeaseHash != "" || repo.touchedAt != nil || len(svc.RegistrySnapshot()) != 0 {
		t.Fatalf("startup failure mutated readiness/token state: runtime=%+v lease=%+v rotated=%q touched=%v registry=%+v", repo.createdRuntime, repo.createdLease, repo.rotatedLeaseHash, repo.touchedAt, svc.RegistrySnapshot())
	}
	if len(repo.admissions) != 1 {
		t.Fatalf("admission failures = %+v, want one", repo.admissions)
	}
	failure := repo.admissions[0]
	if failure.UserID != 42 || failure.CredentialKeyID != "key-1" || failure.RequestedRuntimeID != "runtime-1" ||
		failure.Source != domain.RuntimeSourceSelfHosted || failure.Role != domain.CredentialRoleExecutor ||
		failure.FailureCode != runtimeDependencyProfileInvalidCode {
		t.Fatalf("admission failure = %+v", failure)
	}
	if failure.Reason != "numpy: runtime dependency startup probe failed; profile=platform-python-3.13 version=1.0.0 image_build_id=normal-build" {
		t.Fatalf("reason = %q, want allowlisted safe reason", failure.Reason)
	}
}

func TestReportRuntimeStartupFailureRejectsReplay(t *testing.T) {
	repo, privateKey, now := newAuthFixture(t, domain.CredentialStatusActive)
	svc := NewWithConfig(repo, Config{ExpectedDependencyProfile: expectedDependencyProfile()})
	svc.SetClock(func() time.Time { return now })
	request := signedStartupFailureRequest(t, privateKey, now, "nonce-replay")

	if _, err := svc.ReportRuntimeStartupFailure(context.Background(), request); err != nil {
		t.Fatalf("first report: %v", err)
	}
	if _, err := svc.ReportRuntimeStartupFailure(context.Background(), request); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("replay error = %v, want PermissionDenied", err)
	}
	if len(repo.admissions) != 1 {
		t.Fatalf("replay recorded extra failure: %+v", repo.admissions)
	}
}

func TestReportRuntimeStartupFailureRejectsUntrustedRequestsWithoutRecording(t *testing.T) {
	tests := []struct {
		name       string
		status     domain.CredentialStatus
		prepare    func(*stubRepo, *cpv1.ReportRuntimeStartupFailureRequest)
		resign     bool
		wantStatus codes.Code
	}{
		{name: "expired timestamp", status: domain.CredentialStatusActive, prepare: func(_ *stubRepo, request *cpv1.ReportRuntimeStartupFailureRequest) {
			request.IssuedAtUnixMs -= int64((10 * time.Minute) / time.Millisecond)
		}, resign: true, wantStatus: codes.PermissionDenied},
		{name: "wrong source", status: domain.CredentialStatusActive, prepare: func(_ *stubRepo, request *cpv1.ReportRuntimeStartupFailureRequest) {
			request.Source = domain.RuntimeSourceHosted
		}, resign: true, wantStatus: codes.PermissionDenied},
		{name: "wrong owner", status: domain.CredentialStatusActive, prepare: func(repo *stubRepo, _ *cpv1.ReportRuntimeStartupFailureRequest) {
			repo.getRuntime = map[string]domain.Runtime{"runtime-1": {RuntimeID: "runtime-1", UserID: 99, CredentialKeyID: "other-key", Status: domain.RuntimeStatusActive}}
		}, resign: true, wantStatus: codes.PermissionDenied},
		{name: "altered signed field", status: domain.CredentialStatusActive, prepare: func(_ *stubRepo, request *cpv1.ReportRuntimeStartupFailureRequest) {
			request.DependencyError.Module = "pandas"
		}, wantStatus: codes.PermissionDenied},
		{name: "unknown code", status: domain.CredentialStatusActive, prepare: func(_ *stubRepo, request *cpv1.ReportRuntimeStartupFailureRequest) {
			request.DependencyError.Code = "ATTACKER_CODE"
		}, resign: true, wantStatus: codes.InvalidArgument},
		{name: "unsafe message", status: domain.CredentialStatusActive, prepare: func(_ *stubRepo, request *cpv1.ReportRuntimeStartupFailureRequest) {
			request.DependencyError.Message = "secret=token"
		}, resign: true, wantStatus: codes.InvalidArgument},
		{name: "oversized module", status: domain.CredentialStatusActive, prepare: func(_ *stubRepo, request *cpv1.ReportRuntimeStartupFailureRequest) {
			request.DependencyError.Module = strings.Repeat("a", 129)
		}, resign: true, wantStatus: codes.InvalidArgument},
		{name: "revoked credential", status: domain.CredentialStatusRevoked, prepare: func(*stubRepo, *cpv1.ReportRuntimeStartupFailureRequest) {}, resign: true, wantStatus: codes.PermissionDenied},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo, privateKey, now := newAuthFixture(t, tt.status)
			svc := NewWithConfig(repo, Config{ExpectedDependencyProfile: expectedDependencyProfile()})
			svc.SetClock(func() time.Time { return now })
			request := signedStartupFailureRequest(t, privateKey, now, "nonce-"+strings.ReplaceAll(tt.name, " ", "-"))
			tt.prepare(repo, request)
			if tt.resign {
				signStartupFailureRequest(t, privateKey, request)
			}
			if _, err := svc.ReportRuntimeStartupFailure(context.Background(), request); status.Code(err) != tt.wantStatus {
				t.Fatalf("error = %v, want %s", err, tt.wantStatus)
			}
			if len(repo.admissions) != 0 {
				t.Fatalf("untrusted request was recorded: %+v", repo.admissions)
			}
		})
	}
}

func TestReportRuntimeStartupFailureAllowsConsumedCredentialOnlyForBoundRuntime(t *testing.T) {
	repo, privateKey, now := newAuthFixture(t, domain.CredentialStatusConsumed)
	repo.cred.ConsumedRuntimeID = "runtime-1"
	svc := NewWithConfig(repo, Config{ExpectedDependencyProfile: expectedDependencyProfile()})
	svc.SetClock(func() time.Time { return now })
	if _, err := svc.ReportRuntimeStartupFailure(context.Background(), signedStartupFailureRequest(t, privateKey, now, "nonce-consumed")); err != nil {
		t.Fatalf("bound consumed credential report: %v", err)
	}

	repo2, privateKey2, _ := newAuthFixture(t, domain.CredentialStatusConsumed)
	repo2.cred.ConsumedRuntimeID = "runtime-other"
	svc2 := NewWithConfig(repo2, Config{ExpectedDependencyProfile: expectedDependencyProfile()})
	svc2.SetClock(func() time.Time { return now })
	if _, err := svc2.ReportRuntimeStartupFailure(context.Background(), signedStartupFailureRequest(t, privateKey2, now, "nonce-wrong-bound")); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("wrong bound credential error = %v, want PermissionDenied", err)
	}
	if len(repo2.admissions) != 0 {
		t.Fatalf("wrong bound credential was recorded: %+v", repo2.admissions)
	}
}

func TestReportRuntimeStartupFailureRecordsStructurallyValidMismatchedProfile(t *testing.T) {
	repo, privateKey, now := newAuthFixture(t, domain.CredentialStatusActive)
	svc := NewWithConfig(repo, Config{ExpectedDependencyProfile: expectedDependencyProfile()})
	svc.SetClock(func() time.Time { return now })
	request := signedStartupFailureRequest(t, privateKey, now, "nonce-old-profile")
	request.ActualProfile.ProfileName = "platform-python-3.12"
	request.ActualProfile.ProfileVersion = "0.9.0"
	request.ActualProfile.ContractSha256 = strings.Repeat("b", 64)
	request.DependencyError.RuntimeProfile = request.ActualProfile.ProfileName
	request.DependencyError.RuntimeProfileVersion = request.ActualProfile.ProfileVersion
	signStartupFailureRequest(t, privateKey, request)

	response, err := svc.ReportRuntimeStartupFailure(context.Background(), request)
	if err != nil {
		t.Fatalf("ReportRuntimeStartupFailure: %v", err)
	}
	if !response.GetRecorded() || len(repo.admissions) != 1 {
		t.Fatalf("response=%+v admissions=%+v", response, repo.admissions)
	}
	if reason := repo.admissions[0].Reason; !strings.Contains(reason, "platform-python-3.12") || !strings.Contains(reason, "normal-build") {
		t.Fatalf("recorded reason = %q, want safe actual profile facts", reason)
	}
}

func TestStartupFailureCanonicalPayloadCoversEveryField(t *testing.T) {
	_, privateKey, now := newAuthFixture(t, domain.CredentialStatusActive)
	original := signedStartupFailureRequest(t, privateKey, now, "nonce-fields")
	signature, err := base64.RawURLEncoding.DecodeString(original.GetSignature())
	if err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*cpv1.ReportRuntimeStartupFailureRequest){
		"runtime": func(request *cpv1.ReportRuntimeStartupFailureRequest) { request.RuntimeId = "runtime-2" },
		"profile": func(request *cpv1.ReportRuntimeStartupFailureRequest) { request.ActualProfile.ImageBuildId = "build-2" },
		"error":   func(request *cpv1.ReportRuntimeStartupFailureRequest) { request.DependencyError.Module = "pandas" },
		"nonce":   func(request *cpv1.ReportRuntimeStartupFailureRequest) { request.Nonce = "nonce-other" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(original).(*cpv1.ReportRuntimeStartupFailureRequest)
			mutate(changed)
			if ed25519.Verify(privateKey.Public().(ed25519.PublicKey), canonicalRuntimeStartupFailurePayload(changed), signature) {
				t.Fatal("signature remained valid after mutation")
			}
		})
	}
}
