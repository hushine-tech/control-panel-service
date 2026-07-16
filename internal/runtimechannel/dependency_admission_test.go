package runtimechannel

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	"github.com/hushine-tech/control-panel-service/internal/domain"
	"github.com/hushine-tech/control-panel-service/internal/runtimecert"
	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
)

const testDependencyContractSHA256 = "8457b3c35618558fc8bfc74d4135b7eb52e00c33a8c9a49d202830f3fd5b62c5"

func expectedDependencyProfile() ExpectedDependencyProfile {
	return ExpectedDependencyProfile{
		SchemaVersion:  1,
		Name:           "platform-python-3.13",
		Version:        "1.0.0",
		ContractSHA256: testDependencyContractSHA256,
	}
}

func completeDependencyProfile(buildID string) *strategyv1.RuntimeDependencyProfile {
	return &strategyv1.RuntimeDependencyProfile{
		SchemaVersion:         1,
		ProfileName:           "platform-python-3.13",
		ProfileVersion:        "1.0.0",
		ContractSha256:        testDependencyContractSHA256,
		HostedPython:          "/opt/hushine/runtime-python/bin/python",
		PublicImportRoots:     []string{"hushine", "numpy", "pandas"},
		StrategyServiceCommit: "strategy-service-commit",
		StrategyLibraryCommit: "strategy-library-commit",
		ImageBuildId:          buildID,
	}
}

func signHelloWithDependencyProfile(t *testing.T, privateKey ed25519.PrivateKey, at time.Time, profile *strategyv1.RuntimeDependencyProfile) *cpv1.RuntimeHello {
	t.Helper()
	hello := signedHello(t, privateKey, at)
	hello.DependencyProfile = profile
	payload, err := CanonicalHelloPayload(hello)
	if err != nil {
		t.Fatal(err)
	}
	hello.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	return hello
}

func TestCanonicalHelloPayloadMatchesRuntimeAgentDependencyFieldOrder(t *testing.T) {
	hello := &cpv1.RuntimeHello{
		KeyId:           "key-1",
		RuntimeId:       "runtime-1",
		Name:            "desk",
		EndpointHost:    "127.0.0.1",
		GrpcPort:        50052,
		DebugPort:       5678,
		Capabilities:    []string{"strategy", "spot"},
		ResourceProfile: "small",
		Version:         "0.1.0",
		IssuedAtUnixMs:  1234,
		Nonce:           "nonce",
		Source:          domain.RuntimeSourceSelfHosted,
		UserId:          42,
		DependencyProfile: &strategyv1.RuntimeDependencyProfile{
			SchemaVersion:         1,
			ProfileName:           "platform-python-3.13",
			ProfileVersion:        "1.0.0",
			ContractSha256:        testDependencyContractSHA256,
			HostedPython:          "/opt/python",
			PublicImportRoots:     []string{"pandas", "numpy"},
			StrategyServiceCommit: "service-commit",
			StrategyLibraryCommit: "library-commit",
			ImageBuildId:          "build-1",
		},
	}

	got, err := CanonicalHelloPayload(hello)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"capabilities":["strategy","spot"],"dependency_contract_sha256":"` + testDependencyContractSHA256 + `","dependency_hosted_python":"/opt/python","dependency_image_build_id":"build-1","dependency_profile_name":"platform-python-3.13","dependency_profile_version":"1.0.0","dependency_public_import_roots":["numpy","pandas"],"dependency_schema_version":1,"dependency_strategy_library_commit":"library-commit","dependency_strategy_service_commit":"service-commit","debug_port":5678,"endpoint_host":"127.0.0.1","grpc_port":50052,"issued_at_unix_ms":1234,"key_id":"key-1","nonce":"nonce","resource_profile":"small","runtime_id":"runtime-1","name":"desk","source":"self_hosted","user_id":42,"version":"0.1.0"}`
	if string(got) != want {
		t.Fatalf("canonical HELLO = %s\nwant = %s", got, want)
	}
}

func TestDependencyAdmissionRejectsIncompleteAndMismatchedProfiles(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*strategyv1.RuntimeDependencyProfile) *strategyv1.RuntimeDependencyProfile
	}{
		{name: "missing", mutate: func(*strategyv1.RuntimeDependencyProfile) *strategyv1.RuntimeDependencyProfile { return nil }},
		{name: "schema", mutate: func(p *strategyv1.RuntimeDependencyProfile) *strategyv1.RuntimeDependencyProfile {
			p.SchemaVersion = 2
			return p
		}},
		{name: "name", mutate: func(p *strategyv1.RuntimeDependencyProfile) *strategyv1.RuntimeDependencyProfile {
			p.ProfileName = "other"
			return p
		}},
		{name: "version", mutate: func(p *strategyv1.RuntimeDependencyProfile) *strategyv1.RuntimeDependencyProfile {
			p.ProfileVersion = "2.0.0"
			return p
		}},
		{name: "digest", mutate: func(p *strategyv1.RuntimeDependencyProfile) *strategyv1.RuntimeDependencyProfile {
			p.ContractSha256 = strings.Repeat("b", 64)
			return p
		}},
		{name: "hosted python", mutate: func(p *strategyv1.RuntimeDependencyProfile) *strategyv1.RuntimeDependencyProfile {
			p.HostedPython = ""
			return p
		}},
		{name: "empty roots", mutate: func(p *strategyv1.RuntimeDependencyProfile) *strategyv1.RuntimeDependencyProfile {
			p.PublicImportRoots = nil
			return p
		}},
		{name: "unsorted roots", mutate: func(p *strategyv1.RuntimeDependencyProfile) *strategyv1.RuntimeDependencyProfile {
			p.PublicImportRoots = []string{"pandas", "numpy"}
			return p
		}},
		{name: "duplicate roots", mutate: func(p *strategyv1.RuntimeDependencyProfile) *strategyv1.RuntimeDependencyProfile {
			p.PublicImportRoots = []string{"numpy", "numpy"}
			return p
		}},
		{name: "blank root", mutate: func(p *strategyv1.RuntimeDependencyProfile) *strategyv1.RuntimeDependencyProfile {
			p.PublicImportRoots = []string{"numpy", " "}
			return p
		}},
		{name: "service commit", mutate: func(p *strategyv1.RuntimeDependencyProfile) *strategyv1.RuntimeDependencyProfile {
			p.StrategyServiceCommit = ""
			return p
		}},
		{name: "library commit", mutate: func(p *strategyv1.RuntimeDependencyProfile) *strategyv1.RuntimeDependencyProfile {
			p.StrategyLibraryCommit = ""
			return p
		}},
		{name: "build id", mutate: func(p *strategyv1.RuntimeDependencyProfile) *strategyv1.RuntimeDependencyProfile {
			p.ImageBuildId = ""
			return p
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := tt.mutate(completeDependencyProfile("normal-build"))
			if err := validateDependencyAdmission(expectedDependencyProfile(), actual); !errorsIsDependencyMismatch(err) {
				t.Fatalf("validateDependencyAdmission() error = %v, want ErrDependencyProfileMismatch", err)
			}
		})
	}
}

func errorsIsDependencyMismatch(err error) bool {
	return err != nil && strings.Contains(err.Error(), ErrDependencyProfileMismatch.Error())
}

func TestDependencyAdmissionAcceptsNormalAndCoverageBuilds(t *testing.T) {
	for _, buildID := range []string{"normal-build", "coverage-build"} {
		t.Run(buildID, func(t *testing.T) {
			if err := validateDependencyAdmission(expectedDependencyProfile(), completeDependencyProfile(buildID)); err != nil {
				t.Fatalf("validateDependencyAdmission: %v", err)
			}
		})
	}
}

func TestMismatchedHelloIsRecordedBeforeUpsertLeaseOrRegister(t *testing.T) {
	repo, privateKey, now := newAuthFixture(t, domain.CredentialStatusActive)
	svc := NewWithConfig(repo, Config{ExpectedDependencyProfile: expectedDependencyProfile()})
	svc.SetClock(func() time.Time { return now })
	profile := completeDependencyProfile("normal-build")
	profile.ContractSha256 = strings.Repeat("b", 64)

	stream := newFakeRuntimeChannelStream()
	done := make(chan error, 1)
	go func() { done <- svc.Handle(stream) }()
	stream.recv <- &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO,
		Payload: &cpv1.RuntimeFrame_Hello{
			Hello: signHelloWithDependencyProfile(t, privateKey, now, profile),
		},
	}

	if err := <-done; status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Handle() error = %v, want FailedPrecondition", err)
	}
	if repo.createdRuntime != nil || repo.createdLease != nil || len(svc.RegistrySnapshot()) != 0 {
		t.Fatalf("mismatch mutated admission state: runtime=%+v lease=%+v registry=%+v", repo.createdRuntime, repo.createdLease, svc.RegistrySnapshot())
	}
	if len(repo.admissions) != 1 || repo.admissions[0].FailureCode != "RUNTIME_DEPENDENCY_PROFILE_MISMATCH" {
		t.Fatalf("admission failures = %+v", repo.admissions)
	}
}

func TestRuntimeChannelWithoutExplicitExpectedProfileStillFailsClosed(t *testing.T) {
	repo, privateKey, now := newAuthFixture(t, domain.CredentialStatusActive)
	svc := New(repo)
	svc.SetClock(func() time.Time { return now })
	profile := completeDependencyProfile("normal-build")
	profile.ContractSha256 = strings.Repeat("b", 64)
	frame := &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO,
		Payload: &cpv1.RuntimeFrame_Hello{
			Hello: signHelloWithDependencyProfile(t, privateKey, now, profile),
		},
	}

	_, _, _, err := svc.authenticateFirstFrame(context.Background(), frame, runtimecert.RuntimeIdentity{}, false)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("authenticateFirstFrame() error = %v, want FailedPrecondition", err)
	}
	if repo.createdRuntime != nil || repo.createdLease != nil {
		t.Fatalf("unconfigured expected profile failed open: runtime=%+v lease=%+v", repo.createdRuntime, repo.createdLease)
	}
}

func TestMismatchedResumeDoesNotRotateOrRegister(t *testing.T) {
	repo, _, now := newAuthFixture(t, domain.CredentialStatusConsumed)
	token := "resume-token-1"
	repo.lease = domain.RuntimeChannelLease{
		RuntimeID:       "runtime-1",
		UserID:          42,
		CredentialKeyID: "key-1",
		LeaseHash:       hashRuntimeChannelToken(token),
		IssuedAt:        now.Add(-time.Minute),
		ExpiresAt:       now.Add(time.Hour),
	}
	repo.getRuntime = map[string]domain.Runtime{
		"runtime-1": {
			RuntimeID:       "runtime-1",
			UserID:          42,
			Name:            "default",
			Source:          domain.RuntimeSourceSelfHosted,
			Role:            domain.CredentialRoleExecutor,
			Status:          domain.RuntimeStatusActive,
			CredentialKeyID: "key-1",
		},
	}
	svc := NewWithConfig(repo, Config{ExpectedDependencyProfile: expectedDependencyProfile()})
	svc.SetClock(func() time.Time { return now })
	profile := completeDependencyProfile("normal-build")
	profile.ProfileVersion = "2.0.0"

	stream := newFakeRuntimeChannelStream()
	done := make(chan error, 1)
	go func() { done <- svc.Handle(stream) }()
	stream.recv <- &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_RESUME,
		Payload: &cpv1.RuntimeFrame_Resume{Resume: &cpv1.RuntimeResume{
			RuntimeId:         "runtime-1",
			ResumeToken:       token,
			DependencyProfile: profile,
		}},
	}

	if err := <-done; status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Handle() error = %v, want FailedPrecondition", err)
	}
	if repo.rotatedLeaseHash != "" || len(svc.RegistrySnapshot()) != 0 {
		t.Fatalf("mismatch rotated or registered: hash=%q registry=%+v", repo.rotatedLeaseHash, svc.RegistrySnapshot())
	}
	if len(repo.admissions) != 1 || repo.admissions[0].FailureCode != "RUNTIME_DEPENDENCY_PROFILE_MISMATCH" {
		t.Fatalf("admission failures = %+v", repo.admissions)
	}
}

func TestAuthenticatedRuntimeDependencyProfileIsDeepCopied(t *testing.T) {
	profile := completeDependencyProfile("normal-build")
	runtime := AuthenticatedRuntime{RuntimeID: "runtime-1", DependencyProfile: cloneDependencyProfile(profile)}
	registry := NewRegistry()
	if _, err := registry.Register(runtime, time.Now()); err != nil {
		t.Fatal(err)
	}
	profile.PublicImportRoots[0] = "mutated-input"
	first := registry.Snapshot()
	first[0].DependencyProfile.PublicImportRoots[0] = "mutated-snapshot"
	second := registry.Snapshot()
	if got := second[0].DependencyProfile.PublicImportRoots[0]; got != "hushine" {
		t.Fatalf("stored dependency profile root = %q, want deep copy", got)
	}
}
