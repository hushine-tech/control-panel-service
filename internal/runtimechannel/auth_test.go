package runtimechannel

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	"github.com/hushine-tech/control-panel-service/internal/domain"
	"github.com/hushine-tech/control-panel-service/internal/repository"
	accountv1 "github.com/hushine-tech/core-service/gen/accountv1"
	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
)

type stubRepo struct {
	cred               domain.RuntimeCredential
	touchedAt          *time.Time
	ended              []string
	endedByKeyID       map[string]int64
	cancelErr          error
	createErr          error
	createdRuntime     *domain.Runtime
	getRuntime         map[string]domain.Runtime
	lookupErr          error
	ownerInstance      string
	ownerCleared       bool
	claimCommand       domain.RuntimeCommand
	claimOK            bool
	claimOwner         string
	claimLimit         int
	ackedCommandID     string
	runningCommandID   string
	completedCommandID string
	completedStatus    string
	completedFailure   string
	lease              domain.RuntimeChannelLease
	createdLease       *domain.RuntimeChannelLease
	rotatedLeaseHash   string
	rotatedRuntimeID   string
	admissions         []domain.RuntimeAdmissionFailure
}

func (s *stubRepo) GetRuntimeCredential(_ context.Context, keyID string) (domain.RuntimeCredential, error) {
	if s.lookupErr != nil {
		return domain.RuntimeCredential{}, s.lookupErr
	}
	if keyID != s.cred.KeyID {
		return domain.RuntimeCredential{}, repository.ErrNotFound
	}
	return s.cred, nil
}

func (s *stubRepo) TouchRuntimeCredentialUsed(_ context.Context, keyID string, at time.Time) error {
	if keyID == s.cred.KeyID {
		cp := at
		s.touchedAt = &cp
	}
	return nil
}

func (s *stubRepo) CreateOrReplaceSelfHostedRuntime(_ context.Context, rt domain.Runtime) error {
	cp := rt
	s.createdRuntime = &cp
	return s.createErr
}

func (s *stubRepo) CreateOrReplaceHostedRuntime(_ context.Context, rt domain.Runtime) error {
	cp := rt
	s.createdRuntime = &cp
	return s.createErr
}

func (s *stubRepo) UpdateRuntimeHeartbeat(_ context.Context, _ string, _ time.Time) error {
	return nil
}

func (s *stubRepo) RecordRuntimeConnectionOwner(_ context.Context, runtimeID, instanceID string, _ time.Time) error {
	if runtimeID == "runtime-1" {
		s.ownerInstance = instanceID
	}
	return nil
}

func (s *stubRepo) ClearRuntimeConnectionOwner(_ context.Context, runtimeID, instanceID string) error {
	if runtimeID == "runtime-1" && s.ownerInstance == instanceID {
		s.ownerInstance = ""
		s.ownerCleared = true
	}
	return nil
}

func (s *stubRepo) GetRuntime(_ context.Context, runtimeID string) (domain.Runtime, error) {
	if s.getRuntime != nil {
		if rt, ok := s.getRuntime[runtimeID]; ok {
			return rt, nil
		}
	}
	if s.createdRuntime != nil && s.createdRuntime.RuntimeID == runtimeID {
		return *s.createdRuntime, nil
	}
	return domain.Runtime{}, repository.ErrNotFound
}

func (s *stubRepo) EndRuntimesByCredentialKey(_ context.Context, keyID, _ string, _ time.Time) (int64, error) {
	if s.cancelErr != nil {
		return 0, s.cancelErr
	}
	if s.endedByKeyID != nil {
		return s.endedByKeyID[keyID], nil
	}
	return int64(len(s.ended)), nil
}

func (s *stubRepo) ClaimNextRuntimeCommand(_ context.Context, runtimeID, ownerInstanceID string, _ time.Time, inFlightLimit int) (domain.RuntimeCommand, bool, error) {
	s.claimOwner = ownerInstanceID
	s.claimLimit = inFlightLimit
	if s.claimCommand.CommandID == "" || s.claimCommand.RuntimeID != runtimeID {
		return domain.RuntimeCommand{}, false, nil
	}
	if s.claimOK || s.claimCommand.Status == domain.RuntimeCommandStatusSent {
		return s.claimCommand, true, nil
	}
	return domain.RuntimeCommand{}, false, nil
}

func (s *stubRepo) AcknowledgeRuntimeCommand(_ context.Context, commandID string, _ time.Time) (domain.RuntimeCommand, error) {
	s.ackedCommandID = commandID
	return domain.RuntimeCommand{CommandID: commandID, Status: domain.RuntimeCommandStatusAcked}, nil
}

func (s *stubRepo) MarkRuntimeCommandRunning(_ context.Context, commandID string, _ time.Time) (domain.RuntimeCommand, error) {
	s.runningCommandID = commandID
	return domain.RuntimeCommand{CommandID: commandID, Status: domain.RuntimeCommandStatusRunning}, nil
}

func (s *stubRepo) CompleteRuntimeCommand(_ context.Context, commandID, status string, _ []byte, failureReason string, _ time.Time) (domain.RuntimeCommand, error) {
	s.completedCommandID = commandID
	s.completedStatus = status
	s.completedFailure = failureReason
	return domain.RuntimeCommand{CommandID: commandID, Status: status, FailureReason: failureReason}, nil
}

func (s *stubRepo) RuntimeCommandCircuitOpen(_ context.Context, _ string, _ time.Time, _ int64) (bool, int64, error) {
	return false, 0, nil
}

func (s *stubRepo) CreateRuntimeChannelLease(_ context.Context, lease domain.RuntimeChannelLease) error {
	cp := lease
	s.createdLease = &cp
	s.lease = lease
	return nil
}

func (s *stubRepo) GetRuntimeChannelLeaseByHash(_ context.Context, leaseHash string) (domain.RuntimeChannelLease, error) {
	if s.lease.LeaseHash == leaseHash {
		return s.lease, nil
	}
	return domain.RuntimeChannelLease{}, repository.ErrNotFound
}

func (s *stubRepo) TouchRuntimeChannelLease(_ context.Context, runtimeID, leaseHash string, at time.Time) error {
	if s.lease.RuntimeID == runtimeID && s.lease.LeaseHash == leaseHash {
		cp := at
		s.lease.LastUsedAt = &cp
		return nil
	}
	return repository.ErrNotFound
}

func (s *stubRepo) RotateRuntimeChannelLease(_ context.Context, runtimeID, oldLeaseHash, newLeaseHash string, expiresAt, at time.Time) error {
	if s.lease.RuntimeID == runtimeID && s.lease.LeaseHash == oldLeaseHash {
		cp := at
		s.lease.LeaseHash = newLeaseHash
		s.lease.LastUsedAt = &cp
		s.lease.ExpiresAt = expiresAt
		s.lease.UpdatedAt = at
		s.rotatedRuntimeID = runtimeID
		s.rotatedLeaseHash = newLeaseHash
		return nil
	}
	return repository.ErrNotFound
}

func (s *stubRepo) RecordRuntimeAdmissionFailure(_ context.Context, failure domain.RuntimeAdmissionFailure) error {
	s.admissions = append(s.admissions, failure)
	return nil
}

func (s *stubRepo) ListRuntimeAdmissionFailuresByUser(_ context.Context, userID int64, limit int) ([]domain.RuntimeAdmissionFailure, error) {
	var out []domain.RuntimeAdmissionFailure
	for _, failure := range s.admissions {
		if failure.UserID == userID {
			out = append(out, failure)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func TestVerifyHelloValid(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusActive)
	hello := signedHello(t, priv, now)
	got, err := verifyHello(context.Background(), repo, NewReplayCache(time.Minute, 16), func() time.Time { return now }, hello)
	if err != nil {
		t.Fatalf("verifyHello: %v", err)
	}
	if got.UserID != 42 || got.KeyID != "key-1" || got.RuntimeID != "runtime-1" {
		t.Fatalf("authenticated runtime = %+v", got)
	}
	if got.Name != "default" {
		t.Fatalf("name = %q, want default", got.Name)
	}
	if repo.touchedAt == nil || !repo.touchedAt.Equal(now) {
		t.Fatalf("last_used_at not touched: %v", repo.touchedAt)
	}
}

func TestVerifyHelloAllowsDownloadedCredential(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusDownloaded)
	hello := signedHello(t, priv, now)
	got, err := verifyHello(context.Background(), repo, NewReplayCache(time.Minute, 16), func() time.Time { return now }, hello)
	if err != nil {
		t.Fatalf("verifyHello: %v", err)
	}
	if got.KeyID != "key-1" || got.UserID != 42 {
		t.Fatalf("authenticated runtime = %+v", got)
	}
}

func TestVerifyHelloDerivesSourceAndRoleFromCredential(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusDownloaded)
	repo.cred.Role = domain.CredentialRoleDebugger
	hello := signedHello(t, priv, now)

	got, err := verifyHello(context.Background(), repo, NewReplayCache(time.Minute, 16), func() time.Time { return now }, hello)
	if err != nil {
		t.Fatalf("verifyHello: %v", err)
	}
	if got.Source != domain.RuntimeSourceSelfHosted || got.Role != domain.CredentialRoleDebugger {
		t.Fatalf("source/role = %q/%q, want self_hosted/debugger", got.Source, got.Role)
	}
}

func TestVerifyHelloDerivesHostedSourceFromHostedInternalCredential(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusDownloaded)
	repo.cred.Role = domain.CredentialRoleExecutor
	repo.cred.HostedInternal = true
	hello := signedHello(t, priv, now)

	got, err := verifyHello(context.Background(), repo, NewReplayCache(time.Minute, 16), func() time.Time { return now }, hello)
	if err != nil {
		t.Fatalf("verifyHello: %v", err)
	}
	if got.Source != domain.RuntimeSourceHosted || got.Role != domain.CredentialRoleExecutor {
		t.Fatalf("source/role = %q/%q, want hosted/executor", got.Source, got.Role)
	}
}

func TestVerifyHelloGeneratesCustomNameWhenOmitted(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusActive)
	hello := signedHello(t, priv, now)
	hello.Name = ""
	payload, err := CanonicalHelloPayload(hello)
	if err != nil {
		t.Fatal(err)
	}
	hello.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(priv, payload))

	got, err := verifyHello(context.Background(), repo, NewReplayCache(time.Minute, 16), func() time.Time { return now }, hello)
	if err != nil {
		t.Fatalf("verifyHello: %v", err)
	}
	if !strings.HasPrefix(got.Name, "custom-") {
		t.Fatalf("generated name = %q, want custom-*", got.Name)
	}
	if got.Name == "custom-" || !validRuntimeChannelName(got.Name) {
		t.Fatalf("generated name = %q is not a valid runtime channel name", got.Name)
	}
}

func TestVerifyHelloRejectsExpiredTimestamp(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusActive)
	hello := signedHello(t, priv, now.Add(-10*time.Minute))
	_, err := verifyHello(context.Background(), repo, NewReplayCache(time.Minute, 16), func() time.Time { return now }, hello)
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("err = %v, want ErrPermissionDenied", err)
	}
}

func TestVerifyHelloRejectsReplay(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusActive)
	cache := NewReplayCache(time.Minute, 16)
	hello := signedHello(t, priv, now)
	if _, err := verifyHello(context.Background(), repo, cache, func() time.Time { return now }, hello); err != nil {
		t.Fatalf("first verifyHello: %v", err)
	}
	if _, err := verifyHello(context.Background(), repo, cache, func() time.Time { return now }, hello); !errors.Is(err, ErrReplay) {
		t.Fatalf("second err = %v, want ErrReplay", err)
	}
}

func TestVerifyHelloRejectsRevokedKey(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusRevoked)
	hello := signedHello(t, priv, now)
	_, err := verifyHello(context.Background(), repo, NewReplayCache(time.Minute, 16), func() time.Time { return now }, hello)
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("err = %v, want ErrPermissionDenied", err)
	}
}

func TestVerifyHelloRejectsConsumedCredentialForDifferentRuntimeWithTerminalGuidance(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusConsumed)
	repo.cred.ConsumedRuntimeID = "runtime-old"
	hello := signedHello(t, priv, now)
	hello.RuntimeId = "runtime-new"
	payload, err := CanonicalHelloPayload(hello)
	if err != nil {
		t.Fatal(err)
	}
	hello.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(priv, payload))

	_, err = verifyHello(context.Background(), repo, NewReplayCache(time.Minute, 16), func() time.Time { return now }, hello)
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("err = %v, want ErrPermissionDenied", err)
	}
	if !strings.Contains(err.Error(), "consumed") || !strings.Contains(err.Error(), "stop retrying") {
		t.Fatalf("err = %v, want consumed terminal guidance", err)
	}
}

func TestVerifyHelloRejectsConsumedCredentialForSameRuntimeWithTerminalGuidance(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusConsumed)
	repo.cred.ConsumedRuntimeID = "runtime-1"
	consumedAt := now.Add(-time.Minute)
	repo.cred.ConsumedAt = &consumedAt
	hello := signedHello(t, priv, now)

	_, err := verifyHello(context.Background(), repo, NewReplayCache(time.Minute, 16), func() time.Time { return now }, hello)
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("err = %v, want ErrPermissionDenied", err)
	}
	if !strings.Contains(err.Error(), "consumed") || !strings.Contains(err.Error(), "stop retrying") {
		t.Fatalf("err = %v, want consumed terminal guidance", err)
	}
}

func TestVerifyHelloRejectsExpiredCredentialWithTerminalGuidance(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusExpired)
	hello := signedHello(t, priv, now)

	_, err := verifyHello(context.Background(), repo, NewReplayCache(time.Minute, 16), func() time.Time { return now }, hello)
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("err = %v, want ErrPermissionDenied", err)
	}
	if !strings.Contains(err.Error(), "expired") || !strings.Contains(err.Error(), "stop retrying") {
		t.Fatalf("err = %v, want expired terminal guidance", err)
	}
}

func TestVerifyHelloRejectsSignatureMismatch(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusActive)
	hello := signedHello(t, priv, now)
	hello.Name = "tampered"
	_, err := verifyHello(context.Background(), repo, NewReplayCache(time.Minute, 16), func() time.Time { return now }, hello)
	if !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("err = %v, want ErrPermissionDenied", err)
	}
}

func TestRuntimeChannelRegistersHostedInternalCredentialAsHostedRuntime(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusDownloaded)
	repo.cred.Role = domain.CredentialRoleExecutor
	repo.cred.HostedInternal = true
	svc := New(repo)
	svc.SetClock(func() time.Time { return now })

	stream := newFakeRuntimeChannelStream()
	done := make(chan error, 1)
	go func() { done <- svc.Handle(stream) }()
	stream.recv <- &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO,
		Payload:   &cpv1.RuntimeFrame_Hello{Hello: signedHello(t, priv, now)},
	}

	deadline := time.Now().Add(time.Second)
	for repo.createdRuntime == nil && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	stream.cancel()
	_ = <-done

	if repo.createdRuntime == nil {
		t.Fatal("runtime was not registered")
	}
	if repo.createdRuntime.Source != domain.RuntimeSourceHosted || repo.createdRuntime.Role != domain.CredentialRoleExecutor {
		t.Fatalf("registered source/role = %q/%q, want hosted/executor", repo.createdRuntime.Source, repo.createdRuntime.Role)
	}
}

func TestRuntimeChannelSendsHelloAckWithResumeToken(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusDownloaded)
	svc := New(repo)
	svc.SetClock(func() time.Time { return now })

	stream := newFakeRuntimeChannelStream()
	done := make(chan error, 1)
	go func() { done <- svc.Handle(stream) }()
	stream.recv <- &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO,
		Payload:   &cpv1.RuntimeFrame_Hello{Hello: signedHello(t, priv, now)},
	}

	var frame *cpv1.RuntimeFrame
	select {
	case frame = <-stream.sent:
	case <-time.After(time.Second):
		t.Fatal("hello ack not sent")
	}
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_HELLO_ACK {
		t.Fatalf("frame_type = %s, want HELLO_ACK", frame.GetFrameType())
	}
	if frame.GetHelloAck().GetRuntimeId() != "runtime-1" || frame.GetHelloAck().GetResumeToken() == "" {
		t.Fatalf("hello_ack = %+v, want runtime_id and resume_token", frame.GetHelloAck())
	}
	if repo.createdLease == nil || repo.createdLease.RuntimeID != "runtime-1" || repo.createdLease.LeaseHash == "" {
		t.Fatalf("created lease = %+v, want runtime-1 hashed lease", repo.createdLease)
	}
	stream.cancel()
	_ = <-done
}

func TestRuntimeChannelRecordsAdmissionFailureForConsumedCredential(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusConsumed)
	repo.cred.ConsumedRuntimeID = "runtime-old"
	svc := New(repo)
	svc.SetClock(func() time.Time { return now })

	stream := newFakeRuntimeChannelStream()
	done := make(chan error, 1)
	go func() { done <- svc.Handle(stream) }()
	stream.recv <- &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO,
		Payload:   &cpv1.RuntimeFrame_Hello{Hello: signedHello(t, priv, now)},
	}

	if err := <-done; status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Handle err = %v, want PermissionDenied", err)
	}
	if len(repo.admissions) != 1 {
		t.Fatalf("admission failures = %+v, want one", repo.admissions)
	}
	got := repo.admissions[0]
	if got.CredentialKeyID != "key-1" || got.UserID != 42 || got.ConsumedRuntimeID != "runtime-old" {
		t.Fatalf("admission failure = %+v, want credential/user/consumed runtime", got)
	}
	if !strings.Contains(got.Reason, "consumed") {
		t.Fatalf("reason = %q, want consumed", got.Reason)
	}
}

func TestRuntimeChannelResumeUsesLeaseWithoutTouchingCredential(t *testing.T) {
	repo, _, now := newAuthFixture(t, domain.CredentialStatusConsumed)
	repo.cred.ConsumedRuntimeID = "runtime-1"
	token := "resume-token-1"
	repo.lease = domain.RuntimeChannelLease{
		RuntimeID:       "runtime-1",
		UserID:          42,
		CredentialKeyID: "key-1",
		LeaseHash:       hashRuntimeChannelToken(token),
		IssuedAt:        now.Add(-time.Minute),
		ExpiresAt:       now.Add(time.Hour),
		CreatedAt:       now.Add(-time.Minute),
		UpdatedAt:       now.Add(-time.Minute),
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
			Capabilities:    []string{"strategy"},
			ResourceProfile: "small",
			Version:         "0.1.0",
		},
	}
	svc := NewWithInstanceID(repo, "cp-resume")
	svc.SetClock(func() time.Time { return now })

	stream := newFakeRuntimeChannelStream()
	done := make(chan error, 1)
	go func() { done <- svc.Handle(stream) }()
	stream.recv <- &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_RESUME,
		Payload: &cpv1.RuntimeFrame_Resume{
			Resume: &cpv1.RuntimeResume{RuntimeId: "runtime-1", ResumeToken: token},
		},
	}
	ack := waitForHelloAck(t, stream)
	next := ack.GetHelloAck().GetFingerprint()
	if ack.GetHelloAck().GetRuntimeId() != "runtime-1" || next == "" || next == token {
		t.Fatalf("hello_ack = %+v, want runtime-1 and rotated fingerprint", ack.GetHelloAck())
	}
	if repo.touchedAt != nil {
		t.Fatalf("credential was touched on resume: %v", repo.touchedAt)
	}
	if snap := svc.RegistrySnapshot(); len(snap) != 1 || snap[0].RuntimeID != "runtime-1" {
		t.Fatalf("registry snapshot = %+v, want runtime-1", snap)
	}
	stream.cancel()
	_ = <-done
}

func TestRuntimeChannelResumeFromUnhealthyRotatesFingerprint(t *testing.T) {
	repo, _, now := newAuthFixture(t, domain.CredentialStatusConsumed)
	repo.cred.ConsumedRuntimeID = "runtime-1"
	fingerprint := "fingerprint-1"
	repo.lease = domain.RuntimeChannelLease{
		RuntimeID:       "runtime-1",
		UserID:          42,
		CredentialKeyID: "key-1",
		LeaseHash:       hashRuntimeChannelToken(fingerprint),
		IssuedAt:        now.Add(-time.Minute),
		ExpiresAt:       now.Add(time.Hour),
		CreatedAt:       now.Add(-time.Minute),
		UpdatedAt:       now.Add(-time.Minute),
	}
	repo.getRuntime = map[string]domain.Runtime{
		"runtime-1": {
			RuntimeID:       "runtime-1",
			UserID:          42,
			Name:            "default",
			Source:          domain.RuntimeSourceSelfHosted,
			Role:            domain.CredentialRoleExecutor,
			Status:          domain.RuntimeStatusUnhealthy,
			CredentialKeyID: "key-1",
			Capabilities:    []string{"strategy"},
			ResourceProfile: "small",
			Version:         "0.1.0",
		},
	}
	svc := NewWithInstanceID(repo, "cp-resume")
	svc.SetClock(func() time.Time { return now })

	stream := newFakeRuntimeChannelStream()
	done := make(chan error, 1)
	go func() { done <- svc.Handle(stream) }()
	stream.recv <- &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_RESUME,
		Payload: &cpv1.RuntimeFrame_Resume{
			Resume: &cpv1.RuntimeResume{RuntimeId: "runtime-1", Fingerprint: fingerprint},
		},
	}
	ack := waitForHelloAck(t, stream)
	next := ack.GetHelloAck().GetFingerprint()
	if next == "" || next == fingerprint {
		t.Fatalf("hello_ack fingerprint = %q, want new non-empty fingerprint", next)
	}
	if repo.rotatedRuntimeID != "runtime-1" || repo.rotatedLeaseHash != hashRuntimeChannelToken(next) {
		t.Fatalf("rotated lease runtime/hash = %q/%q, want runtime-1 hash(next)", repo.rotatedRuntimeID, repo.rotatedLeaseHash)
	}
	if repo.touchedAt != nil {
		t.Fatalf("credential was touched on resume: %v", repo.touchedAt)
	}
	stream.cancel()
	_ = <-done
}

func TestRuntimeChannelTerminalRuntimeRejectsFingerprintResume(t *testing.T) {
	repo, _, now := newAuthFixture(t, domain.CredentialStatusConsumed)
	repo.cred.ConsumedRuntimeID = "runtime-1"
	fingerprint := "fingerprint-1"
	repo.lease = domain.RuntimeChannelLease{
		RuntimeID:       "runtime-1",
		UserID:          42,
		CredentialKeyID: "key-1",
		LeaseHash:       hashRuntimeChannelToken(fingerprint),
		IssuedAt:        now.Add(-time.Minute),
		ExpiresAt:       now.Add(time.Hour),
		CreatedAt:       now.Add(-time.Minute),
		UpdatedAt:       now.Add(-time.Minute),
	}
	repo.getRuntime = map[string]domain.Runtime{
		"runtime-1": {
			RuntimeID:       "runtime-1",
			UserID:          42,
			Name:            "default",
			Source:          domain.RuntimeSourceSelfHosted,
			Role:            domain.CredentialRoleExecutor,
			Status:          domain.RuntimeStatusCancelled,
			CredentialKeyID: "key-1",
		},
	}
	svc := NewWithInstanceID(repo, "cp-resume")
	svc.SetClock(func() time.Time { return now })

	stream := newFakeRuntimeChannelStream()
	done := make(chan error, 1)
	go func() { done <- svc.Handle(stream) }()
	stream.recv <- &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_RESUME,
		Payload: &cpv1.RuntimeFrame_Resume{
			Resume: &cpv1.RuntimeResume{RuntimeId: "runtime-1", Fingerprint: fingerprint},
		},
	}

	if err := <-done; status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Handle err = %v, want FailedPrecondition", err)
	}
	if repo.rotatedLeaseHash != "" {
		t.Fatalf("terminal resume rotated lease hash %q", repo.rotatedLeaseHash)
	}
	if len(repo.admissions) != 1 {
		t.Fatalf("admission failures = %+v, want one terminal resume failure", repo.admissions)
	}
	got := repo.admissions[0]
	if got.UserID != 42 || got.CredentialKeyID != "key-1" || got.RequestedRuntimeID != "runtime-1" {
		t.Fatalf("admission failure = %+v, want runtime/user/credential", got)
	}
	if strings.Contains(got.Reason, fingerprint) {
		t.Fatalf("admission failure leaked fingerprint in reason: %q", got.Reason)
	}
}

func TestRuntimeChannelHeartbeatRotatesFingerprint(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusDownloaded)
	svc := NewWithInstanceID(repo, "cp-heartbeat")
	svc.SetClock(func() time.Time { return now })

	stream := newFakeRuntimeChannelStream()
	done := make(chan error, 1)
	go func() { done <- svc.Handle(stream) }()
	stream.recv <- &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO,
		Payload:   &cpv1.RuntimeFrame_Hello{Hello: signedHello(t, priv, now)},
	}
	ack := waitForHelloAck(t, stream)
	first := ack.GetHelloAck().GetFingerprint()
	if first == "" {
		t.Fatalf("hello_ack fingerprint is empty")
	}

	stream.recv <- &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HEARTBEAT,
		Payload: &cpv1.RuntimeFrame_Heartbeat{
			Heartbeat: &cpv1.Heartbeat{
				SentAtUnixMs: now.Add(time.Second).UnixMilli(),
				Fingerprint:  first,
			},
		},
	}
	hbAck := waitForFrameType(t, stream, cpv1.FrameType_FRAME_TYPE_HEARTBEAT_ACK)
	next := hbAck.GetHeartbeatAck().GetFingerprint()
	if next == "" || next == first {
		t.Fatalf("heartbeat ack fingerprint = %q, want rotated", next)
	}
	if repo.rotatedRuntimeID != "runtime-1" || repo.rotatedLeaseHash != hashRuntimeChannelToken(next) {
		t.Fatalf("rotated lease runtime/hash = %q/%q, want runtime-1 hash(next)", repo.rotatedRuntimeID, repo.rotatedLeaseHash)
	}
	stream.cancel()
	_ = <-done
}

func TestRuntimeChannelRecordsAndClearsConnectionOwner(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusDownloaded)
	svc := NewWithInstanceID(repo, "cp-test-a")
	svc.SetClock(func() time.Time { return now })

	stream := newFakeRuntimeChannelStream()
	done := make(chan error, 1)
	go func() { done <- svc.Handle(stream) }()
	stream.recv <- &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO,
		Payload:   &cpv1.RuntimeFrame_Hello{Hello: signedHello(t, priv, now)},
	}

	deadline := time.Now().Add(time.Second)
	for repo.ownerInstance == "" && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if repo.ownerInstance != "cp-test-a" {
		t.Fatalf("owner = %q, want cp-test-a", repo.ownerInstance)
	}
	stream.cancel()
	_ = <-done
	if !repo.ownerCleared || repo.ownerInstance != "" {
		t.Fatalf("owner clear = %v owner=%q, want cleared", repo.ownerCleared, repo.ownerInstance)
	}
}

func TestRegistryRejectsSameCredentialForDifferentRuntime(t *testing.T) {
	registry := NewRegistry()
	now := time.Unix(1_700_000_000, 0)
	if _, err := registry.Register(AuthenticatedRuntime{KeyID: "key-1", RuntimeID: "runtime-a"}, now); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	if _, err := registry.Register(AuthenticatedRuntime{KeyID: "key-1", RuntimeID: "runtime-b"}, now); !errors.Is(err, ErrRuntimeCredentialConnected) {
		t.Fatalf("second Register err = %v, want ErrRuntimeCredentialConnected", err)
	}
}

func TestRegistryCloseByKeyIDUsesDoubleIndex(t *testing.T) {
	svc := New(&stubRepo{
		cred:         domain.RuntimeCredential{KeyID: "key-1"},
		endedByKeyID: map[string]int64{"key-1": 1},
	})
	now := time.Unix(1_700_000_000, 0)
	mustRegister(t, svc.registry, AuthenticatedRuntime{KeyID: "key-1", RuntimeID: "runtime-a"}, now)
	mustRegister(t, svc.registry, AuthenticatedRuntime{KeyID: "key-2", RuntimeID: "runtime-c"}, now)

	closed, ended, err := svc.CloseStreamsForKey(context.Background(), "key-1")
	if err != nil {
		t.Fatalf("CloseStreamsForKey: %v", err)
	}
	if closed != 1 || ended != 1 {
		t.Fatalf("counters = (%d, %d), want (1, 1)", closed, ended)
	}
	snap := svc.RegistrySnapshot()
	if len(snap) != 1 || snap[0].RuntimeID != "runtime-c" {
		t.Fatalf("snapshot after close = %+v", snap)
	}
}

func TestInvokeStrategyUnaryByRuntimeIDTargetsSelectedStream(t *testing.T) {
	svc := New(&stubRepo{cred: domain.RuntimeCredential{KeyID: "key-1"}})
	now := time.Unix(1_700_000_000, 0)
	streamA := mustRegister(t, svc.registry, AuthenticatedRuntime{
		KeyID:     "key-1",
		UserID:    42,
		RuntimeID: "runtime-a",
		Name:      "default",
	}, now)
	streamB := mustRegister(t, svc.registry, AuthenticatedRuntime{
		KeyID:     "key-2",
		UserID:    42,
		RuntimeID: "runtime-b",
		Name:      "default",
	}, now)
	sentA := make(chan *cpv1.RuntimeFrame, 1)
	sentB := make(chan *cpv1.RuntimeFrame, 1)
	streamA.setSender(func(frame *cpv1.RuntimeFrame) error {
		sentA <- frame
		return nil
	})
	streamB.setSender(func(frame *cpv1.RuntimeFrame) error {
		sentB <- frame
		return nil
	})

	done := make(chan error, 1)
	go func() {
		resp := &strategyv1.RunStrategyResponse{}
		err := svc.InvokeStrategyUnaryByRuntimeID(
			context.Background(),
			42,
			"runtime-b",
			"RunStrategy",
			&strategyv1.RunStrategyRequest{AccountId: 7, UserId: 42, RuntimeId: "runtime-b"},
			resp,
		)
		if err == nil && resp.GetSessionId() != "sess-b" {
			err = status.Errorf(codes.Internal, "session_id=%q", resp.GetSessionId())
		}
		done <- err
	}()

	var req *cpv1.RuntimeFrame
	select {
	case req = <-sentB:
	case <-time.After(time.Second):
		t.Fatal("runtime-b did not receive request")
	}
	select {
	case frame := <-sentA:
		t.Fatalf("runtime-a received unexpected frame: %+v", frame)
	default:
	}
	if streamA.deliver(&cpv1.RuntimeFrame{
		CorrelationId: req.GetCorrelationId(),
		FrameType:     cpv1.FrameType_FRAME_TYPE_RESPONSE,
	}) {
		t.Fatal("runtime-a response completed runtime-b request")
	}

	packedResp, err := anypb.New(&strategyv1.RunStrategyResponse{SessionId: "sess-b"})
	if err != nil {
		t.Fatal(err)
	}
	if !streamB.deliver(&cpv1.RuntimeFrame{
		CorrelationId: req.GetCorrelationId(),
		FrameType:     cpv1.FrameType_FRAME_TYPE_RESPONSE,
		Payload: &cpv1.RuntimeFrame_Response{
			Response: &cpv1.StrategyResponse{Response: packedResp},
		},
	}) {
		t.Fatal("response was not delivered")
	}
	if err := <-done; err != nil {
		t.Fatalf("InvokeStrategyUnaryByRuntimeID: %v", err)
	}
}

func TestInvokeStrategyUnaryByRuntimeIDInjectsTraceContext(t *testing.T) {
	oldProvider := otel.GetTracerProvider()
	oldPropagator := otel.GetTextMapPropagator()
	provider := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer func() {
		otel.SetTracerProvider(oldProvider)
		otel.SetTextMapPropagator(oldPropagator)
		_ = provider.Shutdown(context.Background())
	}()

	svc := New(&stubRepo{cred: domain.RuntimeCredential{KeyID: "key-1"}})
	now := time.Unix(1_700_000_000, 0)
	stream := mustRegister(t, svc.registry, AuthenticatedRuntime{
		KeyID:     "key-1",
		UserID:    42,
		RuntimeID: "runtime-trace",
		Name:      "default",
	}, now)
	sent := make(chan *cpv1.RuntimeFrame, 1)
	stream.setSender(func(frame *cpv1.RuntimeFrame) error {
		sent <- frame
		return nil
	})

	ctx, span := provider.Tracer("runtimechannel-test").Start(context.Background(), "parent")
	defer span.End()
	done := make(chan error, 1)
	go func() {
		done <- svc.InvokeStrategyUnaryByRuntimeID(
			ctx,
			42,
			"runtime-trace",
			"RunStrategy",
			&strategyv1.RunStrategyRequest{AccountId: 7, UserId: 42, RuntimeId: "runtime-trace"},
			&strategyv1.RunStrategyResponse{},
		)
	}()

	var req *cpv1.RuntimeFrame
	select {
	case req = <-sent:
	case <-time.After(time.Second):
		t.Fatal("runtime did not receive request")
	}
	traceparent := req.GetRequest().GetTraceContext()["traceparent"]
	if traceparent == "" {
		t.Fatalf("trace_context = %+v, want traceparent", req.GetRequest().GetTraceContext())
	}
	if !strings.Contains(traceparent, span.SpanContext().TraceID().String()) {
		t.Fatalf("traceparent = %q, want trace id %s", traceparent, span.SpanContext().TraceID())
	}

	packedResp, err := anypb.New(&strategyv1.RunStrategyResponse{SessionId: "sess-trace"})
	if err != nil {
		t.Fatal(err)
	}
	stream.deliver(&cpv1.RuntimeFrame{
		CorrelationId: req.GetCorrelationId(),
		FrameType:     cpv1.FrameType_FRAME_TYPE_RESPONSE,
		Payload: &cpv1.RuntimeFrame_Response{
			Response: &cpv1.StrategyResponse{Response: packedResp},
		},
	})
	if err := <-done; err != nil {
		t.Fatalf("InvokeStrategyUnaryByRuntimeID: %v", err)
	}
}

func TestInvokeStrategyUnaryByRuntimeIDSupportsGetStrategyStatus(t *testing.T) {
	svc := New(&stubRepo{cred: domain.RuntimeCredential{KeyID: "key-1"}})
	now := time.Unix(1_700_000_000, 0)
	stream := mustRegister(t, svc.registry, AuthenticatedRuntime{
		KeyID:     "key-1",
		UserID:    42,
		RuntimeID: "runtime-status",
		Name:      "default",
		Source:    domain.RuntimeSourceHosted,
	}, now)
	sent := make(chan *cpv1.RuntimeFrame, 1)
	stream.setSender(func(frame *cpv1.RuntimeFrame) error {
		sent <- frame
		return nil
	})

	done := make(chan error, 1)
	go func() {
		resp := &strategyv1.GetStrategyStatusResponse{}
		err := svc.InvokeStrategyUnaryByRuntimeID(
			context.Background(),
			42,
			"runtime-status",
			"GetStrategyStatus",
			&strategyv1.GetStrategyStatusRequest{SessionId: "sess-1", UserId: 42, RuntimeId: "runtime-status"},
			resp,
		)
		if err == nil && (resp.GetStatus() != "running" || resp.GetBarsProcessed() != 9) {
			err = status.Errorf(codes.Internal, "unexpected status response: %+v", resp)
		}
		done <- err
	}()

	req := <-sent
	if req.GetRequest().GetMethod() != "GetStrategyStatus" {
		t.Fatalf("method = %q, want GetStrategyStatus", req.GetRequest().GetMethod())
	}
	packedResp, err := anypb.New(&strategyv1.GetStrategyStatusResponse{Status: "running", BarsProcessed: 9})
	if err != nil {
		t.Fatal(err)
	}
	if !stream.deliver(&cpv1.RuntimeFrame{
		CorrelationId: req.GetCorrelationId(),
		FrameType:     cpv1.FrameType_FRAME_TYPE_RESPONSE,
		Payload: &cpv1.RuntimeFrame_Response{
			Response: &cpv1.StrategyResponse{Response: packedResp},
		},
	}) {
		t.Fatal("status response was not delivered")
	}
	if err := <-done; err != nil {
		t.Fatalf("InvokeStrategyUnaryByRuntimeID: %v", err)
	}
}

func TestInvokeStrategyUnaryByRuntimeIDFailsClosedWhenDisconnected(t *testing.T) {
	svc := New(&stubRepo{cred: domain.RuntimeCredential{KeyID: "key-1"}})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := svc.InvokeStrategyUnaryByRuntimeID(
		ctx,
		42,
		"runtime-missing",
		"RunStrategy",
		&strategyv1.RunStrategyRequest{AccountId: 7, UserId: 42, RuntimeId: "runtime-missing"},
		&strategyv1.RunStrategyResponse{},
	)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable (err=%v)", status.Code(err), err)
	}
}

func TestInvokeStrategyUnaryByRuntimeIDUnblocksWhenStreamUnregisters(t *testing.T) {
	svc := New(&stubRepo{cred: domain.RuntimeCredential{KeyID: "key-1"}})
	now := time.Unix(1_700_000_000, 0)
	stream := mustRegister(t, svc.registry, AuthenticatedRuntime{
		KeyID:     "key-1",
		UserID:    42,
		RuntimeID: "runtime-disconnect",
		Name:      "default",
		Source:    domain.RuntimeSourceSelfHosted,
	}, now)
	sent := make(chan *cpv1.RuntimeFrame, 1)
	stream.setSender(func(frame *cpv1.RuntimeFrame) error {
		sent <- frame
		return nil
	})

	done := make(chan error, 1)
	go func() {
		done <- svc.InvokeStrategyUnaryByRuntimeID(
			context.Background(),
			42,
			"runtime-disconnect",
			"RunStrategy",
			&strategyv1.RunStrategyRequest{AccountId: 7, UserId: 42, RuntimeId: "runtime-disconnect"},
			&strategyv1.RunStrategyResponse{},
		)
	}()

	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("runtime did not receive request")
	}
	svc.registry.Unregister("runtime-disconnect")

	select {
	case err := <-done:
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("code = %v, want Unavailable (err=%v)", status.Code(err), err)
		}
	case <-time.After(time.Second):
		t.Fatal("InvokeStrategyUnaryByRuntimeID did not unblock after stream unregister")
	}
}

func TestInvokeStrategyUnaryByRuntimeIDRequiresRuntimeID(t *testing.T) {
	svc := New(&stubRepo{cred: domain.RuntimeCredential{KeyID: "key-1"}})
	err := svc.InvokeStrategyUnaryByRuntimeID(
		context.Background(),
		42,
		"",
		"RunStrategy",
		&strategyv1.RunStrategyRequest{AccountId: 7, UserId: 42},
		&strategyv1.RunStrategyResponse{},
	)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument (err=%v)", status.Code(err), err)
	}
}

func TestMetricsSnapshotIncludesStreamHealthFields(t *testing.T) {
	svc := New(&stubRepo{cred: domain.RuntimeCredential{KeyID: "key-1"}})
	opened := time.Unix(1_700_000_000, 0)
	svc.SetClock(func() time.Time { return opened.Add(5 * time.Second) })
	stream := mustRegister(t, svc.registry, AuthenticatedRuntime{
		KeyID:     "key-1",
		UserID:    42,
		RuntimeID: "runtime-a",
		Name:      "default",
	}, opened)
	stream.registerCall("corr-1")

	metrics := svc.MetricsSnapshot()

	if len(metrics) != 1 {
		t.Fatalf("metrics len = %d, want 1", len(metrics))
	}
	if metrics[0].RuntimeID != "runtime-a" || metrics[0].InFlightCalls != 1 {
		t.Fatalf("metrics = %+v", metrics[0])
	}
	if metrics[0].Uptime != 5*time.Second || metrics[0].LastFrameLatency != 5*time.Second {
		t.Fatalf("timing metrics = %+v", metrics[0])
	}
}

func TestRuntimeChannelHeartbeatKeepsIdleStreamAlive(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusActive)
	svc := New(repo)
	clock := now
	svc.SetClock(func() time.Time { return clock })
	svc.streamIdleTimeout = 40 * time.Millisecond
	svc.streamCheckInterval = 5 * time.Millisecond

	stream := newFakeRuntimeChannelStream()
	done := make(chan error, 1)
	go func() { done <- svc.Handle(stream) }()

	stream.recv <- &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO,
		Payload:   &cpv1.RuntimeFrame_Hello{Hello: signedHello(t, priv, now)},
	}
	time.Sleep(15 * time.Millisecond)
	clock = now.Add(30 * time.Millisecond)
	stream.recv <- &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HEARTBEAT,
		Payload: &cpv1.RuntimeFrame_Heartbeat{Heartbeat: &cpv1.Heartbeat{
			SentAtUnixMs: clock.UnixMilli(),
			Fingerprint:  waitForHelloAck(t, stream).GetHelloAck().GetFingerprint(),
		}},
	}
	time.Sleep(20 * time.Millisecond)

	if snap := svc.RegistrySnapshot(); len(snap) != 1 || snap[0].RuntimeID != "runtime-1" {
		t.Fatalf("registry snapshot = %+v, want active runtime", snap)
	}
	stream.cancel()
	if err := <-done; status.Code(err) != codes.Unavailable {
		t.Fatalf("Handle err = %v, want Unavailable after cancel", err)
	}
}

func TestRuntimeChannelRejectsCredentialAlreadyBound(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusActive)
	repo.createErr = fmt.Errorf("%w: credential already bound to runtime runtime-other", repository.ErrConflict)
	svc := New(repo)
	svc.SetClock(func() time.Time { return now })

	stream := newFakeRuntimeChannelStream()
	done := make(chan error, 1)
	go func() { done <- svc.Handle(stream) }()

	stream.recv <- &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO,
		Payload:   &cpv1.RuntimeFrame_Hello{Hello: signedHello(t, priv, now)},
	}

	if err := <-done; status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Handle err = %v, want PermissionDenied", err)
	}
	if snap := svc.RegistrySnapshot(); len(snap) != 0 {
		t.Fatalf("registry snapshot = %+v, want empty", snap)
	}
}

func TestRuntimeChannelRejectsGenericRepositoryConflictWithoutCredentialMask(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusActive)
	repo.createErr = fmt.Errorf("%w: runtime name already occupied by runtime-other", repository.ErrConflict)
	svc := New(repo)
	svc.SetClock(func() time.Time { return now })

	stream := newFakeRuntimeChannelStream()
	done := make(chan error, 1)
	go func() { done <- svc.Handle(stream) }()

	stream.recv <- &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO,
		Payload:   &cpv1.RuntimeFrame_Hello{Hello: signedHello(t, priv, now)},
	}

	err := <-done
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Handle code = %v, want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	if strings.Contains(err.Error(), "credential is already bound") {
		t.Fatalf("generic conflict was masked as credential binding: %v", err)
	}
	if !strings.Contains(err.Error(), "runtime name already occupied") {
		t.Fatalf("Handle err = %v, want underlying conflict message", err)
	}
}

func TestRuntimeChannelRejectsEndedRuntimeRegistration(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusActive)
	repo.createErr = repository.ErrConflict
	repo.getRuntime = map[string]domain.Runtime{
		"runtime-1": {
			RuntimeID: "runtime-1",
			UserID:    42,
			Name:      "desk",
			Status:    domain.RuntimeStatusEnded,
		},
	}
	svc := New(repo)
	svc.SetClock(func() time.Time { return now })

	stream := newFakeRuntimeChannelStream()
	done := make(chan error, 1)
	go func() { done <- svc.Handle(stream) }()

	stream.recv <- &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO,
		Payload:   &cpv1.RuntimeFrame_Hello{Hello: signedHello(t, priv, now)},
	}

	if err := <-done; status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "runtime already ended") {
		t.Fatalf("Handle err = %v, want FailedPrecondition runtime already ended", err)
	}
	if snap := svc.RegistrySnapshot(); len(snap) != 0 {
		t.Fatalf("registry snapshot = %+v, want empty", snap)
	}
}

func TestRuntimeChannelDispatchesRuntimeOriginatedPlatformRequest(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusActive)
	svc := New(repo)
	dispatcher := &fakePlatformDispatcher{
		resp: &accountv1.SaveSessionResponse{},
	}
	svc.SetPlatformDispatcher(dispatcher)
	svc.SetClock(func() time.Time { return now })

	stream := newFakeRuntimeChannelStream()
	done := make(chan error, 1)
	go func() { done <- svc.Handle(stream) }()
	stream.recv <- &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO,
		Payload:   &cpv1.RuntimeFrame_Hello{Hello: signedHello(t, priv, now)},
	}
	waitForHelloAck(t, stream)
	payload, err := anypb.New(&accountv1.SaveSessionRequest{SessionId: "sess-1", AccountId: 7})
	if err != nil {
		t.Fatal(err)
	}
	stream.recv <- &cpv1.RuntimeFrame{
		CorrelationId: "corr-platform",
		FrameType:     cpv1.FrameType_FRAME_TYPE_REQUEST,
		Payload: &cpv1.RuntimeFrame_Request{
			Request: &cpv1.StrategyRequest{
				Method:  "account.SaveSession",
				Request: payload,
			},
		},
	}

	var frame *cpv1.RuntimeFrame
	select {
	case frame = <-stream.sent:
	case <-time.After(time.Second):
		t.Fatal("runtime platform response not sent")
	}
	if frame.GetFrameType() != cpv1.FrameType_FRAME_TYPE_RESPONSE {
		t.Fatalf("frame_type = %s, want RESPONSE", frame.GetFrameType())
	}
	if dispatcher.method != "account.SaveSession" || dispatcher.rt.RuntimeID != "runtime-1" || dispatcher.rt.UserID != 42 {
		t.Fatalf("dispatcher saw method=%q runtime=%+v", dispatcher.method, dispatcher.rt)
	}
	stream.cancel()
	if err := <-done; status.Code(err) != codes.Unavailable {
		t.Fatalf("Handle err = %v, want Unavailable after cancel", err)
	}
}

func TestRuntimeChannelNoFrameTimeoutDeclaresStreamDead(t *testing.T) {
	repo, priv, now := newAuthFixture(t, domain.CredentialStatusActive)
	svc := New(repo)
	clock := now
	svc.SetClock(func() time.Time { return clock })
	svc.streamIdleTimeout = 15 * time.Millisecond
	svc.streamCheckInterval = 5 * time.Millisecond

	stream := newFakeRuntimeChannelStream()
	done := make(chan error, 1)
	go func() { done <- svc.Handle(stream) }()
	stream.recv <- &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO,
		Payload:   &cpv1.RuntimeFrame_Hello{Hello: signedHello(t, priv, now)},
	}
	time.Sleep(20 * time.Millisecond)
	clock = now.Add(30 * time.Millisecond)

	err := <-done
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Handle err = %v, want Unavailable", err)
	}
	if snap := svc.RegistrySnapshot(); len(snap) != 0 {
		t.Fatalf("registry snapshot after timeout = %+v, want empty", snap)
	}
}

func TestRuntimeChannelDispatchesRuntimeOriginatedPlatformRequestWithTraceContext(t *testing.T) {
	oldPropagator := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	defer otel.SetTextMapPropagator(oldPropagator)

	repo, priv, now := newAuthFixture(t, domain.CredentialStatusActive)
	svc := New(repo)
	dispatcher := &fakePlatformDispatcher{
		resp: &accountv1.SaveSessionResponse{},
	}
	svc.SetPlatformDispatcher(dispatcher)
	svc.SetClock(func() time.Time { return now })

	stream := newFakeRuntimeChannelStream()
	done := make(chan error, 1)
	go func() { done <- svc.Handle(stream) }()
	stream.recv <- &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO,
		Payload:   &cpv1.RuntimeFrame_Hello{Hello: signedHello(t, priv, now)},
	}
	waitForHelloAck(t, stream)
	payload, err := anypb.New(&accountv1.SaveSessionRequest{SessionId: "sess-1", AccountId: 7})
	if err != nil {
		t.Fatal(err)
	}
	const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	stream.recv <- &cpv1.RuntimeFrame{
		CorrelationId: "corr-platform",
		FrameType:     cpv1.FrameType_FRAME_TYPE_REQUEST,
		Payload: &cpv1.RuntimeFrame_Request{
			Request: &cpv1.StrategyRequest{
				Method:       "account.SaveSession",
				Request:      payload,
				TraceContext: map[string]string{"traceparent": traceparent},
			},
		},
	}

	select {
	case <-stream.sent:
	case <-time.After(time.Second):
		t.Fatal("runtime platform response not sent")
	}
	got := trace.SpanContextFromContext(dispatcher.ctx).TraceID().String()
	if got != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("dispatcher trace id = %q, want propagated trace id", got)
	}
	stream.cancel()
	if err := <-done; status.Code(err) != codes.Unavailable {
		t.Fatalf("Handle err = %v, want Unavailable after cancel", err)
	}
}

type fakePlatformDispatcher struct {
	rt     AuthenticatedRuntime
	method string
	resp   proto.Message
	ctx    context.Context
}

func (f *fakePlatformDispatcher) DispatchRuntimeRequest(ctx context.Context, rt AuthenticatedRuntime, method string, payload *anypb.Any) (proto.Message, error) {
	f.ctx = ctx
	f.rt = rt
	f.method = method
	if payload == nil {
		return nil, status.Error(codes.InvalidArgument, "missing payload")
	}
	return f.resp, nil
}

func newAuthFixture(t *testing.T, status domain.CredentialStatus) (*stubRepo, ed25519.PrivateKey, time.Time) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	repo := &stubRepo{
		cred: domain.RuntimeCredential{
			KeyID:        "key-1",
			UserID:       42,
			Role:         domain.CredentialRoleExecutor,
			PublicKeyPEM: string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})),
			Status:       status,
		},
	}
	return repo, priv, time.Unix(1_700_000_000, 0).UTC()
}

type fakeRuntimeChannelStream struct {
	cpv1.ControlPanelService_RuntimeChannelServer
	ctx    context.Context
	cancel context.CancelFunc
	recv   chan *cpv1.RuntimeFrame
	sent   chan *cpv1.RuntimeFrame
}

func newFakeRuntimeChannelStream() *fakeRuntimeChannelStream {
	ctx, cancel := context.WithCancel(context.Background())
	return &fakeRuntimeChannelStream{
		ctx:    ctx,
		cancel: cancel,
		recv:   make(chan *cpv1.RuntimeFrame, 8),
		sent:   make(chan *cpv1.RuntimeFrame, 8),
	}
}

func (s *fakeRuntimeChannelStream) Context() context.Context { return s.ctx }

func (s *fakeRuntimeChannelStream) Recv() (*cpv1.RuntimeFrame, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case frame, ok := <-s.recv:
		if !ok {
			return nil, io.EOF
		}
		return frame, nil
	}
}

func (s *fakeRuntimeChannelStream) Send(frame *cpv1.RuntimeFrame) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.sent <- frame:
		return nil
	}
}

func (s *fakeRuntimeChannelStream) SendHeader(metadata.MD) error { return nil }
func (s *fakeRuntimeChannelStream) SetHeader(metadata.MD) error  { return nil }
func (s *fakeRuntimeChannelStream) SetTrailer(metadata.MD)       {}
func (s *fakeRuntimeChannelStream) SendMsg(any) error            { return nil }
func (s *fakeRuntimeChannelStream) RecvMsg(any) error            { return nil }

func waitForHelloAck(t *testing.T, stream *fakeRuntimeChannelStream) *cpv1.RuntimeFrame {
	t.Helper()
	return waitForFrameType(t, stream, cpv1.FrameType_FRAME_TYPE_HELLO_ACK)
}

func waitForFrameType(t *testing.T, stream *fakeRuntimeChannelStream, typ cpv1.FrameType) *cpv1.RuntimeFrame {
	t.Helper()
	select {
	case frame := <-stream.sent:
		if frame.GetFrameType() != typ {
			t.Fatalf("frame_type = %s, want %s", frame.GetFrameType(), typ)
		}
		return frame
	case <-time.After(time.Second):
		t.Fatalf("%s frame not sent", typ)
		return nil
	}
}

func signedHello(t *testing.T, priv ed25519.PrivateKey, at time.Time) *cpv1.RuntimeHello {
	t.Helper()
	hello := &cpv1.RuntimeHello{
		KeyId:           "key-1",
		RuntimeId:       "runtime-1",
		Name:            "default",
		Capabilities:    []string{"strategy", "spot", "futures"},
		ResourceProfile: "small",
		Version:         "0.1.0",
		IssuedAtUnixMs:  at.UnixMilli(),
		Nonce:           base64.RawURLEncoding.EncodeToString([]byte("1234567890abcdef")),
	}
	payload, err := CanonicalHelloPayload(hello)
	if err != nil {
		t.Fatal(err)
	}
	hello.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(priv, payload))
	return hello
}

func mustRegister(t *testing.T, registry *Registry, rt AuthenticatedRuntime, at time.Time) *runtimeStream {
	t.Helper()
	stream, err := registry.Register(rt, at)
	if err != nil {
		t.Fatalf("Register(%s): %v", rt.RuntimeID, err)
	}
	return stream
}
