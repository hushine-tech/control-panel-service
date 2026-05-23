package runtimechannel

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	"github.com/hushine-tech/control-panel-service/internal/domain"
	"github.com/hushine-tech/control-panel-service/internal/repository"
)

const streamDeadAfter = 90 * time.Second
const runtimeChannelLeaseTTL = 24 * time.Hour

var ErrRuntimeAlreadyEnded = errors.New("runtime already ended")

type Service struct {
	repo                Repository
	registry            *Registry
	replay              *ReplayCache
	platform            PlatformDispatcher
	dataTransfer        RuntimeDataTransfer
	dataWindow          *RuntimeDataWindow
	instanceID          string
	now                 func() time.Time
	streamIdleTimeout   time.Duration
	streamCheckInterval time.Duration
}

func New(repo Repository) *Service {
	return NewWithInstanceID(repo, "")
}

func NewWithInstanceID(repo Repository, instanceID string) *Service {
	if instanceID == "" {
		instanceID = fmt.Sprintf("control-panel-%d", time.Now().UnixNano())
	}
	return &Service{
		repo:                repo,
		registry:            NewRegistry(),
		replay:              NewReplayCache(replayTTL, 8192),
		dataWindow:          NewRuntimeDataWindow(1024),
		instanceID:          instanceID,
		now:                 time.Now,
		streamIdleTimeout:   streamDeadAfter,
		streamCheckInterval: time.Second,
	}
}

func (s *Service) SetClock(now func() time.Time) {
	if now != nil {
		s.now = now
	}
}

func (s *Service) SetPlatformDispatcher(dispatcher PlatformDispatcher) {
	s.platform = dispatcher
}

func (s *Service) SetDataTransfer(transfer RuntimeDataTransfer) {
	s.dataTransfer = transfer
}

func (s *Service) InstanceID() string {
	if s == nil {
		return ""
	}
	return s.instanceID
}

func (s *Service) RegistrySnapshot() []AuthenticatedRuntime {
	return s.registry.Snapshot()
}

func (s *Service) MetricsSnapshot() []StreamMetric {
	return s.registry.MetricsSnapshot(s.now().UTC())
}

type recvResult struct {
	frame *cpv1.RuntimeFrame
	err   error
}

// Handle owns one RuntimeChannel stream. The first received frame must be a
// signed HELLO; after that this minimal D3 section-2 implementation consumes
// HEARTBEAT and future response/progress frames, keeping registry liveness
// accurate for the proxy work in section 3.
func (s *Service) Handle(stream cpv1.ControlPanelService_RuntimeChannelServer) error {
	first, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return status.Errorf(codes.Unavailable, "receive runtime hello: %v", err)
	}
	rt, resumeToken, resumeExpiresAt, err := s.authenticateFirstFrame(stream.Context(), first)
	if err != nil {
		return err
	}
	if err := s.recordConnectionOwner(stream.Context(), rt.RuntimeID, rt.AuthenticatedAt); err != nil {
		return status.Errorf(codes.Unavailable, "record runtime connection owner: %v", err)
	}
	rs, err := s.registry.Register(rt, s.now().UTC())
	if err != nil {
		if errors.Is(err, ErrRuntimeCredentialConnected) {
			return status.Error(codes.PermissionDenied, "runtime credential is already connected by another runtime")
		}
		return status.Errorf(codes.Unavailable, "register runtime channel: %v", err)
	}
	rs.setSender(stream.Send)
	if err := rs.sendFrame(&cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO_ACK,
		Payload: &cpv1.RuntimeFrame_HelloAck{
			HelloAck: &cpv1.RuntimeHelloAck{
				RuntimeId:            rt.RuntimeID,
				ResumeToken:          resumeToken,
				ResumeTokenExpiresAt: timestamppb.New(resumeExpiresAt),
				Fingerprint:          resumeToken,
				FingerprintExpiresAt: timestamppb.New(resumeExpiresAt),
			},
		},
	}); err != nil {
		return status.Errorf(codes.Unavailable, "send runtime hello ack: %v", err)
	}
	defer func() {
		s.registry.Unregister(rt.RuntimeID)
		_ = s.repo.ClearRuntimeConnectionOwner(context.Background(), rt.RuntimeID, s.instanceID)
	}()

	recvCh := make(chan recvResult, 1)
	go func() {
		for {
			frame, err := stream.Recv()
			recvCh <- recvResult{frame: frame, err: err}
			if err != nil {
				return
			}
		}
	}()

	ticker := time.NewTicker(s.streamCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return status.Error(codes.Unavailable, "runtime channel disconnected")
		case <-rs.closed:
			return status.Error(codes.PermissionDenied, "runtime channel closed")
		case res := <-recvCh:
			if res.err != nil {
				if errors.Is(res.err, io.EOF) {
					return nil
				}
				return status.Errorf(codes.Unavailable, "runtime channel receive failed: %v", res.err)
			}
			now := s.now().UTC()
			if err := validatePostHelloFrame(res.frame); err != nil {
				return err
			}
			if res.frame.GetFrameType() == cpv1.FrameType_FRAME_TYPE_HEARTBEAT {
				nextFingerprint, nextExpiresAt, err := s.rotateRuntimeFingerprint(stream.Context(), rt.RuntimeID, res.frame.GetHeartbeat().GetFingerprint())
				if err != nil {
					return err
				}
				rs.touch(now)
				if err := s.recordHeartbeat(stream.Context(), rt.RuntimeID, now); err != nil {
					if errors.Is(err, ErrRuntimeAlreadyEnded) {
						return status.Error(codes.FailedPrecondition, "runtime already ended")
					}
					return status.Errorf(codes.Unavailable, "record runtime heartbeat: %v", err)
				}
				_ = s.recordConnectionOwner(stream.Context(), rt.RuntimeID, now)
				if err := rs.sendFrame(&cpv1.RuntimeFrame{
					FrameType: cpv1.FrameType_FRAME_TYPE_HEARTBEAT_ACK,
					Payload: &cpv1.RuntimeFrame_HeartbeatAck{
						HeartbeatAck: &cpv1.RuntimeHeartbeatAck{
							RuntimeId:            rt.RuntimeID,
							Fingerprint:          nextFingerprint,
							FingerprintExpiresAt: timestamppb.New(nextExpiresAt),
						},
					},
				}); err != nil {
					return status.Errorf(codes.Unavailable, "send runtime heartbeat ack: %v", err)
				}
				continue
			}
			rs.touch(now)
			if err := s.recordHeartbeat(stream.Context(), rt.RuntimeID, now); err != nil {
				if errors.Is(err, ErrRuntimeAlreadyEnded) {
					return status.Error(codes.FailedPrecondition, "runtime already ended")
				}
				return status.Errorf(codes.Unavailable, "record runtime heartbeat: %v", err)
			}
			_ = s.recordConnectionOwner(stream.Context(), rt.RuntimeID, now)
			switch res.frame.GetFrameType() {
			case cpv1.FrameType_FRAME_TYPE_RESPONSE,
				cpv1.FrameType_FRAME_TYPE_PROGRESS,
				cpv1.FrameType_FRAME_TYPE_ERROR:
				_ = rs.deliver(res.frame)
			case cpv1.FrameType_FRAME_TYPE_REQUEST:
				go s.handleRuntimeRequest(stream.Context(), rs, res.frame)
			case cpv1.FrameType_FRAME_TYPE_COMMAND_ACK,
				cpv1.FrameType_FRAME_TYPE_COMMAND_RESULT:
				if rs.deliver(res.frame) {
					continue
				}
				if err := s.handleRuntimeCommandFrame(stream.Context(), res.frame); err != nil {
					return err
				}
			case cpv1.FrameType_FRAME_TYPE_STATUS_PATCH,
				cpv1.FrameType_FRAME_TYPE_DATA_BACKPRESSURE,
				cpv1.FrameType_FRAME_TYPE_DATA_END:
				// Accepted protocol frames. Section 6/7 wire these into
				// worker-health and stream-delivery state; for section 5 the
				// important guardrail is that they do not poison the stream.
			case cpv1.FrameType_FRAME_TYPE_DATA_ACK:
				s.handleRuntimeDataAck(res.frame)
			}
		case <-ticker.C:
			if s.now().UTC().Sub(rs.lastFrame()) > s.streamIdleTimeout {
				return status.Error(codes.Unavailable, "runtime channel heartbeat timeout")
			}
		}
	}
}

func (s *Service) authenticateFirstFrame(ctx context.Context, first *cpv1.RuntimeFrame) (AuthenticatedRuntime, string, time.Time, error) {
	switch first.GetFrameType() {
	case cpv1.FrameType_FRAME_TYPE_HELLO:
		if first.GetHello() == nil {
			return AuthenticatedRuntime{}, "", time.Time{}, status.Error(codes.InvalidArgument, "HELLO frame missing payload")
		}
		rt, err := verifyHello(ctx, s.repo, s.replay, s.now, first.GetHello())
		if err != nil {
			s.recordAdmissionFailure(ctx, first.GetHello(), rt, err)
			return AuthenticatedRuntime{}, "", time.Time{}, helloErrorToStatus(err)
		}
		if err := s.upsertRuntime(ctx, rt); err != nil {
			s.recordAdmissionFailure(ctx, first.GetHello(), rt, err)
			if errors.Is(err, ErrRuntimeAlreadyEnded) {
				return AuthenticatedRuntime{}, "", time.Time{}, status.Error(codes.FailedPrecondition, "runtime already ended")
			}
			if errors.Is(err, repository.ErrConflict) {
				return AuthenticatedRuntime{}, "", time.Time{}, runtimeRegistrationConflictToStatus(err)
			}
			return AuthenticatedRuntime{}, "", time.Time{}, status.Errorf(codes.Unavailable, "register runtime: %v", err)
		}
		token, expiresAt, err := s.issueRuntimeChannelLease(ctx, rt)
		if err != nil {
			return AuthenticatedRuntime{}, "", time.Time{}, status.Errorf(codes.Unavailable, "issue runtime channel lease: %v", err)
		}
		return rt, token, expiresAt, nil
	case cpv1.FrameType_FRAME_TYPE_RESUME:
		if first.GetResume() == nil {
			return AuthenticatedRuntime{}, "", time.Time{}, status.Error(codes.InvalidArgument, "RESUME frame missing payload")
		}
		rt, token, expiresAt, err := s.verifyResume(ctx, first.GetResume())
		if err != nil {
			s.recordResumeFailure(ctx, first.GetResume(), err)
			return AuthenticatedRuntime{}, "", time.Time{}, err
		}
		return rt, token, expiresAt, nil
	default:
		return AuthenticatedRuntime{}, "", time.Time{}, status.Error(codes.InvalidArgument, "first RuntimeChannel frame must be HELLO or RESUME")
	}
}

func (s *Service) recordConnectionOwner(ctx context.Context, runtimeID string, at time.Time) error {
	if s.instanceID == "" {
		return nil
	}
	return s.repo.RecordRuntimeConnectionOwner(ctx, runtimeID, s.instanceID, at.UTC())
}

func runtimeRegistrationConflictToStatus(err error) error {
	msg := err.Error()
	if strings.Contains(msg, "credential already bound") {
		return status.Error(codes.PermissionDenied, "runtime credential is already bound to another runtime; cancel the existing runtime or generate a new credential")
	}
	return status.Errorf(codes.FailedPrecondition, "runtime registration conflict: %v", err)
}

func (s *Service) issueRuntimeChannelLease(ctx context.Context, rt AuthenticatedRuntime) (string, time.Time, error) {
	token, err := newRuntimeChannelResumeToken()
	if err != nil {
		return "", time.Time{}, err
	}
	now := s.now().UTC()
	expiresAt := now.Add(runtimeChannelLeaseTTL)
	if err := s.repo.CreateRuntimeChannelLease(ctx, domain.RuntimeChannelLease{
		RuntimeID:       rt.RuntimeID,
		UserID:          rt.UserID,
		CredentialKeyID: rt.KeyID,
		LeaseHash:       hashRuntimeChannelToken(token),
		IssuedAt:        now,
		ExpiresAt:       expiresAt,
		CreatedAt:       now,
		UpdatedAt:       now,
	}); err != nil {
		return "", time.Time{}, err
	}
	return token, expiresAt, nil
}

func (s *Service) verifyResume(ctx context.Context, resume *cpv1.RuntimeResume) (AuthenticatedRuntime, string, time.Time, error) {
	runtimeID := strings.TrimSpace(resume.GetRuntimeId())
	token := strings.TrimSpace(resume.GetFingerprint())
	if token == "" {
		token = strings.TrimSpace(resume.GetResumeToken())
	}
	if runtimeID == "" || token == "" {
		return AuthenticatedRuntime{}, "", time.Time{}, status.Error(codes.InvalidArgument, "RESUME requires runtime_id and fingerprint")
	}
	leaseHash := hashRuntimeChannelToken(token)
	lease, err := s.repo.GetRuntimeChannelLeaseByHash(ctx, leaseHash)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return AuthenticatedRuntime{}, "", time.Time{}, status.Error(codes.PermissionDenied, "runtime resume token is invalid")
		}
		return AuthenticatedRuntime{}, "", time.Time{}, status.Errorf(codes.Unavailable, "lookup runtime resume token: %v", err)
	}
	now := s.now().UTC()
	if lease.RuntimeID != runtimeID {
		return AuthenticatedRuntime{}, "", time.Time{}, status.Error(codes.PermissionDenied, "runtime resume token does not match runtime_id")
	}
	if !lease.ExpiresAt.After(now) || lease.RevokedAt != nil {
		return AuthenticatedRuntime{}, "", time.Time{}, status.Error(codes.PermissionDenied, "runtime fingerprint expired")
	}
	rt, err := s.repo.GetRuntime(ctx, runtimeID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return AuthenticatedRuntime{}, "", time.Time{}, status.Error(codes.NotFound, "runtime not found")
		}
		return AuthenticatedRuntime{}, "", time.Time{}, status.Errorf(codes.Unavailable, "lookup runtime: %v", err)
	}
	if rt.UserID != lease.UserID || rt.CredentialKeyID != lease.CredentialKeyID {
		return AuthenticatedRuntime{}, "", time.Time{}, status.Error(codes.PermissionDenied, "runtime fingerprint binding mismatch")
	}
	if domain.IsRuntimeTerminalStatus(rt.Status) {
		return AuthenticatedRuntime{}, "", time.Time{}, status.Error(codes.FailedPrecondition, "runtime already ended")
	}
	nextToken, expiresAt, err := s.rotateRuntimeFingerprintWithHash(ctx, runtimeID, leaseHash, now)
	if err != nil {
		return AuthenticatedRuntime{}, "", time.Time{}, err
	}
	return AuthenticatedRuntime{
		KeyID:           rt.CredentialKeyID,
		UserID:          rt.UserID,
		RuntimeID:       rt.RuntimeID,
		Name:            rt.Name,
		Source:          rt.Source,
		Role:            rt.Role,
		EndpointHost:    rt.EndpointHost,
		GRPCPort:        rt.GRPCPort,
		DebugPort:       rt.DebugPort,
		Capabilities:    append([]string(nil), rt.Capabilities...),
		ResourceProfile: rt.ResourceProfile,
		Version:         rt.Version,
		AuthenticatedAt: now,
	}, nextToken, expiresAt, nil
}

func (s *Service) rotateRuntimeFingerprint(ctx context.Context, runtimeID, previousFingerprint string) (string, time.Time, error) {
	previousFingerprint = strings.TrimSpace(previousFingerprint)
	if runtimeID == "" || previousFingerprint == "" {
		return "", time.Time{}, status.Error(codes.PermissionDenied, "runtime heartbeat fingerprint is required")
	}
	oldHash := hashRuntimeChannelToken(previousFingerprint)
	lease, err := s.repo.GetRuntimeChannelLeaseByHash(ctx, oldHash)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return "", time.Time{}, status.Error(codes.PermissionDenied, "runtime fingerprint is invalid")
		}
		return "", time.Time{}, status.Errorf(codes.Unavailable, "lookup runtime fingerprint: %v", err)
	}
	now := s.now().UTC()
	if lease.RuntimeID != runtimeID {
		return "", time.Time{}, status.Error(codes.PermissionDenied, "runtime fingerprint does not match runtime_id")
	}
	if !lease.ExpiresAt.After(now) || lease.RevokedAt != nil {
		return "", time.Time{}, status.Error(codes.PermissionDenied, "runtime fingerprint expired")
	}
	rt, err := s.repo.GetRuntime(ctx, runtimeID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return "", time.Time{}, status.Error(codes.NotFound, "runtime not found")
		}
		return "", time.Time{}, status.Errorf(codes.Unavailable, "lookup runtime: %v", err)
	}
	if rt.UserID != lease.UserID || rt.CredentialKeyID != lease.CredentialKeyID {
		return "", time.Time{}, status.Error(codes.PermissionDenied, "runtime fingerprint binding mismatch")
	}
	if domain.IsRuntimeTerminalStatus(rt.Status) {
		return "", time.Time{}, status.Error(codes.FailedPrecondition, "runtime already ended")
	}
	return s.rotateRuntimeFingerprintWithHash(ctx, runtimeID, oldHash, now)
}

func (s *Service) rotateRuntimeFingerprintWithHash(ctx context.Context, runtimeID, oldHash string, now time.Time) (string, time.Time, error) {
	nextToken, err := newRuntimeChannelResumeToken()
	if err != nil {
		return "", time.Time{}, status.Errorf(codes.Unavailable, "generate runtime fingerprint: %v", err)
	}
	expiresAt := now.UTC().Add(runtimeChannelLeaseTTL)
	if err := s.repo.RotateRuntimeChannelLease(ctx, runtimeID, oldHash, hashRuntimeChannelToken(nextToken), expiresAt, now.UTC()); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return "", time.Time{}, status.Error(codes.PermissionDenied, "runtime fingerprint is invalid")
		}
		return "", time.Time{}, status.Errorf(codes.Unavailable, "rotate runtime fingerprint: %v", err)
	}
	return nextToken, expiresAt, nil
}

func newRuntimeChannelResumeToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

func hashRuntimeChannelToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func (s *Service) recordAdmissionFailure(ctx context.Context, hello *cpv1.RuntimeHello, rt AuthenticatedRuntime, cause error) {
	if s.repo == nil || hello == nil || cause == nil {
		return
	}
	failure := domain.RuntimeAdmissionFailure{
		UserID:             rt.UserID,
		CredentialKeyID:    hello.GetKeyId(),
		RequestedRuntimeID: hello.GetRuntimeId(),
		RequestedName:      strings.TrimSpace(hello.GetName()),
		Source:             rt.Source,
		Role:               rt.Role,
		FailureCode:        admissionFailureCode(cause),
		Reason:             cause.Error(),
		FirstSeenAt:        s.now().UTC(),
		LastSeenAt:         s.now().UTC(),
		AttemptCount:       1,
	}
	if failure.CredentialKeyID != "" {
		if cred, err := s.repo.GetRuntimeCredential(ctx, failure.CredentialKeyID); err == nil {
			if failure.UserID == 0 {
				failure.UserID = cred.UserID
			}
			if failure.Role == "" {
				failure.Role = cred.Role
			}
			if failure.Source == "" {
				if cred.HostedInternal {
					failure.Source = domain.RuntimeSourceHosted
				} else {
					failure.Source = domain.RuntimeSourceSelfHosted
				}
			}
			failure.ConsumedRuntimeID = cred.ConsumedRuntimeID
		}
	}
	_ = s.repo.RecordRuntimeAdmissionFailure(ctx, failure)
}

func (s *Service) recordResumeFailure(ctx context.Context, resume *cpv1.RuntimeResume, cause error) {
	if s.repo == nil || resume == nil || cause == nil {
		return
	}
	runtimeID := strings.TrimSpace(resume.GetRuntimeId())
	failure := domain.RuntimeAdmissionFailure{
		RequestedRuntimeID: runtimeID,
		FailureCode:        admissionFailureCode(cause),
		Reason:             cause.Error(),
		FirstSeenAt:        s.now().UTC(),
		LastSeenAt:         s.now().UTC(),
		AttemptCount:       1,
	}
	if runtimeID != "" {
		if rt, err := s.repo.GetRuntime(ctx, runtimeID); err == nil {
			failure.UserID = rt.UserID
			failure.CredentialKeyID = rt.CredentialKeyID
			failure.RequestedName = rt.Name
			failure.Source = rt.Source
			failure.Role = rt.Role
			if domain.IsRuntimeTerminalStatus(rt.Status) {
				failure.ConsumedRuntimeID = rt.RuntimeID
			}
		}
	}
	_ = s.repo.RecordRuntimeAdmissionFailure(ctx, failure)
}

func admissionFailureCode(err error) string {
	switch {
	case errors.Is(err, ErrInvalidHello):
		return "invalid_hello"
	case errors.Is(err, ErrPermissionDenied):
		return "permission_denied"
	case errors.Is(err, ErrReplay):
		return "replay"
	case errors.Is(err, repository.ErrConflict):
		return "failed_precondition"
	case status.Code(err) == codes.PermissionDenied:
		return "permission_denied"
	case status.Code(err) == codes.FailedPrecondition:
		return "failed_precondition"
	case status.Code(err) == codes.InvalidArgument:
		return "invalid_hello"
	case status.Code(err) == codes.NotFound:
		return "not_found"
	default:
		return "unavailable"
	}
}

func (s *Service) upsertRuntime(ctx context.Context, rt AuthenticatedRuntime) error {
	now := rt.AuthenticatedAt.UTC()
	paired := now
	record := domain.Runtime{
		RuntimeID:       rt.RuntimeID,
		CredentialKeyID: rt.KeyID,
		UserID:          rt.UserID,
		Name:            rt.Name,
		Source:          rt.Source,
		Role:            rt.Role,
		EndpointHost:    rt.EndpointHost,
		GRPCPort:        rt.GRPCPort,
		DebugPort:       rt.DebugPort,
		Capabilities:    rt.Capabilities,
		ResourceProfile: rt.ResourceProfile,
		Version:         rt.Version,
		Status:          domain.RuntimeStatusActive,
		PairedAt:        &paired,
		StartedAt:       &now,
		HeartbeatAt:     &now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	var err error
	switch rt.Source {
	case domain.RuntimeSourceHosted:
		err = s.repo.CreateOrReplaceHostedRuntime(ctx, record)
	case domain.RuntimeSourceSelfHosted:
		err = s.repo.CreateOrReplaceSelfHostedRuntime(ctx, record)
	default:
		return errors.New("unsupported runtime source")
	}
	if err == nil {
		return nil
	}
	if errors.Is(err, repository.ErrConflict) {
		existing, getErr := s.repo.GetRuntime(ctx, rt.RuntimeID)
		if getErr == nil && domain.IsRuntimeTerminalStatus(existing.Status) {
			return ErrRuntimeAlreadyEnded
		}
	}
	return err
}

func (s *Service) recordHeartbeat(ctx context.Context, runtimeID string, at time.Time) error {
	if err := s.repo.UpdateRuntimeHeartbeat(ctx, runtimeID, at); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			rt, getErr := s.repo.GetRuntime(ctx, runtimeID)
			if getErr == nil && domain.IsRuntimeTerminalStatus(rt.Status) {
				return ErrRuntimeAlreadyEnded
			}
			if getErr != nil {
				return err
			}
		}
		return err
	}
	return nil
}

func validatePostHelloFrame(frame *cpv1.RuntimeFrame) error {
	if frame == nil {
		return status.Error(codes.InvalidArgument, "runtime frame is required")
	}
	switch frame.GetFrameType() {
	case cpv1.FrameType_FRAME_TYPE_HEARTBEAT:
		if frame.GetHeartbeat() == nil {
			return status.Error(codes.InvalidArgument, "HEARTBEAT frame missing payload")
		}
	case cpv1.FrameType_FRAME_TYPE_RESPONSE:
		if frame.GetCorrelationId() == "" || frame.GetResponse() == nil {
			return status.Error(codes.InvalidArgument, "RESPONSE frame requires correlation_id and payload")
		}
	case cpv1.FrameType_FRAME_TYPE_PROGRESS:
		if frame.GetCorrelationId() == "" || frame.GetProgress() == nil {
			return status.Error(codes.InvalidArgument, "PROGRESS frame requires correlation_id and payload")
		}
	case cpv1.FrameType_FRAME_TYPE_ERROR:
		if frame.GetCorrelationId() == "" || frame.GetError() == nil {
			return status.Error(codes.InvalidArgument, "ERROR frame requires correlation_id and payload")
		}
	case cpv1.FrameType_FRAME_TYPE_REQUEST:
		if frame.GetCorrelationId() == "" || frame.GetRequest() == nil || frame.GetRequest().GetRequest() == nil {
			return status.Error(codes.InvalidArgument, "REQUEST frame requires correlation_id and payload")
		}
	case cpv1.FrameType_FRAME_TYPE_COMMAND_ACK:
		if frame.GetCommandAck() == nil || frame.GetCommandAck().GetCommandId() == "" {
			return status.Error(codes.InvalidArgument, "COMMAND_ACK frame requires command_id")
		}
	case cpv1.FrameType_FRAME_TYPE_COMMAND_RESULT:
		if frame.GetCommandResult() == nil || frame.GetCommandResult().GetCommandId() == "" {
			return status.Error(codes.InvalidArgument, "COMMAND_RESULT frame requires command_id")
		}
	case cpv1.FrameType_FRAME_TYPE_STATUS_PATCH:
		if frame.GetStatusPatch() == nil || frame.GetStatusPatch().GetStatus() == "" {
			return status.Error(codes.InvalidArgument, "STATUS_PATCH frame requires status")
		}
	case cpv1.FrameType_FRAME_TYPE_DATA_ACK:
		if frame.GetDataAck() == nil || frame.GetDataAck().GetSessionId() == "" {
			return status.Error(codes.InvalidArgument, "DATA_ACK frame requires session_id")
		}
	case cpv1.FrameType_FRAME_TYPE_DATA_BACKPRESSURE:
		if frame.GetDataBackpressure() == nil || frame.GetDataBackpressure().GetSessionId() == "" {
			return status.Error(codes.InvalidArgument, "DATA_BACKPRESSURE frame requires session_id")
		}
	case cpv1.FrameType_FRAME_TYPE_DATA_END:
		if frame.GetDataEnd() == nil || frame.GetDataEnd().GetSessionId() == "" {
			return status.Error(codes.InvalidArgument, "DATA_END frame requires session_id")
		}
	case cpv1.FrameType_FRAME_TYPE_HELLO:
		return status.Error(codes.InvalidArgument, "HELLO is only valid as the first frame")
	case cpv1.FrameType_FRAME_TYPE_RESUME:
		return status.Error(codes.InvalidArgument, "RESUME is only valid as the first frame")
	default:
		return status.Errorf(codes.InvalidArgument, "unsupported runtime frame_type=%s", frame.GetFrameType().String())
	}
	return nil
}

func helloErrorToStatus(err error) error {
	switch {
	case errors.Is(err, ErrInvalidHello):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrPermissionDenied), errors.Is(err, ErrReplay):
		return status.Error(codes.PermissionDenied, err.Error())
	default:
		return status.Error(codes.Unavailable, err.Error())
	}
}
