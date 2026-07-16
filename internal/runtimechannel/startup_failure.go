package runtimechannel

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"regexp"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	"github.com/hushine-tech/control-panel-service/internal/domain"
	"github.com/hushine-tech/control-panel-service/internal/repository"
	"github.com/hushine-tech/control-panel-service/internal/runtimecert"
)

const (
	runtimeDependencyProfileInvalidCode = "RUNTIME_DEPENDENCY_PROFILE_INVALID"
	startupFailureSignatureDomain       = "runtime-startup-failure-v1\n"
	maxStartupFailureIdentifierLength   = 128
)

var (
	startupFailureNonceRE      = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	startupFailureModuleRE     = regexp.MustCompile(`^(?:[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*|[A-Za-z0-9][A-Za-z0-9._-]{0,127})$`)
	safeStartupFailureMessages = map[string]struct{}{
		"worker Python invocation is invalid":                               {},
		"embedded runtime profile is invalid":                               {},
		"runtime dependency startup probe failed":                           {},
		"runtime dependency startup probe returned an invalid response":     {},
		"runtime dependency startup probe did not match the sealed profile": {},
	}
)

func canonicalRuntimeStartupFailurePayload(request *cpv1.ReportRuntimeStartupFailureRequest) []byte {
	profile := request.GetActualProfile()
	roots := append([]string(nil), profile.GetPublicImportRoots()...)
	sort.Strings(roots)
	errorFact := request.GetDependencyError()
	value := map[string]any{
		"dependency_code":                    errorFact.GetCode(),
		"dependency_contract_sha256":         profile.GetContractSha256(),
		"dependency_error_image_build_id":    errorFact.GetImageBuildId(),
		"dependency_error_profile_name":      errorFact.GetRuntimeProfile(),
		"dependency_error_profile_version":   errorFact.GetRuntimeProfileVersion(),
		"dependency_hosted_python":           profile.GetHostedPython(),
		"dependency_image_build_id":          profile.GetImageBuildId(),
		"dependency_message":                 errorFact.GetMessage(),
		"dependency_module":                  errorFact.GetModule(),
		"dependency_profile_name":            profile.GetProfileName(),
		"dependency_profile_version":         profile.GetProfileVersion(),
		"dependency_public_import_roots":     roots,
		"dependency_schema_version":          profile.GetSchemaVersion(),
		"dependency_strategy_library_commit": profile.GetStrategyLibraryCommit(),
		"dependency_strategy_service_commit": profile.GetStrategyServiceCommit(),
		"issued_at_unix_ms":                  request.GetIssuedAtUnixMs(),
		"key_id":                             request.GetKeyId(),
		"nonce":                              request.GetNonce(),
		"runtime_id":                         request.GetRuntimeId(),
		"source":                             request.GetSource(),
	}
	body, _ := json.Marshal(value)
	return append([]byte(startupFailureSignatureDomain), body...)
}

func (s *Service) ReportRuntimeStartupFailure(ctx context.Context, request *cpv1.ReportRuntimeStartupFailureRequest) (*cpv1.ReportRuntimeStartupFailureResponse, error) {
	if s == nil || s.repo == nil {
		return nil, status.Error(codes.FailedPrecondition, "runtime channel service is not configured")
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "startup failure request is required")
	}
	if !startupFailureIdentifierIsSafe(request.GetKeyId()) || !startupFailureIdentifierIsSafe(request.GetRuntimeId()) {
		return nil, status.Error(codes.InvalidArgument, "startup failure identity is invalid")
	}
	if request.GetSource() != domain.RuntimeSourceSelfHosted {
		return nil, status.Error(codes.PermissionDenied, "startup failure source is not authorized")
	}
	if !startupFailureNonceRE.MatchString(request.GetNonce()) {
		return nil, status.Error(codes.InvalidArgument, "startup failure nonce is invalid")
	}
	now := s.now().UTC()
	issuedAt := time.UnixMilli(request.GetIssuedAtUnixMs()).UTC()
	if request.GetIssuedAtUnixMs() <= 0 || now.Sub(issuedAt) > maxHelloClockSkew || issuedAt.Sub(now) > maxHelloClockSkew {
		return nil, status.Error(codes.PermissionDenied, "startup failure timestamp is outside the allowed window")
	}
	if err := validateStartupDependencyFacts(request); err != nil {
		return nil, status.Error(codes.InvalidArgument, "startup dependency failure facts are invalid")
	}

	credential, err := s.repo.GetRuntimeCredential(ctx, request.GetKeyId())
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Error(codes.PermissionDenied, "startup failure credential is not authorized")
		}
		return nil, status.Error(codes.Unavailable, "startup failure credential lookup failed")
	}
	if !startupFailureCredentialIsUsable(credential, request.GetRuntimeId(), now) {
		return nil, status.Error(codes.PermissionDenied, "startup failure credential is not authorized")
	}
	if err := s.validateStartupFailureOwner(ctx, credential, request); err != nil {
		return nil, err
	}
	if err := validateStartupFailurePeer(ctx, credential, request); err != nil {
		return nil, err
	}

	publicKey, err := parseEd25519PublicKeyPEM(credential.PublicKeyPEM)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, "startup failure credential is not authorized")
	}
	signature, err := base64.RawURLEncoding.DecodeString(request.GetSignature())
	if err != nil || len(signature) != ed25519.SignatureSize ||
		!ed25519.Verify(publicKey, canonicalRuntimeStartupFailurePayload(request), signature) {
		return nil, status.Error(codes.PermissionDenied, "startup failure signature is invalid")
	}
	if s.replay != nil && !s.replay.CheckAndStore("startup:"+request.GetKeyId(), request.GetNonce(), now) {
		return nil, status.Error(codes.PermissionDenied, "startup failure report was already received")
	}

	reason := request.GetDependencyError().GetMessage()
	if module := request.GetDependencyError().GetModule(); module != "" {
		reason = module + ": " + reason
	}
	profile := request.GetActualProfile()
	reason = reason + "; profile=" + profile.GetProfileName() +
		" version=" + profile.GetProfileVersion() + " image_build_id=" + profile.GetImageBuildId()
	failure := domain.RuntimeAdmissionFailure{
		UserID:             credential.UserID,
		CredentialKeyID:    credential.KeyID,
		RequestedRuntimeID: request.GetRuntimeId(),
		Source:             domain.RuntimeSourceSelfHosted,
		Role:               credential.Role,
		FailureCode:        runtimeDependencyProfileInvalidCode,
		Reason:             reason,
		ConsumedRuntimeID:  credential.ConsumedRuntimeID,
		FirstSeenAt:        now,
		LastSeenAt:         now,
		AttemptCount:       1,
	}
	if runtime, err := s.repo.GetRuntime(ctx, request.GetRuntimeId()); err == nil {
		failure.RequestedName = runtime.Name
	}
	if err := s.repo.RecordRuntimeAdmissionFailure(ctx, failure); err != nil {
		return nil, status.Error(codes.Unavailable, "record runtime startup failure")
	}
	return &cpv1.ReportRuntimeStartupFailureResponse{Recorded: true}, nil
}

func startupFailureIdentifierIsSafe(value string) bool {
	return value != "" && len(value) <= maxStartupFailureIdentifierLength && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\r\n\x00")
}

func validateStartupDependencyFacts(request *cpv1.ReportRuntimeStartupFailureRequest) error {
	if err := validateDependencyProfileStructure(request.GetActualProfile()); err != nil {
		return err
	}
	errorFact := request.GetDependencyError()
	profile := request.GetActualProfile()
	if errorFact == nil || errorFact.GetCode() != runtimeDependencyProfileInvalidCode {
		return errors.New("unsupported dependency error code")
	}
	if errorFact.GetModule() == "" || len(errorFact.GetModule()) > 128 || !startupFailureModuleRE.MatchString(errorFact.GetModule()) {
		return errors.New("invalid dependency module")
	}
	if _, ok := safeStartupFailureMessages[errorFact.GetMessage()]; !ok {
		return errors.New("invalid dependency error message")
	}
	if errorFact.GetRuntimeProfile() != profile.GetProfileName() ||
		errorFact.GetRuntimeProfileVersion() != profile.GetProfileVersion() ||
		errorFact.GetImageBuildId() != profile.GetImageBuildId() {
		return errors.New("dependency error profile facts do not match actual profile")
	}
	return nil
}

func startupFailureCredentialIsUsable(credential domain.RuntimeCredential, runtimeID string, now time.Time) bool {
	if credential.KeyID == "" || credential.UserID <= 0 || credential.HostedInternal {
		return false
	}
	if credential.ExpiresAt != nil && !credential.ExpiresAt.After(now) {
		return false
	}
	switch credential.Status {
	case domain.CredentialStatusActive, domain.CredentialStatusDownloaded:
		return true
	case domain.CredentialStatusConsumed:
		return credential.ConsumedRuntimeID != "" && credential.ConsumedRuntimeID == runtimeID
	default:
		return false
	}
}

func (s *Service) validateStartupFailureOwner(ctx context.Context, credential domain.RuntimeCredential, request *cpv1.ReportRuntimeStartupFailureRequest) error {
	runtime, err := s.repo.GetRuntime(ctx, request.GetRuntimeId())
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil
		}
		return status.Error(codes.Unavailable, "startup failure owner lookup failed")
	}
	if runtime.UserID != credential.UserID || (runtime.CredentialKeyID != "" && runtime.CredentialKeyID != credential.KeyID) {
		return status.Error(codes.PermissionDenied, "startup failure runtime ownership mismatch")
	}
	return nil
}

func validateStartupFailurePeer(ctx context.Context, credential domain.RuntimeCredential, request *cpv1.ReportRuntimeStartupFailureRequest) error {
	identity, present, err := runtimecert.IdentityFromGRPCContext(ctx)
	if err != nil {
		return status.Error(codes.PermissionDenied, "startup failure client certificate identity is invalid")
	}
	if !present {
		return nil
	}
	if identity.RuntimeID != request.GetRuntimeId() || identity.UserID != credential.UserID ||
		(identity.Source != "" && identity.Source != domain.RuntimeSourceSelfHosted) {
		return status.Error(codes.PermissionDenied, "startup failure client certificate identity mismatch")
	}
	return nil
}
