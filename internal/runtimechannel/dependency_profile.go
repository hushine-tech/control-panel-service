package runtimechannel

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
)

var ErrDependencyProfileMismatch = errors.New("runtime dependency profile mismatch")

var (
	dependencyContractSHA256RE = regexp.MustCompile(`^[0-9a-f]{64}$`)
	dependencyProfileFactRE    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$`)
	dependencyImportRootRE     = regexp.MustCompile(`^(?:[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*|[A-Za-z0-9][A-Za-z0-9._-]{0,127})$`)
)

type ExpectedDependencyProfile struct {
	SchemaVersion  uint32
	Name           string
	Version        string
	ContractSHA256 string
}

func defaultExpectedDependencyProfile() ExpectedDependencyProfile {
	return ExpectedDependencyProfile{
		SchemaVersion:  1,
		Name:           "platform-python-3.13",
		Version:        "1.0.0",
		ContractSHA256: "8457b3c35618558fc8bfc74d4135b7eb52e00c33a8c9a49d202830f3fd5b62c5",
	}
}

func (p ExpectedDependencyProfile) configured() bool {
	return p.SchemaVersion != 0 || p.Name != "" || p.Version != "" || p.ContractSHA256 != ""
}

func validateDependencyAdmission(expected ExpectedDependencyProfile, actual *strategyv1.RuntimeDependencyProfile) error {
	if err := validateDependencyProfileStructure(actual); err != nil {
		return err
	}
	if !expected.configured() {
		return nil
	}
	if expected.SchemaVersion == 0 || !dependencyProfileFactRE.MatchString(expected.Name) ||
		!dependencyProfileFactRE.MatchString(expected.Version) ||
		!dependencyContractSHA256RE.MatchString(expected.ContractSHA256) {
		return fmt.Errorf("%w: expected profile configuration is invalid", ErrDependencyProfileMismatch)
	}
	if actual.GetSchemaVersion() != expected.SchemaVersion ||
		actual.GetProfileName() != expected.Name ||
		actual.GetProfileVersion() != expected.Version ||
		actual.GetContractSha256() != expected.ContractSHA256 {
		return fmt.Errorf(
			"%w: expected schema=%d name=%q version=%q digest=%q; actual schema=%d name=%q version=%q digest=%q",
			ErrDependencyProfileMismatch,
			expected.SchemaVersion,
			expected.Name,
			expected.Version,
			expected.ContractSHA256,
			actual.GetSchemaVersion(),
			actual.GetProfileName(),
			actual.GetProfileVersion(),
			actual.GetContractSha256(),
		)
	}
	return nil
}

func validateDependencyProfileStructure(actual *strategyv1.RuntimeDependencyProfile) error {
	if actual == nil {
		return fmt.Errorf("%w: actual profile is missing", ErrDependencyProfileMismatch)
	}
	if actual.GetSchemaVersion() == 0 {
		return fmt.Errorf("%w: schema_version is required", ErrDependencyProfileMismatch)
	}
	if !dependencyProfileFactRE.MatchString(actual.GetProfileName()) {
		return fmt.Errorf("%w: profile_name is invalid", ErrDependencyProfileMismatch)
	}
	if !dependencyProfileFactRE.MatchString(actual.GetProfileVersion()) {
		return fmt.Errorf("%w: profile_version is invalid", ErrDependencyProfileMismatch)
	}
	if !dependencyContractSHA256RE.MatchString(actual.GetContractSha256()) {
		return fmt.Errorf("%w: contract_sha256 must be 64 lowercase hexadecimal characters", ErrDependencyProfileMismatch)
	}
	if !dependencyFactIsSafe(actual.GetHostedPython(), 1024) {
		return fmt.Errorf("%w: hosted_python is invalid", ErrDependencyProfileMismatch)
	}
	roots := actual.GetPublicImportRoots()
	if len(roots) == 0 || len(roots) > 512 {
		return fmt.Errorf("%w: public_import_roots is required", ErrDependencyProfileMismatch)
	}
	for i, root := range roots {
		if !dependencyImportRootRE.MatchString(root) {
			return fmt.Errorf("%w: public_import_roots contains an invalid value", ErrDependencyProfileMismatch)
		}
		if i > 0 && roots[i-1] >= root {
			return fmt.Errorf("%w: public_import_roots must be sorted and unique", ErrDependencyProfileMismatch)
		}
	}
	if !dependencyFactIsSafe(actual.GetStrategyServiceCommit(), 128) {
		return fmt.Errorf("%w: strategy_service_commit is required", ErrDependencyProfileMismatch)
	}
	if !dependencyFactIsSafe(actual.GetStrategyLibraryCommit(), 128) {
		return fmt.Errorf("%w: strategy_library_commit is required", ErrDependencyProfileMismatch)
	}
	if !dependencyFactIsSafe(actual.GetImageBuildId(), 256) {
		return fmt.Errorf("%w: image_build_id is required", ErrDependencyProfileMismatch)
	}
	return nil
}

func dependencyFactIsSafe(value string, maxLength int) bool {
	if value == "" || len(value) > maxLength || strings.TrimSpace(value) != value || !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func cloneDependencyProfile(profile *strategyv1.RuntimeDependencyProfile) *strategyv1.RuntimeDependencyProfile {
	if profile == nil {
		return nil
	}
	return &strategyv1.RuntimeDependencyProfile{
		SchemaVersion:         profile.GetSchemaVersion(),
		ProfileName:           profile.GetProfileName(),
		ProfileVersion:        profile.GetProfileVersion(),
		ContractSha256:        profile.GetContractSha256(),
		HostedPython:          profile.GetHostedPython(),
		PublicImportRoots:     append([]string(nil), profile.GetPublicImportRoots()...),
		StrategyServiceCommit: profile.GetStrategyServiceCommit(),
		StrategyLibraryCommit: profile.GetStrategyLibraryCommit(),
		ImageBuildId:          profile.GetImageBuildId(),
	}
}

func cloneAuthenticatedRuntime(runtime AuthenticatedRuntime) AuthenticatedRuntime {
	runtime.Capabilities = append([]string(nil), runtime.Capabilities...)
	runtime.DependencyProfile = cloneDependencyProfile(runtime.DependencyProfile)
	return runtime
}
