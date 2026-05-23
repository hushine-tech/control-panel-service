package auth

import "testing"

func TestGenerateRuntimeIDUnique(t *testing.T) {
	a := GenerateRuntimeID()
	b := GenerateRuntimeID()
	if a == b {
		t.Fatalf("expected unique runtime ids, got %q twice", a)
	}
	if len(a) <= len("rt-") {
		t.Fatalf("runtime id too short: %q", a)
	}
}

func TestGenerateRegistrationTokenUnique(t *testing.T) {
	a := GenerateRegistrationToken()
	b := GenerateRegistrationToken()
	if a == b {
		t.Fatalf("expected unique tokens, got %q twice", a)
	}
}

func TestHashTokenDeterministic(t *testing.T) {
	const input = "hello-control-panel"
	if HashToken(input) != HashToken(input) {
		t.Fatalf("HashToken not deterministic")
	}
	if HashToken(input) == HashToken("hello-control-pane") {
		t.Fatalf("HashToken should differ for different inputs")
	}
}
