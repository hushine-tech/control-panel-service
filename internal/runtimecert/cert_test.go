package runtimecert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"
)

func TestSignRuntimeClientCertificateIncludesIdentity(t *testing.T) {
	caCertPEM, caKeyPEM := mustTestCA(t)
	csrPEM, _ := mustRuntimeCSR(t, "rt-1")
	signer, err := NewSignerFromPEM(caCertPEM, caKeyPEM)
	if err != nil {
		t.Fatalf("NewSignerFromPEM: %v", err)
	}

	certPEM, cert, err := signer.SignRuntimeClientCertificate(SignRequest{
		CSRPEM:    csrPEM,
		RuntimeID: "rt-1",
		UserID:    42,
		Source:    "bare",
		Role:      "debugger",
		Name:      "bare-debug",
		TTL:       time.Hour,
		Now:       time.Date(2026, 6, 21, 1, 2, 3, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("SignRuntimeClientCertificate: %v", err)
	}
	if len(certPEM) == 0 {
		t.Fatal("certificate PEM is empty")
	}
	id, err := IdentityFromCertificate(cert)
	if err != nil {
		t.Fatalf("IdentityFromCertificate: %v", err)
	}
	if id.RuntimeID != "rt-1" || id.UserID != 42 || id.Source != "bare" || id.Role != "debugger" {
		t.Fatalf("identity = %+v", id)
	}
}

func TestParseCSRRejectsMismatchedCommonName(t *testing.T) {
	caCertPEM, caKeyPEM := mustTestCA(t)
	csrPEM, _ := mustRuntimeCSR(t, "other-runtime")
	signer, err := NewSignerFromPEM(caCertPEM, caKeyPEM)
	if err != nil {
		t.Fatalf("NewSignerFromPEM: %v", err)
	}

	_, _, err = signer.SignRuntimeClientCertificate(SignRequest{
		CSRPEM:    csrPEM,
		RuntimeID: "rt-expected",
		UserID:    42,
		Source:    "bare",
		Role:      "debugger",
		Name:      "bare-debug",
		TTL:       time.Hour,
		Now:       time.Now().UTC(),
	})
	if err == nil {
		t.Fatal("expected mismatched runtime common name rejection")
	}
}

func mustRuntimeCSR(t *testing.T, commonName string) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate runtime key: %v", err)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: commonName},
	}, key)
	if err != nil {
		t.Fatalf("create csr: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func mustTestCA(t *testing.T) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serialForTest(t),
		Subject:               pkix.Name{CommonName: "hushine-runtime-client-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create ca: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal ca key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func serialForTest(t *testing.T) *big.Int {
	t.Helper()
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("serial: %v", err)
	}
	return serial
}
