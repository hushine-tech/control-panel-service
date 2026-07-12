package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hushine-tech/control-panel-service/internal/config"
	"github.com/hushine-tech/control-panel-service/internal/runtimecert"
)

func TestRuntimeClientCertSignerFromConfigLoadsCA(t *testing.T) {
	certFile, keyFile := writeRuntimeClientCAForTest(t)

	signer, err := runtimeClientCertSignerFromConfig(config.RuntimeChannelServerTLSConfig{
		ClientCAFile:    certFile,
		ClientCAKeyFile: keyFile,
	})
	if err != nil {
		t.Fatalf("runtimeClientCertSignerFromConfig: %v", err)
	}
	if signer == nil {
		t.Fatal("signer is nil")
	}
	csrPEM := mustRuntimeCSRForMainTest(t, "rt-main-test")
	_, _, err = signer.SignRuntimeClientCertificate(runtimecert.SignRequest{
		CSRPEM:    csrPEM,
		RuntimeID: "rt-main-test",
		UserID:    42,
		Source:    "hosted",
		Role:      "runner",
		Name:      "hosted-main-test",
		TTL:       time.Hour,
		Now:       time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("SignRuntimeClientCertificate: %v", err)
	}
}

func TestRuntimeClientCertSignerFromConfigRequiresKeyPair(t *testing.T) {
	certFile, _ := writeRuntimeClientCAForTest(t)

	_, err := runtimeClientCertSignerFromConfig(config.RuntimeChannelServerTLSConfig{
		ClientCAFile: certFile,
	})
	if err == nil {
		t.Fatal("expected missing key file error")
	}
	if !strings.Contains(err.Error(), "client_ca_key_file") {
		t.Fatalf("err = %v, want client_ca_key_file", err)
	}
}

func TestRuntimeServerCAPEMFromConfigReadsServerCertFile(t *testing.T) {
	dir := t.TempDir()
	serverCertFile := filepath.Join(dir, "runtime-channel-server.pem")
	if err := os.WriteFile(serverCertFile, []byte("runtime-channel-server-ca"), 0o644); err != nil {
		t.Fatalf("write server cert: %v", err)
	}

	pem, err := runtimeServerCAPEMFromConfig(config.RuntimeChannelServerTLSConfig{CertFile: serverCertFile})
	if err != nil {
		t.Fatalf("runtimeServerCAPEMFromConfig: %v", err)
	}
	if string(pem) != "runtime-channel-server-ca" {
		t.Fatalf("server ca = %q", string(pem))
	}
}

func TestRuntimeCoverageStartupLogReportsEffectiveSettings(t *testing.T) {
	got := runtimeCoverageStartupLog(config.DockerCoverageConfig{
		Enabled:            true,
		Image:              "hushine/strategy-runtime:executor-coverage",
		OutputDir:          "/tmp/census/manual-1/coverage/runtime-agent",
		StopTimeoutSeconds: 10,
	})
	for _, want := range []string{
		"runtime coverage: enabled",
		"image=hushine/strategy-runtime:executor-coverage",
		"output_dir=/tmp/census/manual-1/coverage/runtime-agent",
		"stop_timeout_seconds=10",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("runtimeCoverageStartupLog() = %q, want %q", got, want)
		}
	}
}

func TestRuntimeCoverageStartupLogDisabledIsEmpty(t *testing.T) {
	if got := runtimeCoverageStartupLog(config.DockerCoverageConfig{}); got != "" {
		t.Fatalf("runtimeCoverageStartupLog() = %q, want empty", got)
	}
}

func writeRuntimeClientCAForTest(t *testing.T) (string, string) {
	t.Helper()
	certPEM, keyPEM := mustCAForMainTest(t)
	dir := t.TempDir()
	certFile := filepath.Join(dir, "runtime-client-ca.pem")
	keyFile := filepath.Join(dir, "runtime-client-ca.key")
	if err := os.WriteFile(certFile, certPEM, 0o644); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certFile, keyFile
}

func mustRuntimeCSRForMainTest(t *testing.T, commonName string) []byte {
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
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})
}

func mustCAForMainTest(t *testing.T) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate ca key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("serial: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
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
