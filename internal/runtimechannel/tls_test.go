package runtimechannel

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestServerTLSCredentialsDisabled(t *testing.T) {
	creds, err := ServerTLSCredentials(ServerTLSConfig{Enabled: false})
	if err != nil {
		t.Fatalf("ServerTLSCredentials: %v", err)
	}
	if creds != nil {
		t.Fatal("creds = non-nil, want nil when tls is disabled")
	}
}

func TestServerTLSCredentialsRequiresCertAndKey(t *testing.T) {
	_, err := ServerTLSCredentials(ServerTLSConfig{Enabled: true})
	if err == nil || !strings.Contains(err.Error(), "cert_file is required") {
		t.Fatalf("err = %v, want cert_file required", err)
	}
}

func TestServerTLSCredentialsLoadsKeyPair(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestCertificate(t, dir)

	creds, err := ServerTLSCredentials(ServerTLSConfig{
		Enabled:  true,
		CertFile: certFile,
		KeyFile:  keyFile,
	})
	if err != nil {
		t.Fatalf("ServerTLSCredentials: %v", err)
	}
	if creds == nil {
		t.Fatal("creds = nil, want server tls credentials")
	}
}

func TestServerTLSCredentialsRejectsInvalidClientCA(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestCertificate(t, dir)
	caFile := filepath.Join(dir, "client-ca.pem")
	if err := os.WriteFile(caFile, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("write client ca: %v", err)
	}

	_, err := ServerTLSCredentials(ServerTLSConfig{
		Enabled:      true,
		CertFile:     certFile,
		KeyFile:      keyFile,
		ClientCAFile: caFile,
	})
	if err == nil || !strings.Contains(err.Error(), "no valid certificates") {
		t.Fatalf("err = %v, want invalid client ca rejection", err)
	}
}

func TestServerTLSCredentialsRequiresClientCertificateWhenClientCAConfigured(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeTestCertificate(t, dir)

	creds, err := ServerTLSCredentials(ServerTLSConfig{
		Enabled:      true,
		CertFile:     certFile,
		KeyFile:      keyFile,
		ClientCAFile: certFile,
	})
	if err != nil {
		t.Fatalf("ServerTLSCredentials: %v", err)
	}

	serverConn, clientConn := net.Pipe()
	serverErr := make(chan error, 1)
	go func() {
		_, _, err := creds.ServerHandshake(serverConn)
		serverErr <- err
	}()

	certPEM, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatalf("read cert: %v", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("server cert did not parse as root")
	}
	client := tls.Client(clientConn, &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: "localhost",
	})
	_ = client.Handshake()
	_ = client.Close()

	select {
	case err := <-serverErr:
		if err == nil {
			t.Fatal("server handshake succeeded without client certificate")
		}
	case <-time.After(time.Second):
		t.Fatal("server handshake did not finish")
	}
}

func writeTestCertificate(t *testing.T, dir string) (string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	keyDER := x509.MarshalPKCS1PrivateKey(key)
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certFile, keyFile
}
