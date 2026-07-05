package runtimecert

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

var (
	oidRuntimeID = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 55555, 1, 1}
	oidUserID    = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 55555, 1, 2}
	oidSource    = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 55555, 1, 3}
	oidRole      = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 55555, 1, 4}
	oidName      = asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 55555, 1, 5}
)

// RuntimeIdentity is the authenticated identity embedded in a runtime client certificate.
type RuntimeIdentity struct {
	RuntimeID string
	UserID    int64
	Source    string
	Role      string
	Name      string
}

type SignRequest struct {
	CSRPEM    []byte
	RuntimeID string
	UserID    int64
	Source    string
	Role      string
	Name      string
	TTL       time.Duration
	Now       time.Time
}

type Signer struct {
	caCert *x509.Certificate
	caKey  any
	caPEM  []byte
}

func NewSignerFromPEM(caCertPEM, caKeyPEM []byte) (*Signer, error) {
	certBlock, _ := pem.Decode(caCertPEM)
	if certBlock == nil {
		return nil, fmt.Errorf("client ca certificate PEM is required")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse client ca certificate: %w", err)
	}
	if !cert.IsCA {
		return nil, fmt.Errorf("client ca certificate is not a CA")
	}
	key, err := parsePrivateKeyPEM(caKeyPEM)
	if err != nil {
		return nil, err
	}
	return &Signer{
		caCert: cert,
		caKey:  key,
		caPEM:  append([]byte(nil), caCertPEM...),
	}, nil
}

func (s *Signer) CAPEM() []byte {
	if s == nil {
		return nil
	}
	return append([]byte(nil), s.caPEM...)
}

func (s *Signer) SignRuntimeClientCertificate(req SignRequest) ([]byte, *x509.Certificate, error) {
	if s == nil || s.caCert == nil || s.caKey == nil {
		return nil, nil, fmt.Errorf("runtime certificate signer is not configured")
	}
	if strings.TrimSpace(req.RuntimeID) == "" {
		return nil, nil, fmt.Errorf("runtime_id is required")
	}
	if req.UserID <= 0 {
		return nil, nil, fmt.Errorf("user_id must be positive")
	}
	if strings.TrimSpace(req.Source) == "" {
		return nil, nil, fmt.Errorf("runtime source is required")
	}
	if strings.TrimSpace(req.Role) == "" {
		return nil, nil, fmt.Errorf("runtime role is required")
	}
	if req.TTL <= 0 {
		return nil, nil, fmt.Errorf("runtime client certificate ttl must be positive")
	}
	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}

	csr, err := parseCSR(req.CSRPEM)
	if err != nil {
		return nil, nil, err
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, nil, fmt.Errorf("runtime CSR signature invalid: %w", err)
	}
	if csr.Subject.CommonName != req.RuntimeID {
		return nil, nil, fmt.Errorf("runtime CSR common name %q does not match runtime_id %q", csr.Subject.CommonName, req.RuntimeID)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, fmt.Errorf("generate runtime client certificate serial: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: req.RuntimeID,
		},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(req.TTL),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		ExtraExtensions:       identityExtensions(req),
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, s.caCert, csr.PublicKey, s.caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("sign runtime client certificate: %w", err)
	}
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, nil, fmt.Errorf("parse signed runtime client certificate: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), cert, nil
}

func Fingerprint(cert *x509.Certificate) string {
	if cert == nil {
		return ""
	}
	sum := sha256.Sum256(cert.Raw)
	return hex.EncodeToString(sum[:])
}

func IdentityFromCertificate(cert *x509.Certificate) (RuntimeIdentity, error) {
	if cert == nil {
		return RuntimeIdentity{}, fmt.Errorf("runtime client certificate is required")
	}
	values := make(map[string]string, 5)
	for _, ext := range cert.Extensions {
		switch {
		case ext.Id.Equal(oidRuntimeID):
			values["runtime_id"] = extensionString(ext)
		case ext.Id.Equal(oidUserID):
			values["user_id"] = extensionString(ext)
		case ext.Id.Equal(oidSource):
			values["source"] = extensionString(ext)
		case ext.Id.Equal(oidRole):
			values["role"] = extensionString(ext)
		case ext.Id.Equal(oidName):
			values["name"] = extensionString(ext)
		}
	}
	userID, err := strconv.ParseInt(values["user_id"], 10, 64)
	if err != nil {
		return RuntimeIdentity{}, fmt.Errorf("runtime client certificate user_id invalid: %w", err)
	}
	id := RuntimeIdentity{
		RuntimeID: values["runtime_id"],
		UserID:    userID,
		Source:    values["source"],
		Role:      values["role"],
		Name:      values["name"],
	}
	if id.RuntimeID == "" || id.UserID <= 0 || id.Source == "" || id.Role == "" {
		return RuntimeIdentity{}, fmt.Errorf("runtime client certificate identity is incomplete")
	}
	return id, nil
}

func IdentityFromGRPCContext(ctx context.Context) (RuntimeIdentity, bool, error) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		return RuntimeIdentity{}, false, nil
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return RuntimeIdentity{}, false, nil
	}
	id, err := IdentityFromCertificate(tlsInfo.State.PeerCertificates[0])
	return id, true, err
}

func parsePrivateKeyPEM(keyPEM []byte) (any, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, fmt.Errorf("client ca private key PEM is required")
	}
	if key, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		switch key := key.(type) {
		case *ecdsa.PrivateKey, *rsa.PrivateKey, ed25519.PrivateKey:
			return key, nil
		default:
			return nil, fmt.Errorf("unsupported client ca private key type %T", key)
		}
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	return nil, fmt.Errorf("parse client ca private key")
}

func parseCSR(csrPEM []byte) (*x509.CertificateRequest, error) {
	block, _ := pem.Decode(csrPEM)
	if block == nil {
		return nil, fmt.Errorf("runtime CSR PEM is required")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse runtime CSR: %w", err)
	}
	return csr, nil
}

func identityExtensions(req SignRequest) []pkix.Extension {
	return []pkix.Extension{
		stringExtension(oidRuntimeID, req.RuntimeID),
		stringExtension(oidUserID, strconv.FormatInt(req.UserID, 10)),
		stringExtension(oidSource, req.Source),
		stringExtension(oidRole, req.Role),
		stringExtension(oidName, req.Name),
	}
}

func stringExtension(oid asn1.ObjectIdentifier, value string) pkix.Extension {
	encoded, _ := asn1.Marshal(value)
	return pkix.Extension{Id: oid, Value: encoded}
}

func extensionString(ext pkix.Extension) string {
	var value string
	if _, err := asn1.Unmarshal(ext.Value, &value); err == nil {
		return value
	}
	return string(ext.Value)
}
