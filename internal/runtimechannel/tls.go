package runtimechannel

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc/credentials"
)

type ServerTLSConfig struct {
	Enabled      bool
	CertFile     string
	KeyFile      string
	ClientCAFile string
}

func ServerTLSCredentials(cfg ServerTLSConfig) (credentials.TransportCredentials, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	if strings.TrimSpace(cfg.CertFile) == "" {
		return nil, fmt.Errorf("runtime channel tls cert_file is required when tls is enabled")
	}
	if strings.TrimSpace(cfg.KeyFile) == "" {
		return nil, fmt.Errorf("runtime channel tls key_file is required when tls is enabled")
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load runtime channel tls keypair: %w", err)
	}
	tlsCfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	if strings.TrimSpace(cfg.ClientCAFile) != "" {
		caPEM, err := os.ReadFile(cfg.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("read runtime channel client ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("runtime channel client ca contains no valid certificates")
		}
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		tlsCfg.ClientCAs = pool
	}
	return credentials.NewTLS(tlsCfg), nil
}
