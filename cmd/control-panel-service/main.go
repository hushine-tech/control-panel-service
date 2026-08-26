package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	grpcmw "github.com/hushine-tech/golang-lib/middleware/grpc"
	grpcclientmw "github.com/hushine-tech/golang-lib/middleware/grpcclient"
	httpmw "github.com/hushine-tech/golang-lib/middleware/httpserver"
	elog "github.com/hushine-tech/golang-lib/pkg/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	mdv1 "github.com/hushine-tech/control-panel-service/gen/marketdatav1"
	"github.com/hushine-tech/control-panel-service/internal/config"
	"github.com/hushine-tech/control-panel-service/internal/credential"
	"github.com/hushine-tech/control-panel-service/internal/debugger"
	"github.com/hushine-tech/control-panel-service/internal/httpserver"
	"github.com/hushine-tech/control-panel-service/internal/logger"
	"github.com/hushine-tech/control-panel-service/internal/marketdata"
	mdrepo "github.com/hushine-tech/control-panel-service/internal/marketdata/repository"
	cpnotify "github.com/hushine-tech/control-panel-service/internal/notification"
	"github.com/hushine-tech/control-panel-service/internal/plan"
	"github.com/hushine-tech/control-panel-service/internal/portfolioclient"
	"github.com/hushine-tech/control-panel-service/internal/provision"
	"github.com/hushine-tech/control-panel-service/internal/repository"
	"github.com/hushine-tech/control-panel-service/internal/runtime"
	"github.com/hushine-tech/control-panel-service/internal/runtimecert"
	"github.com/hushine-tech/control-panel-service/internal/runtimechannel"
	orderv1 "github.com/hushine-tech/core-service/gen/orderv1"
)

func fallbackString(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

// portFromBindAddr extracts the ":<port>" suffix from a Go net bind
// address (":50055", "0.0.0.0:50055", "127.0.0.1:50055"). Used to
// derive a reasonable RuntimeChannel dial address for runtime containers
// in host networking mode. Falls back to ":50055" if parsing is ambiguous.
func portFromBindAddr(bind string) string {
	if bind == "" {
		return ":50055"
	}
	// strings package usage kept minimal so we don't add an import:
	// find last ':' in string.
	i := -1
	for k := len(bind) - 1; k >= 0; k-- {
		if bind[k] == ':' {
			i = k
			break
		}
	}
	if i < 0 {
		return ":" + bind
	}
	return bind[i:]
}

func runtimeClientCertSignerFromConfig(tlsCfg config.RuntimeChannelServerTLSConfig) (*runtimecert.Signer, error) {
	certFile := strings.TrimSpace(tlsCfg.ClientCAFile)
	keyFile := strings.TrimSpace(tlsCfg.ClientCAKeyFile)
	if certFile == "" && keyFile == "" {
		return nil, nil
	}
	if certFile == "" {
		return nil, fmt.Errorf("runtime_channel_server.tls.client_ca_file is required when client_ca_key_file is set")
	}
	if keyFile == "" {
		return nil, fmt.Errorf("runtime_channel_server.tls.client_ca_key_file is required when client_ca_file is set")
	}
	certPEM, err := os.ReadFile(certFile)
	if err != nil {
		return nil, fmt.Errorf("read runtime channel client ca file: %w", err)
	}
	keyPEM, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("read runtime channel client ca key file: %w", err)
	}
	signer, err := runtimecert.NewSignerFromPEM(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("load runtime channel client ca signer: %w", err)
	}
	return signer, nil
}

func runtimeServerCAPEMFromConfig(tlsCfg config.RuntimeChannelServerTLSConfig) ([]byte, error) {
	certFile := strings.TrimSpace(tlsCfg.CertFile)
	if certFile == "" {
		return nil, nil
	}
	serverCAPEM, err := os.ReadFile(certFile)
	if err != nil {
		return nil, fmt.Errorf("read runtime channel server ca file: %w", err)
	}
	return serverCAPEM, nil
}

func expectedRuntimeDependencyProfile(profile config.RuntimeDependencyProfileConfig) runtimechannel.ExpectedDependencyProfile {
	return runtimechannel.ExpectedDependencyProfile{
		SchemaVersion:  profile.SchemaVersion,
		Name:           profile.Name,
		Version:        profile.Version,
		ContractSHA256: profile.ContractSHA256,
	}
}

func main() {
	configPath := flag.String("config", "config.yaml", "path to config.yaml")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Printf("config file %q not found; using built-in defaults + env overrides", *configPath)
			cfg = config.Default()
		} else {
			log.Fatalf("load config: %v", err)
		}
	}
	if err := cfg.ApplyEnvOverrides(); err != nil {
		log.Fatalf("validate config after environment overrides: %v", err)
	}

	// ── Logger ────────────────────────────────────────────────────────────────
	if err := logger.InitWithConfig(&cfg.Log); err != nil {
		log.Fatalf("init logger: %v", err)
	}
	defer logger.Close()

	if cfg.Log.Tracing.Enabled {
		if tracerShutdown, err := elog.InitTracerFromConfig(cfg.Log.Tracing); err != nil {
			log.Printf("init tracer: %v (continuing without tracing)", err)
		} else {
			defer tracerShutdown(context.Background())
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	logger.Info(ctx, "system", "control-panel-service starting")

	// ── TimescaleDB ───────────────────────────────────────────────────────────
	repo, err := repository.NewTimescaleRepository(cfg.Database.DSN(), logger.Instance())
	if err != nil {
		log.Fatalf("init timescaledb: %v", err)
	}
	defer repo.Close()
	logger.Info(ctx, "system", "timescaledb connected (control_panel)")

	// ── core-service client (for plan_code lookup) ───────────────────────────
	accClient, err := portfolioclient.New(
		cfg.Dependencies.PortfolioServiceGRPC,
		grpc.WithUnaryInterceptor(grpcclientmw.UnaryClientInterceptor(logger.Instance())),
	)
	if err != nil {
		log.Fatalf("init core-service client: %v", err)
	}
	defer accClient.Close()
	logger.Info(ctx, "system", fmt.Sprintf("core-service client → %s", cfg.Dependencies.PortfolioServiceGRPC))

	var orderClient orderv1.OrderServiceClient
	var orderConn *grpc.ClientConn
	if cfg.Dependencies.OrderServiceGRPC != "" {
		orderConn, err = grpc.NewClient(
			cfg.Dependencies.OrderServiceGRPC,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(grpcclientmw.UnaryClientInterceptor(logger.Instance())),
		)
		if err != nil {
			log.Fatalf("init order.v1 client: %v", err)
		}
		defer orderConn.Close()
		orderClient = orderv1.NewOrderServiceClient(orderConn)
		logger.Info(ctx, "system", fmt.Sprintf("order.v1 API → %s", cfg.Dependencies.OrderServiceGRPC))
	} else {
		logger.Warn(ctx, "system", "order.v1 grpc address is empty; self-hosted order proxy will fail closed")
	}

	// ── Plan resolver ─────────────────────────────────────────────────────────
	planResolver := plan.NewResolver(accClient, cfg.RuntimePlans, cfg.RuntimePlatform)

	// ── Provisioner ───────────────────────────────────────────────────────────
	// Phase D1 section 5: pick a backend per `provisioning.backend`.
	//   ""       → NoOpProvisioner (default; EnsureHostedRuntime fails
	//              closed with FailedPrecondition).
	//   "docker" → DockerProvisioner (section 5.5).
	var provisioner provision.Provisioner
	switch cfg.Provisioning.Backend {
	case "", "noop":
		provisioner = provision.NoOpProvisioner{}
		logger.Info(ctx, "system", "provisioner: noop (EnsureHostedRuntime will fail closed)")
	case "docker":
		dialAddr := cfg.Provisioning.Docker.RuntimeChannelDialAddr
		if dialAddr == "" {
			network := fallbackString(cfg.Provisioning.Docker.NetworkMode, "host")
			if network == "host" {
				// Host networking: container shares host net stack, so
				// 127.0.0.1 + the bind port works.
				dialAddr = "127.0.0.1" + portFromBindAddr(cfg.RuntimeChannelServer.GRPCAddr)
				logger.Info(ctx, "system", fmt.Sprintf(
					"provisioning.docker.runtime_channel_dial_addr unset; defaulted to %q for host networking", dialAddr,
				))
			} else {
				log.Fatalf("provisioning.docker.runtime_channel_dial_addr is required when network_mode=%q (no safe default)", network)
			}
		}
		provisioner = provision.NewDockerProvisioner(
			provision.ExecCommandRunner{},
			cfg.Provisioning,
			dialAddr,
		)
		logger.Info(ctx, "system", fmt.Sprintf(
			"provisioner: docker image=%s network=%s runtime_channel_dial_addr=%s",
			cfg.Provisioning.Image,
			fallbackString(cfg.Provisioning.Docker.NetworkMode, "host"),
			dialAddr,
		))
		if message := runtimeCoverageStartupLog(cfg.Provisioning.Docker.Coverage); message != "" {
			logger.Info(ctx, "system", message)
		}
	default:
		log.Fatalf("unknown provisioning.backend=%q (expected: noop, docker)", cfg.Provisioning.Backend)
	}

	// ── RuntimeChannel stream registry (Phase D3) ──────────────────────────
	runtimeChannelSvc := runtimechannel.NewWithConfig(repo, runtimechannel.Config{
		Auth: runtimechannel.AuthConfig{
			AllowBareRuntime: cfg.RuntimePlatform.DebugBareRuntimeEnabled,
		},
		ExpectedDependencyProfile: expectedRuntimeDependencyProfile(cfg.RuntimeChannelServer.DependencyProfile),
	})
	credentialSvc := credential.New(repo, runtimeChannelSvc)
	runtimeCertSigner, err := runtimeClientCertSignerFromConfig(cfg.RuntimeChannelServer.TLS)
	if err != nil {
		log.Fatalf("init runtime client cert signer: %v", err)
	}
	if runtimeCertSigner != nil {
		credentialSvc.SetCertificateSigner(runtimeCertSigner)
		logger.Info(ctx, "system", "runtime client certificate signer enabled")
	} else {
		logger.Warn(ctx, "system", "runtime client certificate signer disabled; runtime credential issuing will fail closed")
	}
	runtimeServerCAPEM, err := runtimeServerCAPEMFromConfig(cfg.RuntimeChannelServer.TLS)
	if err != nil {
		log.Fatalf("init runtime channel server ca bundle: %v", err)
	}
	if len(runtimeServerCAPEM) > 0 {
		credentialSvc.SetRuntimeServerCAPEM(runtimeServerCAPEM)
		logger.Info(ctx, "system", "runtime channel server ca bundle enabled")
	} else {
		logger.Warn(ctx, "system", "runtime channel server ca bundle disabled; runtime credential issuing will fail closed")
	}

	// ── Notification publisher ─────────────────────────────────────────────
	var notificationPublisher cpnotify.Publisher = cpnotify.NoopPublisher{}
	if cfg.Notification.Enabled {
		kafkaPublisher, err := cpnotify.NewKafkaPublisher(cfg.Notification.Kafka.Brokers, cfg.Notification.Kafka.Topic, cfg.Notification.Kafka.ClientID)
		if err != nil {
			logger.Warn(ctx, "system", fmt.Sprintf("notification publisher disabled: %v", err))
		} else {
			defer kafkaPublisher.Close()
			notificationPublisher = kafkaPublisher
			logger.Info(ctx, "system", fmt.Sprintf("notification publisher enabled topic=%s brokers=%v", cfg.Notification.Kafka.Topic, cfg.Notification.Kafka.Brokers))
		}
	} else {
		logger.Info(ctx, "system", "notification publisher disabled")
	}
	runtimeChannelSvc.SetNotificationPublisher(notificationPublisher)

	// ── Runtime control-plane service + gRPC handler ───────────────────────
	runtimeCfg := runtime.Config{
		HeartbeatGrace:              time.Duration(cfg.RuntimePlatform.HeartbeatGraceSeconds) * time.Second,
		DeathGrace:                  time.Duration(cfg.RuntimePlatform.DeathGraceSeconds) * time.Second,
		BareRuntimeDeathGrace:       time.Duration(cfg.RuntimePlatform.BareRuntimeDeathGraceSeconds) * time.Second,
		RuntimePlatform:             cfg.RuntimePlatform,
		Provisioning:                cfg.Provisioning,
		Provisioner:                 provisioner,
		SessionClient:               accClient.ServiceClient(),
		RuntimeStreamCloser:         runtimeChannelSvc,
		HostedCredentialIssuer:      credentialSvc,
		NotificationPublisher:       notificationPublisher,
		RuntimeServerCAPEM:          runtimeServerCAPEM,
		RuntimeChannelTLSServerName: cfg.RuntimeChannelServer.TLS.ServerName,
	}
	if runtimeCertSigner != nil {
		runtimeCfg.RuntimeCertSigner = runtimeCertSigner
	}
	runtimeSvc := runtime.New(repo, planResolver, runtimeCfg)
	watchdogEvery := time.Duration(cfg.RuntimePlatform.HeartbeatGraceSeconds) * time.Second / 2
	if watchdogEvery < 5*time.Second {
		watchdogEvery = 5 * time.Second
	}
	go func() {
		ticker := time.NewTicker(watchdogEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				stale, err := runtimeSvc.ReapStaleRuntimes(ctx)
				if err != nil {
					logger.Warn(ctx, "system", fmt.Sprintf("runtime watchdog failed: %v", err))
					continue
				}
				if len(stale) > 0 {
					logger.Warn(ctx, "system", fmt.Sprintf("runtime watchdog marked %d runtime(s) unhealthy", len(stale)))
				}
			}
		}
	}()

	// ── Market-data control-plane service (Phase D2) ───────────────────────
	// Shares the *sql.DB pool with the runtime repository; same control_panel
	// database, distinct package ownership. Tables: market_data_*.
	marketDataRepo := mdrepo.NewTimescaleRepository(repo.DB())
	marketDataQuery := runtimechannel.NewMarketDataQuery(runtimechannel.MarketDataQueryConfig{
		Host:     cfg.MarketData.Host,
		Port:     cfg.MarketData.Port,
		User:     cfg.MarketData.User,
		Password: cfg.MarketData.Password,
		Database: cfg.MarketData.Database,
		SSLMode:  cfg.MarketData.SSLMode,
	})
	marketDataSvc := marketdata.NewService(marketDataRepo, marketdata.WithMarketDataQuery(marketDataQuery))
	debuggerSvc := debugger.New(repo, runtimeChannelSvc, marketDataQuery)

	// ── RuntimeChannel + credential service (Phase D3) ─────────────────────
	// RuntimeChannel verifies signed HELLO frames against runtime_credentials,
	// keeps the live stream double-index, and acts as the credential revoke
	// closer so revocation actively closes streams.
	platformProxy := runtimechannel.NewPlatformProxy(
		accClient.ServiceClient(),
		orderClient,
		marketDataSvc,
	)
	platformProxy.SetNotificationPublisher(notificationPublisher)
	platformProxy.SetMarketDataQuery(marketDataQuery)
	platformProxy.SetDatasetDeliverer(runtimeChannelSvc)
	platformProxy.SetDebugReplayStarter(debuggerSvc)
	runtimeChannelSvc.SetPlatformDispatcher(platformProxy)
	incomeDeliveryWorker := runtimechannel.NewIncomeDeliveryWorker(
		runtimechannel.NewPortfolioIncomeDeliverySessionSource(runtimeChannelSvc, accClient.ServiceClient()),
		accClient.ServiceClient(),
		runtimeChannelSvc,
		runtimechannel.IncomeDeliveryConfig{},
	)
	runtimeChannelSvc.SetIncomeDeliveryObserver(incomeDeliveryWorker)
	go func() {
		if err := incomeDeliveryWorker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Warn(ctx, "system", fmt.Sprintf("Income RuntimeChannel delivery worker stopped: %v", err))
		}
	}()
	logger.Info(ctx, "system", "Income RuntimeChannel delivery enabled")
	if cfg.MarketData.LiveDeliveryEnabled {
		liveDeliveryWorker := runtimechannel.NewKafkaLiveDeliveryWorker(
			marketDataRepo,
			runtimeChannelSvc,
			runtimechannel.KafkaLiveDeliveryConfig{
				Brokers:               cfg.MarketData.KafkaBrokers,
				OwnerInstanceID:       runtimeChannelSvc.InstanceID(),
				NotificationPublisher: notificationPublisher,
			},
		)
		go func() {
			if err := liveDeliveryWorker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Warn(ctx, "system", fmt.Sprintf("market-data live delivery worker stopped: %v", err))
			}
		}()
		logger.Info(ctx, "system", fmt.Sprintf("market-data live delivery enabled brokers=%v", cfg.MarketData.KafkaBrokers))
	} else {
		logger.Info(ctx, "system", "market-data live delivery disabled")
	}
	if orderClient != nil {
		orderLifecycleWorker := runtimechannel.NewOrderLifecycleDeliveryWorker(
			marketDataRepo,
			orderClient,
			runtimeChannelSvc,
			runtimechannel.OrderLifecycleDeliveryConfig{},
		)
		go func() {
			if err := orderLifecycleWorker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Warn(ctx, "system", fmt.Sprintf("order lifecycle delivery worker stopped: %v", err))
			}
		}()
		logger.Info(ctx, "system", "order lifecycle RuntimeChannel delivery enabled")
	} else {
		logger.Warn(ctx, "system", "order lifecycle RuntimeChannel delivery disabled: order.v1 client is not configured")
	}

	// ── HTTP Server (health + readiness) ──────────────────────────────────────
	healthHandler := httpserver.NewHandler()
	httpAddr := cfg.Server.HTTPAddr
	if httpAddr == "" {
		httpAddr = ":8082"
	}
	httpMux := httpmw.Middleware(logger.Instance())(httpserver.NewMux(healthHandler))
	httpSrv := &http.Server{
		Addr:         httpAddr,
		Handler:      httpMux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}
	go func() {
		logger.Info(ctx, "system", fmt.Sprintf("http server listening on %s", httpAddr))
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("http server error: %v", err)
		}
	}()

	// ── gRPC Server ───────────────────────────────────────────────────────────
	grpcAddr := cfg.Server.GRPCAddr
	if grpcAddr == "" {
		grpcAddr = ":50054"
	}
	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(grpcmw.UnaryServerInterceptor(logger.Instance())),
	)
	controlPanelGRPC := runtime.NewControlPanelGRPCService(runtimeSvc, credentialSvc, runtimeChannelSvc, accClient.ServiceClient())
	controlPanelGRPC.SetDebuggerService(debuggerSvc)
	cpv1.RegisterControlPanelServiceServer(grpcSrv, controlPanelGRPC)
	mdv1.RegisterMarketDataControlPlaneServiceServer(grpcSrv, marketDataSvc)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("listen grpc: %v", err)
	}
	go func() {
		logger.Info(ctx, "system", fmt.Sprintf("grpc server listening on %s", grpcAddr))
		if err := grpcSrv.Serve(lis); err != nil {
			log.Printf("grpc server error: %v", err)
		}
	}()

	runtimeChannelAddr := cfg.RuntimeChannelServer.GRPCAddr
	if runtimeChannelAddr == "" {
		runtimeChannelAddr = ":50055"
	}
	runtimeChannelCreds, err := runtimechannel.ServerTLSCredentials(runtimechannel.ServerTLSConfig{
		Enabled:      cfg.RuntimeChannelServer.TLS.Enabled,
		CertFile:     cfg.RuntimeChannelServer.TLS.CertFile,
		KeyFile:      cfg.RuntimeChannelServer.TLS.KeyFile,
		ClientCAFile: cfg.RuntimeChannelServer.TLS.ClientCAFile,
	})
	if err != nil {
		log.Fatalf("init runtime channel tls: %v", err)
	}
	var runtimeChannelOptions []grpc.ServerOption
	if runtimeChannelCreds != nil {
		runtimeChannelOptions = append(runtimeChannelOptions, grpc.Creds(runtimeChannelCreds))
	}
	runtimeChannelGRPCSrv := grpc.NewServer(runtimeChannelOptions...)
	cpv1.RegisterControlPanelServiceServer(runtimeChannelGRPCSrv, runtimechannel.NewGRPCService(runtimeChannelSvc))
	runtimeChannelLis, err := net.Listen("tcp", runtimeChannelAddr)
	if err != nil {
		log.Fatalf("listen runtime channel grpc: %v", err)
	}
	go func() {
		tlsMode := "disabled"
		if runtimeChannelCreds != nil {
			tlsMode = "server_tls"
			if cfg.RuntimeChannelServer.TLS.ClientCAFile != "" {
				tlsMode = "mutual_tls"
			}
		}
		logger.Info(ctx, "system", fmt.Sprintf("runtime channel grpc server listening on %s tls=%s", runtimeChannelAddr, tlsMode))
		if err := runtimeChannelGRPCSrv.Serve(runtimeChannelLis); err != nil {
			log.Printf("runtime channel grpc server error: %v", err)
		}
	}()

	healthHandler.MarkReady()
	logger.Info(ctx, "system", "control-panel-service ready")

	// ── Graceful Shutdown ─────────────────────────────────────────────────────
	<-ctx.Done()
	logger.Info(context.Background(), "system", "shutting down")

	healthHandler.MarkNotReady()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	_ = httpSrv.Shutdown(shutdownCtx)
	runtimeChannelSvc.CloseAllStreams()
	stopGRPCServer(shutdownCtx, runtimeChannelGRPCSrv)
	stopGRPCServer(shutdownCtx, grpcSrv)

	logger.Info(context.Background(), "system", "control-panel-service stopped")
}

type grpcStopper interface {
	GracefulStop()
	Stop()
}

func stopGRPCServer(ctx context.Context, server grpcStopper) {
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return
	case <-ctx.Done():
		select {
		case <-done:
			return
		default:
		}
		server.Stop()
		<-done
	}
}

func runtimeCoverageStartupLog(coverage config.DockerCoverageConfig) string {
	if !coverage.Enabled {
		return ""
	}
	return fmt.Sprintf(
		"runtime coverage: enabled image=%s output_dir=%s stop_timeout_seconds=%d",
		coverage.Image,
		coverage.OutputDir,
		coverage.StopTimeoutSeconds,
	)
}
