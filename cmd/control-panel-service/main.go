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
	"github.com/hushine-tech/control-panel-service/internal/accountclient"
	"github.com/hushine-tech/control-panel-service/internal/config"
	"github.com/hushine-tech/control-panel-service/internal/credential"
	"github.com/hushine-tech/control-panel-service/internal/debugger"
	"github.com/hushine-tech/control-panel-service/internal/httpserver"
	"github.com/hushine-tech/control-panel-service/internal/logger"
	"github.com/hushine-tech/control-panel-service/internal/marketdata"
	mdrepo "github.com/hushine-tech/control-panel-service/internal/marketdata/repository"
	cpnotify "github.com/hushine-tech/control-panel-service/internal/notification"
	"github.com/hushine-tech/control-panel-service/internal/plan"
	"github.com/hushine-tech/control-panel-service/internal/provision"
	"github.com/hushine-tech/control-panel-service/internal/repository"
	"github.com/hushine-tech/control-panel-service/internal/runtime"
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
// address (":50054", "0.0.0.0:50054", "127.0.0.1:50054"). Used to
// derive a reasonable dial address for runtime containers in host
// networking mode. Falls back to ":50054" if parsing is ambiguous.
func portFromBindAddr(bind string) string {
	if bind == "" {
		return ":50054"
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
	cfg.ApplyEnvOverrides()

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

	// ── account-service client (for plan_code lookup) ────────────────────────
	accClient, err := accountclient.New(
		cfg.Dependencies.AccountServiceGRPC,
		grpc.WithUnaryInterceptor(grpcclientmw.UnaryClientInterceptor(logger.Instance())),
	)
	if err != nil {
		log.Fatalf("init account-service client: %v", err)
	}
	defer accClient.Close()
	logger.Info(ctx, "system", fmt.Sprintf("account-service client → %s", cfg.Dependencies.AccountServiceGRPC))

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
		dialAddr := cfg.Provisioning.Docker.ControlPanelDialAddr
		if dialAddr == "" {
			network := fallbackString(cfg.Provisioning.Docker.NetworkMode, "host")
			if network == "host" {
				// Host networking: container shares host net stack, so
				// 127.0.0.1 + the bind port works.
				dialAddr = "127.0.0.1" + portFromBindAddr(cfg.Server.GRPCAddr)
				logger.Info(ctx, "system", fmt.Sprintf(
					"provisioning.docker.control_panel_dial_addr unset; defaulted to %q for host networking", dialAddr,
				))
			} else {
				log.Fatalf("provisioning.docker.control_panel_dial_addr is required when network_mode=%q (no safe default)", network)
			}
		}
		provisioner = provision.NewDockerProvisioner(
			provision.ExecCommandRunner{},
			cfg.Provisioning,
			dialAddr,
		)
		logger.Info(ctx, "system", fmt.Sprintf(
			"provisioner: docker image=%s network=%s control_panel_dial_addr=%s",
			cfg.Provisioning.Image,
			fallbackString(cfg.Provisioning.Docker.NetworkMode, "host"),
			dialAddr,
		))
	default:
		log.Fatalf("unknown provisioning.backend=%q (expected: noop, docker)", cfg.Provisioning.Backend)
	}

	// ── RuntimeChannel stream registry (Phase D3) ──────────────────────────
	runtimeChannelSvc := runtimechannel.New(repo)
	credentialSvc := credential.New(repo, runtimeChannelSvc)

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

	// ── Runtime control-plane service + gRPC handler ───────────────────────
	runtimeSvc := runtime.New(repo, planResolver, runtime.Config{
		HeartbeatGrace:         time.Duration(cfg.RuntimePlatform.HeartbeatGraceSeconds) * time.Second,
		DeathGrace:             time.Duration(cfg.RuntimePlatform.DeathGraceSeconds) * time.Second,
		CallerTokenTTL:         time.Duration(cfg.RuntimePlatform.CallerTokenTTLSeconds) * time.Second,
		Provisioning:           cfg.Provisioning,
		Provisioner:            provisioner,
		SessionClient:          accClient.ServiceClient(),
		RuntimeStreamCloser:    runtimeChannelSvc,
		HostedCredentialIssuer: credentialSvc,
		NotificationPublisher:  notificationPublisher,
	})
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
	if cfg.MarketData.LiveDeliveryEnabled {
		liveDeliveryWorker := runtimechannel.NewKafkaLiveDeliveryWorker(
			marketDataRepo,
			runtimeChannelSvc,
			runtimechannel.KafkaLiveDeliveryConfig{
				Brokers:         cfg.MarketData.KafkaBrokers,
				OwnerInstanceID: runtimeChannelSvc.InstanceID(),
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

	healthHandler.MarkReady()
	logger.Info(ctx, "system", "control-panel-service ready")

	// ── Graceful Shutdown ─────────────────────────────────────────────────────
	<-ctx.Done()
	logger.Info(context.Background(), "system", "shutting down")

	healthHandler.MarkNotReady()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	_ = httpSrv.Shutdown(shutdownCtx)
	grpcSrv.GracefulStop()

	logger.Info(context.Background(), "system", "control-panel-service stopped")
}
