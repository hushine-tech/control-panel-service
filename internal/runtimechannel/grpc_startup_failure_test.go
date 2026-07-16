package runtimechannel

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	"github.com/hushine-tech/control-panel-service/internal/domain"
)

func TestGRPCServiceForwardsRuntimeStartupFailure(t *testing.T) {
	repo, privateKey, now := newAuthFixture(t, domain.CredentialStatusActive)
	svc := NewWithConfig(repo, Config{ExpectedDependencyProfile: expectedDependencyProfile()})
	svc.SetClock(func() time.Time { return now })

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	cpv1.RegisterControlPanelServiceServer(server, NewGRPCService(svc))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	connection, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	defer connection.Close()

	response, err := cpv1.NewControlPanelServiceClient(connection).ReportRuntimeStartupFailure(
		ctx,
		signedStartupFailureRequest(t, privateKey, now, "nonce-grpc"),
	)
	if err != nil {
		t.Fatalf("ReportRuntimeStartupFailure RPC: %v", err)
	}
	if !response.GetRecorded() || len(repo.admissions) != 1 {
		t.Fatalf("response=%+v admissions=%+v", response, repo.admissions)
	}
}
