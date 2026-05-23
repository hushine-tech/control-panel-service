package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/hushine-tech/control-panel-service/internal/runtimechannel"
	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetStrategyStatusRequiresRuntimeChannel(t *testing.T) {
	channelSvc := runtimechannel.New(newStubRepo())
	g := NewControlPanelGRPCService(nil, nil, channelSvc)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := g.GetStrategyStatus(ctx, &strategyv1.GetStrategyStatusRequest{
		SessionId: "sess-1",
		UserId:    42,
		RuntimeId: "runtime-1",
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable (err=%v)", status.Code(err), err)
	}
}
