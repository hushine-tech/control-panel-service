package runtimechannel

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
)

type GRPCService struct {
	cpv1.UnimplementedControlPanelServiceServer
	svc *Service
}

func NewGRPCService(svc *Service) *GRPCService {
	return &GRPCService{svc: svc}
}

func (g *GRPCService) RuntimeChannel(stream cpv1.ControlPanelService_RuntimeChannelServer) error {
	if g == nil || g.svc == nil {
		return status.Error(codes.FailedPrecondition, "runtime channel service is not configured")
	}
	return g.svc.Handle(stream)
}
