package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	controlpanelv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50054", "control-panel-service gRPC address")
	userID := flag.Int64("user", 0, "portfolio.users.id")
	profile := flag.String("profile", "small", "hosted runtime resource profile")
	name := flag.String("name", "", "optional hosted runtime display name")
	timeout := flag.Duration("timeout", 150*time.Second, "smoke timeout")
	flag.Parse()

	if *userID <= 0 {
		fmt.Fprintln(os.Stderr, "required: -user <portfolio.users.id>")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial control-panel-service: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	resp, err := controlpanelv1.NewControlPanelServiceClient(conn).EnsureHostedRuntime(ctx, &controlpanelv1.EnsureHostedRuntimeRequest{
		UserId:          *userID,
		Name:            *name,
		ResourceProfile: *profile,
	})
	if err != nil {
		if st, ok := status.FromError(err); ok {
			fmt.Fprintf(os.Stderr, "EnsureHostedRuntime failed: code=%s message=%s\n", st.Code(), st.Message())
		} else {
			fmt.Fprintf(os.Stderr, "EnsureHostedRuntime failed: %v\n", err)
		}
		os.Exit(1)
	}
	rt := resp.GetRuntime()
	if rt == nil {
		fmt.Fprintln(os.Stderr, "EnsureHostedRuntime returned empty runtime")
		os.Exit(1)
	}
	fmt.Printf("runtime_id=%s source=%s status=%s provisioned=%t endpoint=%s grpc_port=%d credential_key_id=%s\n",
		rt.GetRuntimeId(),
		rt.GetSource(),
		rt.GetStatus(),
		resp.GetProvisioned(),
		rt.GetEndpointHost(),
		rt.GetGrpcPort(),
		rt.GetCredentialKeyId(),
	)
}
