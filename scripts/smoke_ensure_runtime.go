// Phase D1 smoke client: trigger EnsureHostedRuntime end-to-end.
//
// Use:
//
//	go run scripts/smoke_ensure_runtime.go -user 3 -profile small
//
// Verifies the path: handler-side EnsureHostedRuntime gRPC →
// control-panel resolves plan + quota + profile → DockerProvisioner
// runs `docker run` → strategy-runtime container starts → container's
// section-4 self-register code calls back RegisterRuntime →
// EnsureHostedRuntime returns the route + caller_token.
//
// Optional follow-up:
//   - inspect runtime_registry table to see the new row
//   - `docker ps --filter label=hushine.runtime.user_id=<user>` to see
//     the container
//   - call ValidateCallerToken with the returned caller_token to prove
//     the token-store roundtrip
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	cpv1 "github.com/hushine-tech/control-panel-service/gen/controlpanelv1"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50054", "control-panel-service gRPC address")
	user := flag.Int64("user", 0, "user_id to provision a runtime for (REQUIRED, must exist in users table)")
	profile := flag.String("profile", "small", "resource_profile")
	name := flag.String("name", "", "runtime name; empty lets control-panel generate hosted-*")
	timeout := flag.Duration("timeout", 60*time.Second, "RPC timeout")
	validate := flag.Bool("validate", true, "after EnsureHostedRuntime succeeds, call ValidateCallerToken to prove the token-store roundtrip")
	flag.Parse()

	if *user <= 0 {
		log.Fatalf("required: -user <id>")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, *addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer conn.Close()
	cli := cpv1.NewControlPanelServiceClient(conn)

	fmt.Printf("→ EnsureHostedRuntime(user_id=%d, name=%q, resource_profile=%q)\n", *user, *name, *profile)
	start := time.Now()
	resp, err := cli.EnsureHostedRuntime(ctx, &cpv1.EnsureHostedRuntimeRequest{
		UserId:          *user,
		Name:            *name,
		ResourceProfile: *profile,
	})
	elapsed := time.Since(start)
	if err != nil {
		log.Fatalf("EnsureHostedRuntime failed after %s: %v", elapsed, err)
	}
	rt := resp.GetRuntime()
	fmt.Printf("✓ %s — provisioned=%t\n", elapsed, resp.GetProvisioned())
	fmt.Printf("  runtime_id     = %s\n", rt.GetRuntimeId())
	fmt.Printf("  name           = %s\n", rt.GetName())
	fmt.Printf("  status         = %s\n", rt.GetStatus())
	fmt.Printf("  endpoint       = %s\n", resp.GetGrpcEndpoint())
	fmt.Printf("  caller_token   = %s…\n", truncate(resp.GetCallerToken(), 16))
	if ts := resp.GetCallerTokenExpiresAt(); ts != nil {
		fmt.Printf("  token expires  = %s\n", ts.AsTime().Format(time.RFC3339))
	}

	if !*validate {
		return
	}

	fmt.Println()
	fmt.Println("→ ValidateCallerToken (round trip)")
	vresp, err := cli.ValidateCallerToken(ctx, &cpv1.ValidateCallerTokenRequest{
		CallerToken: resp.GetCallerToken(),
		RuntimeId:   rt.GetRuntimeId(),
	})
	if err != nil {
		log.Fatalf("ValidateCallerToken failed: %v", err)
	}
	if !vresp.GetValid() {
		log.Fatalf("ValidateCallerToken returned valid=false reason=%q", vresp.GetReason())
	}
	fmt.Printf("✓ valid=true user_id=%d\n", vresp.GetUserId())
	if vresp.GetUserId() != *user {
		log.Fatalf("BUG: returned user_id=%d expected %d", vresp.GetUserId(), *user)
	}

	fmt.Println()
	fmt.Println("Cross-runtime token mismatch should reject:")
	vresp2, err := cli.ValidateCallerToken(ctx, &cpv1.ValidateCallerTokenRequest{
		CallerToken: resp.GetCallerToken(),
		RuntimeId:   "rt_some_other_runtime",
	})
	if err != nil {
		log.Fatalf("ValidateCallerToken (mismatch) failed: %v", err)
	}
	if vresp2.GetValid() {
		log.Fatalf("BUG: ValidateCallerToken returned valid=true for runtime_id mismatch")
	}
	fmt.Printf("✓ valid=false reason=%q (as expected)\n", vresp2.GetReason())
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
