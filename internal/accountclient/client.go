// Package accountclient is the thin gRPC client control-panel-service uses
// to talk to core-service. Core-service still owns users; market-data
// control-plane state lives in control-panel-service.
package accountclient

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	accountv1 "github.com/hushine-tech/core-service/gen/accountv1"
)

// Client wraps the core-service account.v1 gRPC stub.
type Client struct {
	conn *grpc.ClientConn
	cli  accountv1.AccountServiceClient
}

// New dials core-service at addr. Outbound interceptors (logging,
// tracing) are passed via opts; main.go is expected to inject the
// golang-lib grpcclient interceptor for grpc_ext.log capture.
func New(addr string, opts ...grpc.DialOption) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("core-service grpc address is empty")
	}
	dialOpts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, opts...)
	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial core-service: %w", err)
	}
	return &Client{
		conn: conn,
		cli:  accountv1.NewAccountServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) ServiceClient() accountv1.AccountServiceClient {
	if c == nil {
		return nil
	}
	return c.cli
}

// GetUserPlanCode returns the user's plan_code (e.g. "free" / "developer" /
// "pro"). Returns "" + error on missing user.
func (c *Client) GetUserPlanCode(ctx context.Context, userID int64) (string, error) {
	resp, err := c.cli.GetUser(ctx, &accountv1.GetUserRequest{UserId: userID})
	if err != nil {
		return "", err
	}
	if resp == nil || resp.GetUser() == nil {
		return "", fmt.Errorf("core-service returned empty user")
	}
	return resp.GetUser().GetPlanCode(), nil
}
