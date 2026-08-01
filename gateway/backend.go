package gateway

import (
	"context"
	"fmt"
	kvstorepb "kvstore/pb"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const maxMessageSize = 5 << 20

type backend struct {
	ID      uint64
	Address string

	conn    *grpc.ClientConn
	kv      kvstorepb.KVStoreClient
	cluster kvstorepb.ClusterClient
	health  healthpb.HealthClient
}

var (
	_ kvstorepb.KVStoreClient = (*backend)(nil)
	_ kvstorepb.ClusterClient = (*backend)(nil)
)

func newBackend(id uint64, address string) (*backend, error) {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  200 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   5 * time.Second,
			},
			MinConnectTimeout: 2 * time.Second,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(maxMessageSize),
			grpc.MaxCallRecvMsgSize(maxMessageSize),
		),
	)

	if err != nil {
		return nil, fmt.Errorf("create backend %d connection: %w", id, err)
	}

	return &backend{
		ID:      id,
		Address: address,
		conn:    conn,
		kv:      kvstorepb.NewKVStoreClient(conn),
		cluster: kvstorepb.NewClusterClient(conn),
		health:  healthpb.NewHealthClient(conn),
	}, nil
}

func (b *backend) checkHealth(ctx context.Context) bool {
	checkCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	resp, err := b.health.Check(checkCtx, &healthpb.HealthCheckRequest{Service: "kvstore.KVStore"})
	ok := err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING
	return ok
}

func (b *backend) close() error {
	return b.conn.Close()
}

func (b *backend) Put(ctx context.Context, in *kvstorepb.PutRequest, opts ...grpc.CallOption) (*kvstorepb.PutResponse, error) {
	return b.kv.Put(ctx, in, opts...)
}

func (b *backend) Delete(ctx context.Context, in *kvstorepb.DeleteRequest, opts ...grpc.CallOption) (*kvstorepb.DeleteResponse, error) {
	return b.kv.Delete(ctx, in, opts...)
}

func (b *backend) Read(ctx context.Context, in *kvstorepb.ReadRequest, opts ...grpc.CallOption) (*kvstorepb.ReadResponse, error) {
	return b.kv.Read(ctx, in, opts...)
}

func (b *backend) AddMember(ctx context.Context, in *kvstorepb.AddMemberRequest, opts ...grpc.CallOption) (*kvstorepb.MemberChangeResponse, error) {
	return b.cluster.AddMember(ctx, in, opts...)
}

func (b *backend) RemoveMember(ctx context.Context, in *kvstorepb.RemoveMemberRequest, opts ...grpc.CallOption) (*kvstorepb.MemberChangeResponse, error) {
	return b.cluster.RemoveMember(ctx, in, opts...)
}
