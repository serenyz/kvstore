package gateway

import (
	"context"
	"net"
	"reflect"
	"testing"
	"time"

	kvstorepb "kvstore/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type stubKVClient struct {
	put    func(context.Context, *kvstorepb.PutRequest) (*kvstorepb.PutResponse, error)
	delete func(context.Context, *kvstorepb.DeleteRequest) (*kvstorepb.DeleteResponse, error)
	read   func(context.Context, *kvstorepb.ReadRequest) (*kvstorepb.ReadResponse, error)
}

func (s *stubKVClient) Put(ctx context.Context, req *kvstorepb.PutRequest, _ ...grpc.CallOption) (*kvstorepb.PutResponse, error) {
	if s.put == nil {
		return nil, status.Error(codes.Unimplemented, "Put is not stubbed")
	}
	return s.put(ctx, req)
}

func (s *stubKVClient) Delete(ctx context.Context, req *kvstorepb.DeleteRequest, _ ...grpc.CallOption) (*kvstorepb.DeleteResponse, error) {
	if s.delete == nil {
		return nil, status.Error(codes.Unimplemented, "Delete is not stubbed")
	}
	return s.delete(ctx, req)
}

func (s *stubKVClient) Read(ctx context.Context, req *kvstorepb.ReadRequest, _ ...grpc.CallOption) (*kvstorepb.ReadResponse, error) {
	if s.read == nil {
		return nil, status.Error(codes.Unimplemented, "Read is not stubbed")
	}
	return s.read(ctx, req)
}

func TestGatewayPutRetriesUnreachableBackend(t *testing.T) {
	firstCalls := 0
	secondCalls := 0
	pool := testBackendPool(
		[]kvstorepb.KVStoreClient{
			&stubKVClient{put: func(context.Context, *kvstorepb.PutRequest) (*kvstorepb.PutResponse, error) {
				firstCalls++
				return nil, status.Error(codes.Unavailable, "write outcome unknown")
			}},
			&stubKVClient{put: func(context.Context, *kvstorepb.PutRequest) (*kvstorepb.PutResponse, error) {
				secondCalls++
				return &kvstorepb.PutResponse{}, nil
			}},
		},
		[]bool{true, true},
		[]uint64{0, 0},
	)
	pool.buckets[1].addScoreAt(uint64(time.Now().Unix()), 100)

	server := &gatewayServer{pool: pool}
	if _, err := server.Put(context.Background(), &kvstorepb.PutRequest{Key: "key", Value: "value"}); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if firstCalls != 1 || secondCalls != 1 {
		t.Fatalf("backend calls = (%d, %d), want (1, 1)", firstCalls, secondCalls)
	}
}

func TestGatewayRevisionReadDoesNotRetryOutOfRange(t *testing.T) {
	var calls []string
	pool := testBackendPool(
		[]kvstorepb.KVStoreClient{
			&stubKVClient{read: func(context.Context, *kvstorepb.ReadRequest) (*kvstorepb.ReadResponse, error) {
				calls = append(calls, "first")
				return nil, status.Error(codes.OutOfRange, "revision not applied")
			}},
			&stubKVClient{read: func(context.Context, *kvstorepb.ReadRequest) (*kvstorepb.ReadResponse, error) {
				calls = append(calls, "second")
				return &kvstorepb.ReadResponse{
					Mode:              kvstorepb.ReadMode_READ_MODE_REVISION,
					RequestedRevision: 5,
					Record:            &kvstorepb.Record{Key: "key", Value: "value", ModRevision: 4},
				}, nil
			}},
		},
		[]bool{true, true},
		[]uint64{10, 5},
	)
	pool.buckets[1].addScoreAt(uint64(time.Now().Unix()), 100)

	server := &gatewayServer{pool: pool}
	_, err := server.Read(context.Background(), &kvstorepb.ReadRequest{
		Key:      "key",
		Mode:     kvstorepb.ReadMode_READ_MODE_REVISION,
		Revision: 5,
	})
	if status.Code(err) != codes.OutOfRange {
		t.Fatalf("Read() error = %v, want OutOfRange", err)
	}
	if !reflect.DeepEqual(calls, []string{"first"}) {
		t.Fatalf("backend calls = %v, want [first]", calls)
	}
}

func TestGatewayForwardsIncomingMetadata(t *testing.T) {
	pool := testBackendPool(
		[]kvstorepb.KVStoreClient{
			&stubKVClient{read: func(ctx context.Context, _ *kvstorepb.ReadRequest) (*kvstorepb.ReadResponse, error) {
				md, ok := metadata.FromOutgoingContext(ctx)
				if !ok || !reflect.DeepEqual(md.Get("x-request-id"), []string{"request-1"}) {
					t.Fatalf("outgoing metadata = %v", md)
				}
				return &kvstorepb.ReadResponse{Record: &kvstorepb.Record{Key: "key", Value: "value"}}, nil
			}},
		},
		[]bool{true},
		[]uint64{0},
	)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", "request-1"))
	server := &gatewayServer{pool: pool}
	if _, err := server.Read(ctx, &kvstorepb.ReadRequest{Key: "key", Mode: kvstorepb.ReadMode_READ_MODE_LOCAL}); err != nil {
		t.Fatalf("Read() error = %v", err)
	}
}

func TestGatewayGRPCRoundTrip(t *testing.T) {
	pool := testBackendPool(
		[]kvstorepb.KVStoreClient{
			&stubKVClient{read: func(context.Context, *kvstorepb.ReadRequest) (*kvstorepb.ReadResponse, error) {
				return &kvstorepb.ReadResponse{
					Mode:   kvstorepb.ReadMode_READ_MODE_LOCAL,
					Record: &kvstorepb.Record{Key: "key", Value: "through-gateway"},
				}, nil
			}},
		},
		[]bool{true},
		[]uint64{0},
	)

	listener := bufconn.Listen(1 << 20)
	server, healthServer := newGatewayServer(pool)
	serveErrorC := make(chan error, 1)
	go func() {
		serveErrorC <- server.Serve(listener)
	}()
	t.Cleanup(func() {
		healthServer.Shutdown()
		server.Stop()
		<-serveErrorC
	})

	conn, err := grpc.NewClient(
		"passthrough:///gateway-test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := kvstorepb.NewKVStoreClient(conn).Read(ctx, &kvstorepb.ReadRequest{
		Key:  "key",
		Mode: kvstorepb.ReadMode_READ_MODE_LOCAL,
	})
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if response.GetRecord().GetValue() != "through-gateway" {
		t.Fatalf("Read() value = %q", response.GetRecord().GetValue())
	}

	healthResponse, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{
		Service: kvstorepb.KVStore_ServiceDesc.ServiceName,
	})
	if err != nil {
		t.Fatalf("Health.Check() error = %v", err)
	}
	if healthResponse.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health status = %v, want SERVING", healthResponse.GetStatus())
	}
}

func testBackendPool(clients []kvstorepb.KVStoreClient, healthy []bool, revisions []uint64) *backendPool {
	backends := make([]*backend, len(clients))
	buckets := make([]*window, len(clients))
	for i, client := range clients {
		backends[i] = &backend{ID: uint64(i + 1), kv: client}
		buckets[i] = &window{}
	}

	return &backendPool{
		backends:  backends,
		healthy:   append([]bool(nil), healthy...),
		revisions: append([]uint64(nil), revisions...),
		buckets:   buckets,
	}
}
