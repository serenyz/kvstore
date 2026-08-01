package gateway

import (
	"context"

	kvstorepb "kvstore/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// gatewayServer 保留客户端 RPC 契约，并把每个请求转发给 backendPool 排出的节点。
type gatewayServer struct {
	kvstorepb.UnimplementedKVStoreServer
	kvstorepb.UnimplementedClusterServer

	pool *backendPool
}

func newGatewayServer(pool *backendPool) (*grpc.Server, *health.Server) {
	server := grpc.NewServer(grpc.MaxRecvMsgSize(maxMessageSize))
	healthServer := health.NewServer()
	gateway := &gatewayServer{pool: pool}

	kvstorepb.RegisterKVStoreServer(server, gateway)
	kvstorepb.RegisterClusterServer(server, gateway)
	healthpb.RegisterHealthServer(server, healthServer)

	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(kvstorepb.KVStore_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(kvstorepb.Cluster_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	reflection.Register(server)
	return server, healthServer
}

func (s *gatewayServer) Put(ctx context.Context, req *kvstorepb.PutRequest) (*kvstorepb.PutResponse, error) {
	forwardCtx := forwardContext(ctx)
	var response *kvstorepb.PutResponse
	var err error
	for _, index := range s.pool.pick() {
		response, err = s.pool.backends[index].Put(forwardCtx, req)
		if err == nil {
			s.pool.updateState(index, true, recordRevision(response.GetRecord()))
			return response, nil
		}
		if !unreachable(err) {
			return nil, err
		}
	}
	return nil, err
}

func (s *gatewayServer) Delete(ctx context.Context, req *kvstorepb.DeleteRequest) (*kvstorepb.DeleteResponse, error) {
	forwardCtx := forwardContext(ctx)
	var response *kvstorepb.DeleteResponse
	var err error
	for _, index := range s.pool.pick() {
		response, err = s.pool.backends[index].Delete(forwardCtx, req)
		if err == nil {
			s.pool.updateState(index, true, recordRevision(response.GetRecord()))
			return response, nil
		}
		if !unreachable(err) {
			return nil, err
		}
	}
	return nil, err
}

func (s *gatewayServer) Read(ctx context.Context, req *kvstorepb.ReadRequest) (*kvstorepb.ReadResponse, error) {
	candidates := s.pool.pick()
	if req.GetMode() == kvstorepb.ReadMode_READ_MODE_REVISION {
		candidates = s.pool.pickRevision(req.GetRevision())
	}

	forwardCtx := forwardContext(ctx)
	var lastErr error
	for _, index := range candidates {
		response, err := s.pool.backends[index].Read(forwardCtx, req)
		if err == nil {
			s.pool.updateState(index, true, observedReadRevision(req, response))
			return response, nil
		}
		lastErr = err

		if !unreachable(err) {
			return nil, err
		}
	}

	return nil, lastErr
}

func (s *gatewayServer) AddMember(ctx context.Context, req *kvstorepb.AddMemberRequest) (*kvstorepb.MemberChangeResponse, error) {
	forwardCtx := forwardContext(ctx)
	var response *kvstorepb.MemberChangeResponse
	var err error
	for _, index := range s.pool.pick() {
		response, err = s.pool.backends[index].AddMember(forwardCtx, req)
		if err == nil || !unreachable(err) {
			return response, err
		}
	}
	return nil, err
}

func (s *gatewayServer) RemoveMember(ctx context.Context, req *kvstorepb.RemoveMemberRequest) (*kvstorepb.MemberChangeResponse, error) {
	forwardCtx := forwardContext(ctx)
	var response *kvstorepb.MemberChangeResponse
	var err error
	for _, index := range s.pool.pick() {
		response, err = s.pool.backends[index].RemoveMember(forwardCtx, req)
		if err == nil || !unreachable(err) {
			return response, err
		}
	}
	return nil, err
}

func unreachable(err error) bool {
	return status.Code(err) == codes.Unavailable
}

func observedReadRevision(req *kvstorepb.ReadRequest, response *kvstorepb.ReadResponse) uint64 {
	if req.GetMode() == kvstorepb.ReadMode_READ_MODE_REVISION {
		return req.GetRevision()
	}
	return recordRevision(response.GetRecord())
}

func recordRevision(record *kvstorepb.Record) uint64 {
	if record == nil {
		return 0
	}
	return record.GetModRevision()
}

func forwardContext(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	return metadata.NewOutgoingContext(ctx, md.Copy())
}
