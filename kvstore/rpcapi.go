package kvstore

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"strconv"
	"time"

	kvstorepb "kvstore/pb"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

const (
	maxRPCKeyBytes                 = 4 << 10
	maxRPCValueBytes               = 4 << 20
	maxRPCRequestBytes             = maxRPCValueBytes + maxRPCKeyBytes + 64<<10
	defaultLinearizableReadTimeout = 5 * time.Second
)

// rpcStore 是 RPC 层使用的状态机能力集合。
// 通过窄接口隔离传输层和状态机，也便于在不启动 Raft 集群的情况下验证 RPC 契约。
type rpcStore interface {
	Put(context.Context, string, string) (record, error)
	Delete(context.Context, string) (record, error)
	LookupLocal(string) (record, error)
	LookupAtRevision(string, revision) (record, error)
	LookupLinearize(context.Context, string) (record, error)
}

// rpcKVStore 实现 kvstore.KVStore。
type rpcKVStore struct {
	kvstorepb.UnimplementedKVStoreServer

	store                   rpcStore
	linearizableReadTimeout time.Duration
}

// Put 提交一次 Raft 写入，并在本节点应用该日志后返回生成的记录。
func (s *rpcKVStore) Put(ctx context.Context, req *kvstorepb.PutRequest) (*kvstorepb.PutResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := validateRPCKey(req.GetKey()); err != nil {
		return nil, err
	}
	if len([]byte(req.GetValue())) > maxRPCValueBytes {
		return nil, status.Error(codes.InvalidArgument, "value exceeds 4194304 bytes")
	}

	rec, err := s.store.Put(ctx, req.GetKey(), req.GetValue())
	if err != nil {
		return nil, writeRPCError(err)
	}
	return &kvstorepb.PutResponse{Record: toRPCRecord(req.GetKey(), rec)}, nil
}

// Delete 提交删除命令。删除不存在的键是成功的幂等操作，此时 response.record 为空。
func (s *rpcKVStore) Delete(ctx context.Context, req *kvstorepb.DeleteRequest) (*kvstorepb.DeleteResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := validateRPCKey(req.GetKey()); err != nil {
		return nil, err
	}

	rec, err := s.store.Delete(ctx, req.GetKey())
	if err != nil {
		return nil, writeRPCError(err)
	}
	response := &kvstorepb.DeleteResponse{}
	if rec.ModRevision != NoneRevision {
		response.Record = toRPCRecord(req.GetKey(), rec)
	}
	return response, nil
}

// Read 按请求模式选择本地、历史版本或 ReadIndex 读取路径。
func (s *rpcKVStore) Read(ctx context.Context, req *kvstorepb.ReadRequest) (*kvstorepb.ReadResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := validateRPCKey(req.GetKey()); err != nil {
		return nil, err
	}

	mode, err := normalizeReadMode(req.GetMode(), req.GetRevision())
	if err != nil {
		return nil, err
	}

	var rec record
	switch mode {
	case kvstorepb.ReadMode_READ_MODE_LOCAL:
		rec, err = s.store.LookupLocal(req.GetKey())
	case kvstorepb.ReadMode_READ_MODE_REVISION:
		rec, err = s.store.LookupAtRevision(req.GetKey(), revision(req.GetRevision()))
	case kvstorepb.ReadMode_READ_MODE_LINEARIZABLE:
		timeout := s.linearizableReadTimeout
		if timeout <= 0 {
			timeout = defaultLinearizableReadTimeout
		}
		readCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		rec, err = s.store.LookupLinearize(readCtx, req.GetKey())
	}
	if err != nil {
		return nil, readRPCError(err)
	}

	response := &kvstorepb.ReadResponse{
		Record: toRPCRecord(req.GetKey(), rec),
		Mode:   mode,
	}
	if mode == kvstorepb.ReadMode_READ_MODE_REVISION {
		response.RequestedRevision = req.GetRevision()
	}
	return response, nil
}

func normalizeReadMode(mode kvstorepb.ReadMode, target uint64) (kvstorepb.ReadMode, error) {
	if mode == kvstorepb.ReadMode_READ_MODE_UNSPECIFIED {
		mode = kvstorepb.ReadMode_READ_MODE_LINEARIZABLE
	}

	switch mode {
	case kvstorepb.ReadMode_READ_MODE_LOCAL, kvstorepb.ReadMode_READ_MODE_LINEARIZABLE:
		if target != 0 {
			return 0, status.Error(codes.InvalidArgument, "revision is valid only for READ_MODE_REVISION")
		}
	case kvstorepb.ReadMode_READ_MODE_REVISION:
		if target == 0 {
			return 0, status.Error(codes.InvalidArgument, "revision must be greater than zero for READ_MODE_REVISION")
		}
	default:
		return 0, status.Errorf(codes.InvalidArgument, "unsupported read mode %d", mode)
	}
	return mode, nil
}

func validateRPCKey(key string) error {
	if key == "" {
		return status.Error(codes.InvalidArgument, "key must not be empty")
	}
	if len([]byte(key)) > maxRPCKeyBytes {
		return status.Error(codes.InvalidArgument, "key exceeds 4096 bytes")
	}
	return nil
}

func toRPCRecord(key string, rec record) *kvstorepb.Record {
	return &kvstorepb.Record{
		Key:            key,
		Value:          rec.Value,
		CreateRevision: uint64(rec.CreateRevision),
		ModRevision:    uint64(rec.ModRevision),
		Version:        rec.Version,
		Deleted:        rec.Deleted,
	}
}

func readRPCError(err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, "key does not exist at the requested state")
	case errors.Is(err, ErrFutureRevision):
		return status.Error(codes.OutOfRange, "requested revision has not been applied on this node")
	case errors.Is(err, errTooManyInflightRequests):
		return status.Error(codes.ResourceExhausted, "too many concurrent read requests")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "read was canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "read deadline exceeded")
	default:
		log.Printf("RPC read failed: %v", err)
		return status.Error(codes.Unavailable, "read could not be completed")
	}
}

func writeRPCError(err error) error {
	switch {
	case errors.Is(err, errTooManyInflightRequests):
		return status.Error(codes.ResourceExhausted, "too many concurrent write requests")
	case errors.Is(err, ErrWriteOutcomeUnknown):
		return status.Error(codes.Unavailable, "write outcome is unknown")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "write was canceled before confirmation")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "write deadline exceeded before confirmation")
	default:
		log.Printf("RPC write failed: %v", err)
		return status.Error(codes.Unavailable, "write could not be confirmed")
	}
}

// rpcCluster 实现 kvstore.Cluster，并把成员变更交给 Raft 提案协程。
type rpcCluster struct {
	kvstorepb.UnimplementedClusterServer
	confChangeC chan<- *confChangeProposal
}

func (s *rpcCluster) AddMember(ctx context.Context, req *kvstorepb.AddMemberRequest) (*kvstorepb.MemberChangeResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := validateMemberID(req.GetMemberId()); err != nil {
		return nil, err
	}
	if err := validateRaftURL(req.GetRaftUrl()); err != nil {
		return nil, err
	}
	change := &raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode.Enum(),
		NodeId:  new(req.MemberId),
		Context: []byte(req.RaftUrl),
	}
	if err := s.propose(ctx, change); err != nil {
		return nil, err
	}
	return &kvstorepb.MemberChangeResponse{Accepted: true}, nil
}

func (s *rpcCluster) RemoveMember(ctx context.Context, req *kvstorepb.RemoveMemberRequest) (*kvstorepb.MemberChangeResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := validateMemberID(req.GetMemberId()); err != nil {
		return nil, err
	}
	change := &raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode.Enum(),
		NodeId: new(req.MemberId),
	}
	if err := s.propose(ctx, change); err != nil {
		return nil, err
	}
	return &kvstorepb.MemberChangeResponse{Accepted: true}, nil
}

func (s *rpcCluster) propose(ctx context.Context, change *raftpb.ConfChange) error {
	if s.confChangeC == nil {
		return status.Error(codes.Unavailable, "member change service is unavailable")
	}
	resultC := make(chan error, 1)
	proposal := &confChangeProposal{
		ctx:        ctx,
		confChange: change,
		resultC:    resultC,
	}
	select {
	case s.confChangeC <- proposal:
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}

	select {
	case err := <-resultC:
		if err != nil {
			log.Printf("RPC member change proposal failed: %v", err)
			return status.Error(codes.Unavailable, "member change was not accepted")
		}
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}

func validateMemberID(id uint64) error {
	if id == 0 {
		return status.Error(codes.InvalidArgument, "member_id must be greater than zero")
	}
	return nil
}

func validateRaftURL(address string) error {
	parsed, err := url.ParseRequestURI(address)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return status.Error(codes.InvalidArgument, "raft_url must be an absolute http or https URL")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return status.Error(codes.InvalidArgument, "raft_url must not contain credentials, query, or fragment")
	}
	return nil
}

func newRPCServer(kv rpcStore, confChangeC chan<- *confChangeProposal) (*grpc.Server, *health.Server) {
	server := grpc.NewServer(grpc.MaxRecvMsgSize(maxRPCRequestBytes))
	healthServer := health.NewServer()
	kvstorepb.RegisterKVStoreServer(server, &rpcKVStore{store: kv})
	kvstorepb.RegisterClusterServer(server, &rpcCluster{confChangeC: confChangeC})
	healthv1.RegisterHealthServer(server, healthServer)
	healthServer.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(kvstorepb.KVStore_ServiceDesc.ServiceName, healthv1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(kvstorepb.Cluster_ServiceDesc.ServiceName, healthv1.HealthCheckResponse_SERVING)
	reflection.Register(server)
	return server, healthServer
}

// serveRPCAPI 运行客户端 gRPC 服务，并统一处理 Raft、状态机和监听服务的生命周期。
func serveRPCAPI(
	kv *kvstore,
	port int,
	confChangeC chan<- *confChangeProposal,
	raftErrorC <-chan error,
	stateMachineErrorC <-chan error,
) error {
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		return fmt.Errorf("listen for key-value RPC API: %w", err)
	}

	server, healthServer := newRPCServer(kv, confChangeC)
	serverErrorC := make(chan error, 1)
	go func() {
		err := server.Serve(listener)
		if errors.Is(err, grpc.ErrServerStopped) {
			err = nil
		}
		serverErrorC <- err
	}()

	stop := func() {
		healthServer.Shutdown()
		doneC := make(chan struct{})
		go func() {
			server.GracefulStop()
			close(doneC)
		}()
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case <-doneC:
		case <-timer.C:
			server.Stop()
			<-doneC
		}
	}

	for {
		select {
		case err, ok := <-raftErrorC:
			stop()
			if !ok {
				return nil
			}
			if err == nil {
				err = errors.New("raft node stopped with a nil error")
			}
			return fmt.Errorf("raft node stopped: %w", err)
		case err, ok := <-stateMachineErrorC:
			if !ok {
				stateMachineErrorC = nil
				continue
			}
			stop()
			if err == nil {
				err = errors.New("state machine stopped with a nil error")
			}
			return fmt.Errorf("state machine stopped: %w", err)
		case err := <-serverErrorC:
			if err != nil {
				return fmt.Errorf("serve key-value RPC API: %w", err)
			}
			return nil
		}
	}
}
