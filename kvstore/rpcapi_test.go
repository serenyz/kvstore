package kvstore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	kvstorepb "kvstore/pb"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type stubRPCStore struct {
	put              func(context.Context, string, string) (record, error)
	delete           func(context.Context, string) (record, error)
	lookupLocal      func(string) (record, error)
	lookupAtRevision func(string, revision) (record, error)
	lookupLinearize  func(context.Context, string) (record, error)
}

func (s *stubRPCStore) Put(ctx context.Context, key, value string) (record, error) {
	if s.put == nil {
		return record{}, errors.New("unexpected Put call")
	}
	return s.put(ctx, key, value)
}

func (s *stubRPCStore) Delete(ctx context.Context, key string) (record, error) {
	if s.delete == nil {
		return record{}, errors.New("unexpected Delete call")
	}
	return s.delete(ctx, key)
}

func (s *stubRPCStore) LookupLocal(key string) (record, error) {
	if s.lookupLocal == nil {
		return record{}, errors.New("unexpected LookupLocal call")
	}
	return s.lookupLocal(key)
}

func (s *stubRPCStore) LookupAtRevision(key string, target revision) (record, error) {
	if s.lookupAtRevision == nil {
		return record{}, errors.New("unexpected LookupAtRevision call")
	}
	return s.lookupAtRevision(key, target)
}

func (s *stubRPCStore) LookupLinearize(ctx context.Context, key string) (record, error) {
	if s.lookupLinearize == nil {
		return record{}, errors.New("unexpected LookupLinearize call")
	}
	return s.lookupLinearize(ctx, key)
}

func TestRPCKVStoreReadModes(t *testing.T) {
	want := record{
		Value:          "alice",
		CreateRevision: 2,
		ModRevision:    8,
		Version:        3,
	}
	tests := []struct {
		name             string
		request          *kvstorepb.ReadRequest
		wantMode         kvstorepb.ReadMode
		wantRevision     revision
		wantRequestedRev uint64
	}{
		{
			name:     "unspecified defaults to linearizable",
			request:  &kvstorepb.ReadRequest{Key: "customer/42"},
			wantMode: kvstorepb.ReadMode_READ_MODE_LINEARIZABLE,
		},
		{
			name:     "local",
			request:  &kvstorepb.ReadRequest{Key: "customer/42", Mode: kvstorepb.ReadMode_READ_MODE_LOCAL},
			wantMode: kvstorepb.ReadMode_READ_MODE_LOCAL,
		},
		{
			name: "revision",
			request: &kvstorepb.ReadRequest{
				Key:      "customer/42",
				Mode:     kvstorepb.ReadMode_READ_MODE_REVISION,
				Revision: 7,
			},
			wantMode:         kvstorepb.ReadMode_READ_MODE_REVISION,
			wantRevision:     7,
			wantRequestedRev: 7,
		},
		{
			name:     "explicit linearizable",
			request:  &kvstorepb.ReadRequest{Key: "customer/42", Mode: kvstorepb.ReadMode_READ_MODE_LINEARIZABLE},
			wantMode: kvstorepb.ReadMode_READ_MODE_LINEARIZABLE,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var called kvstorepb.ReadMode
			store := &stubRPCStore{
				lookupLocal: func(key string) (record, error) {
					called = kvstorepb.ReadMode_READ_MODE_LOCAL
					if key != tt.request.Key {
						t.Fatalf("LookupLocal() key = %q, want %q", key, tt.request.Key)
					}
					return want, nil
				},
				lookupAtRevision: func(key string, target revision) (record, error) {
					called = kvstorepb.ReadMode_READ_MODE_REVISION
					if key != tt.request.Key || target != tt.wantRevision {
						t.Fatalf("LookupAtRevision() = (%q, %d), want (%q, %d)", key, target, tt.request.Key, tt.wantRevision)
					}
					return want, nil
				},
				lookupLinearize: func(ctx context.Context, key string) (record, error) {
					called = kvstorepb.ReadMode_READ_MODE_LINEARIZABLE
					if key != tt.request.Key {
						t.Fatalf("LookupLinearize() key = %q, want %q", key, tt.request.Key)
					}
					if _, ok := ctx.Deadline(); !ok {
						t.Fatal("LookupLinearize() context has no deadline")
					}
					return want, nil
				},
			}
			server := &rpcKVStore{store: store}
			response, err := server.Read(context.Background(), tt.request)
			if err != nil {
				t.Fatalf("Read() error = %v", err)
			}
			if called != tt.wantMode || response.GetMode() != tt.wantMode {
				t.Fatalf("read mode = called %s, response %s; want %s", called, response.GetMode(), tt.wantMode)
			}
			if response.GetRequestedRevision() != tt.wantRequestedRev {
				t.Fatalf("requested revision = %d, want %d", response.GetRequestedRevision(), tt.wantRequestedRev)
			}
			got := response.GetRecord()
			if got.GetKey() != tt.request.Key || got.GetValue() != want.Value || got.GetModRevision() != uint64(want.ModRevision) {
				t.Fatalf("Read() record = %#v", got)
			}
		})
	}
}

func TestRPCKVStoreRejectsInvalidReads(t *testing.T) {
	tests := []struct {
		name    string
		request *kvstorepb.ReadRequest
	}{
		{name: "nil request"},
		{name: "empty key", request: &kvstorepb.ReadRequest{Mode: kvstorepb.ReadMode_READ_MODE_LOCAL}},
		{name: "local with revision", request: &kvstorepb.ReadRequest{Key: "key", Mode: kvstorepb.ReadMode_READ_MODE_LOCAL, Revision: 2}},
		{name: "linearizable with revision", request: &kvstorepb.ReadRequest{Key: "key", Mode: kvstorepb.ReadMode_READ_MODE_LINEARIZABLE, Revision: 2}},
		{name: "revision without target", request: &kvstorepb.ReadRequest{Key: "key", Mode: kvstorepb.ReadMode_READ_MODE_REVISION}},
		{name: "unknown mode", request: &kvstorepb.ReadRequest{Key: "key", Mode: kvstorepb.ReadMode(99)}},
		{name: "oversized key", request: &kvstorepb.ReadRequest{Key: strings.Repeat("k", maxRPCKeyBytes+1)}},
	}

	server := &rpcKVStore{store: &stubRPCStore{}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := server.Read(context.Background(), tt.request)
			assertRPCCode(t, err, codes.InvalidArgument)
		})
	}
}

func TestRPCKVStoreReadErrors(t *testing.T) {
	t.Run("missing key", func(t *testing.T) {
		server := &rpcKVStore{store: &stubRPCStore{
			lookupLocal: func(string) (record, error) { return record{}, ErrNotFound },
		}}
		_, err := server.Read(context.Background(), &kvstorepb.ReadRequest{
			Key:  "key",
			Mode: kvstorepb.ReadMode_READ_MODE_LOCAL,
		})
		assertRPCCode(t, err, codes.NotFound)
	})

	t.Run("future revision", func(t *testing.T) {
		server := &rpcKVStore{store: &stubRPCStore{
			lookupAtRevision: func(string, revision) (record, error) { return record{}, ErrFutureRevision },
		}}
		_, err := server.Read(context.Background(), &kvstorepb.ReadRequest{
			Key:      "key",
			Mode:     kvstorepb.ReadMode_READ_MODE_REVISION,
			Revision: 99,
		})
		assertRPCCode(t, err, codes.OutOfRange)
	})

	t.Run("linearizable timeout", func(t *testing.T) {
		server := &rpcKVStore{
			store: &stubRPCStore{
				lookupLinearize: func(ctx context.Context, _ string) (record, error) {
					<-ctx.Done()
					return record{}, ctx.Err()
				},
			},
			linearizableReadTimeout: 5 * time.Millisecond,
		}
		_, err := server.Read(context.Background(), &kvstorepb.ReadRequest{Key: "key"})
		assertRPCCode(t, err, codes.DeadlineExceeded)
	})
}

func TestRPCKVStoreWrites(t *testing.T) {
	t.Run("put returns applied revision", func(t *testing.T) {
		want := record{Value: "dark", CreateRevision: 3, ModRevision: 3, Version: 1}
		server := &rpcKVStore{store: &stubRPCStore{
			put: func(_ context.Context, key, value string) (record, error) {
				if key != "theme" || value != "dark" {
					t.Fatalf("Put() = (%q, %q), want (theme, dark)", key, value)
				}
				return want, nil
			},
		}}
		response, err := server.Put(context.Background(), &kvstorepb.PutRequest{Key: "theme", Value: "dark"})
		if err != nil {
			t.Fatalf("Put() error = %v", err)
		}
		if response.GetRecord().GetModRevision() != 3 || response.GetRecord().GetValue() != "dark" {
			t.Fatalf("Put() response = %#v", response)
		}
	})

	t.Run("missing delete has no record", func(t *testing.T) {
		server := &rpcKVStore{store: &stubRPCStore{
			delete: func(context.Context, string) (record, error) { return record{}, nil },
		}}
		response, err := server.Delete(context.Background(), &kvstorepb.DeleteRequest{Key: "missing"})
		if err != nil {
			t.Fatalf("Delete() error = %v", err)
		}
		if response.GetRecord() != nil {
			t.Fatalf("Delete() record = %#v, want nil", response.GetRecord())
		}
	})

	t.Run("unknown write outcome is unavailable", func(t *testing.T) {
		server := &rpcKVStore{store: &stubRPCStore{
			put: func(context.Context, string, string) (record, error) {
				return record{}, fmt.Errorf("%w: %w", ErrWriteOutcomeUnknown, context.DeadlineExceeded)
			},
		}}
		_, err := server.Put(context.Background(), &kvstorepb.PutRequest{Key: "key", Value: "value"})
		assertRPCCode(t, err, codes.Unavailable)
	})
}

func TestRPCClusterAddMember(t *testing.T) {
	confChangeC := make(chan *confChangeProposal)
	go func() {
		proposal := <-confChangeC
		change := proposal.confChange
		if change.GetNodeId() != 3 || change.GetType() != raftpb.ConfChangeAddNode {
			proposal.resultC <- errors.New("unexpected member change")
			return
		}
		if string(change.Context) != "http://127.0.0.1:9023" {
			proposal.resultC <- errors.New("unexpected member URL")
			return
		}
		proposal.resultC <- nil
	}()

	server := &rpcCluster{confChangeC: confChangeC}
	response, err := server.AddMember(context.Background(), &kvstorepb.AddMemberRequest{
		MemberId: 3,
		RaftUrl:  "http://127.0.0.1:9023",
	})
	if err != nil {
		t.Fatalf("AddMember() error = %v", err)
	}
	if !response.GetAccepted() {
		t.Fatal("AddMember() accepted = false, want true")
	}
}

func TestRPCServerRoundTrip(t *testing.T) {
	want := record{Value: "value", CreateRevision: 2, ModRevision: 4, Version: 2}
	store := &stubRPCStore{
		lookupLocal: func(key string) (record, error) {
			if key != "key" {
				return record{}, fmt.Errorf("LookupLocal() key = %q, want key", key)
			}
			return want, nil
		},
	}

	listener := bufconn.Listen(1 << 20)
	server, healthServer := newRPCServer(store, nil)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("Serve() error = %v", err)
		}
	}()
	t.Cleanup(func() {
		healthServer.Shutdown()
		server.Stop()
		_ = listener.Close()
	})

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("create gRPC client: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := kvstorepb.NewKVStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := client.Read(ctx, &kvstorepb.ReadRequest{
		Key:  "key",
		Mode: kvstorepb.ReadMode_READ_MODE_LOCAL,
	})
	if err != nil {
		t.Fatalf("Read() round trip error = %v", err)
	}
	if response.GetRecord().GetValue() != "value" || response.GetMode() != kvstorepb.ReadMode_READ_MODE_LOCAL {
		t.Fatalf("Read() round trip response = %#v", response)
	}
}

func assertRPCCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("status code = %s, want %s; error %v", got, want, err)
	}
}
