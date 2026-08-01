package kvstore

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

func TestKVStoreApplyAndLookupHistory(t *testing.T) {
	store := newBareKVStore()

	if _, err := store.LookupLocal("key"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("LookupLocal() missing key error = %v, want %v", err, ErrNotFound)
	}

	first := store.applyPut("key", "")
	assertRecord(t, first, record{
		Value:          "",
		CreateRevision: 2,
		ModRevision:    2,
		Version:        1,
	})
	if got, err := store.LookupLocal("key"); err != nil {
		t.Fatalf("LookupLocal() empty value error = %v", err)
	} else {
		assertRecord(t, got, first)
	}

	store.applyPut("other", "value") // Advances the global revision to 3.
	second := store.applyPut("key", "v2")
	assertRecord(t, second, record{
		Value:          "v2",
		CreateRevision: 2,
		ModRevision:    4,
		Version:        2,
	})

	if got := store.applyDelete("missing"); got != (record{}) {
		t.Fatalf("applyDelete() missing key = %#v, want zero record", got)
	}
	if store.curRevision != 4 {
		t.Fatalf("revision after deleting a missing key = %d, want 4", store.curRevision)
	}

	tombstone := store.applyDelete("key")
	assertRecord(t, tombstone, record{
		CreateRevision: 2,
		ModRevision:    5,
		Version:        3,
		Deleted:        true,
	})
	if _, err := store.LookupLocal("key"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("LookupLocal() deleted key error = %v, want %v", err, ErrNotFound)
	}

	if got := store.applyDelete("key"); got != tombstone {
		t.Fatalf("repeated applyDelete() = %#v, want existing tombstone %#v", got, tombstone)
	}
	if store.curRevision != 5 || len(store.kvStore["key"]) != 3 {
		t.Fatalf("repeated delete changed state: revision %d, history length %d", store.curRevision, len(store.kvStore["key"]))
	}

	third := store.applyPut("key", "v3")
	assertRecord(t, third, record{
		Value:          "v3",
		CreateRevision: 2,
		ModRevision:    6,
		Version:        4,
	})

	tests := []struct {
		name    string
		target  revision
		want    record
		wantErr error
	}{
		{name: "before creation", target: 1, wantErr: ErrNotFound},
		{name: "at creation", target: 2, want: first},
		{name: "unrelated revision keeps prior value", target: 3, want: first},
		{name: "at update", target: 4, want: second},
		{name: "at deletion", target: 5, wantErr: ErrNotFound},
		{name: "at recreation", target: 6, want: third},
		{name: "none revision uses current", target: NoneRevision, want: third},
		{name: "future revision", target: 7, wantErr: ErrFutureRevision},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.LookupAtRevision("key", tt.target)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("LookupAtRevision(%d) error = %v, want %v", tt.target, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("LookupAtRevision(%d) error = %v", tt.target, err)
			}
			assertRecord(t, got, tt.want)
		})
	}

	if _, err := store.LookupAtRevision("unknown", 2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("LookupAtRevision() unknown key error = %v, want %v", err, ErrNotFound)
	}
	if err := store.apply(command{CommandType: CommandType(99)}); err == nil || !strings.Contains(err.Error(), "unknown command type 99") {
		t.Fatalf("apply() unknown command error = %v", err)
	}
}

func TestKVStoreApplyDeliversProposalResults(t *testing.T) {
	store := newBareKVStore()
	putResultC, err := store.proposalInflight.register("put-request")
	if err != nil {
		t.Fatalf("register put request: %v", err)
	}
	if err := store.apply(command{
		CommandType: CommandPut,
		RequestID:   "put-request",
		Key:         "key",
		Val:         "value",
	}); err != nil {
		t.Fatalf("apply put: %v", err)
	}
	putResult := receiveApplyResult(t, putResultC)
	assertRecord(t, putResult.record, record{
		Value:          "value",
		CreateRevision: 2,
		ModRevision:    2,
		Version:        1,
	})
	if putResult.err != nil {
		t.Fatalf("put apply result error = %v", putResult.err)
	}

	deleteResultC, err := store.proposalInflight.register("delete-request")
	if err != nil {
		t.Fatalf("register delete request: %v", err)
	}
	if err := store.apply(command{
		CommandType: CommandDelete,
		RequestID:   "delete-request",
		Key:         "key",
	}); err != nil {
		t.Fatalf("apply delete: %v", err)
	}
	deleteResult := receiveApplyResult(t, deleteResultC)
	if !deleteResult.record.Deleted || deleteResult.record.ModRevision != 3 {
		t.Fatalf("delete apply result = %#v, want tombstone at revision 3", deleteResult.record)
	}
	if len(store.proposalInflight.chs) != 0 {
		t.Fatalf("proposal inflight count after apply = %d, want 0", len(store.proposalInflight.chs))
	}
}

func TestKVStoreSnapshotRoundTrip(t *testing.T) {
	source := newBareKVStore()
	first := source.applyPut("key", "v1")
	source.applyPut("other", "value")
	source.applyPut("key", "v2")
	source.applyDelete("other")

	data, err := source.getSnapshot()
	if err != nil {
		t.Fatalf("getSnapshot() error = %v", err)
	}
	var encoded kvSnap
	if err := json.Unmarshal(data, &encoded); err != nil {
		t.Fatalf("decode generated snapshot: %v", err)
	}
	if encoded.CurRevision != source.curRevision || len(encoded.KvStore["key"]) != 2 {
		t.Fatalf("generated snapshot = revision %d, key history %d; want revision %d, history 2", encoded.CurRevision, len(encoded.KvStore["key"]), source.curRevision)
	}

	restored := newBareKVStore()
	restored.applyPut("stale", "remove-me")
	if err := restored.recoverFromSnapshot(data); err != nil {
		t.Fatalf("recoverFromSnapshot() error = %v", err)
	}
	if restored.curRevision != source.curRevision {
		t.Fatalf("restored revision = %d, want %d", restored.curRevision, source.curRevision)
	}
	if _, ok := restored.kvStore["stale"]; ok {
		t.Fatal("recoverFromSnapshot() merged rather than replaced existing state")
	}
	if got, err := restored.LookupAtRevision("key", first.ModRevision); err != nil {
		t.Fatalf("lookup restored historical value: %v", err)
	} else {
		assertRecord(t, got, first)
	}
	if _, err := restored.LookupLocal("other"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("lookup restored tombstone error = %v, want %v", err, ErrNotFound)
	}

	beforeRevision := restored.curRevision
	before, err := restored.LookupLocal("key")
	if err != nil {
		t.Fatalf("lookup before invalid recovery: %v", err)
	}
	if err := restored.recoverFromSnapshot([]byte("not-json")); err == nil {
		t.Fatal("recoverFromSnapshot() accepted invalid JSON")
	}
	after, err := restored.LookupLocal("key")
	if err != nil {
		t.Fatalf("lookup after invalid recovery: %v", err)
	}
	if after != before || restored.curRevision != beforeRevision {
		t.Fatal("invalid snapshot changed the existing state")
	}
}

func TestNewKVStoreRestoresSnapshot(t *testing.T) {
	t.Run("starts empty without a snapshot", func(t *testing.T) {
		snapshotter := newKVTestSnapshotter(t)
		commitC := make(chan *commit)
		readStateC := make(chan []raft.ReadState)
		close(commitC)
		close(readStateC)

		store, applyErrorC, err := newKVStore(
			snapshotter,
			make(chan *proposal),
			make(chan *readIndexRequest),
			commitC,
			readStateC,
		)
		if err != nil {
			t.Fatalf("newKVStore() error = %v", err)
		}
		if store == nil || applyErrorC == nil {
			t.Fatal("newKVStore() returned nil state or error channel")
		}
		if store.curRevision != 1 || len(store.kvStore) != 0 {
			t.Fatalf("new store state = revision %d, keys %d; want revision 1, keys 0", store.curRevision, len(store.kvStore))
		}
		if store.readIndexInflight.appliedIndex != 0 {
			t.Fatalf("new store applied index = %d, want 0", store.readIndexInflight.appliedIndex)
		}
	})

	t.Run("restores data and applied index", func(t *testing.T) {
		snapshotter := newKVTestSnapshotter(t)
		wantSnapshot := kvSnap{
			CurRevision: 5,
			KvStore: map[string]value{
				"key": {{
					Value:          "value",
					CreateRevision: 2,
					ModRevision:    5,
					Version:        2,
				}},
			},
		}
		data, err := json.Marshal(wantSnapshot)
		if err != nil {
			t.Fatalf("marshal test snapshot: %v", err)
		}
		saveKVTestSnapshot(t, snapshotter, 8, 3, data)
		commitC := make(chan *commit)
		readStateC := make(chan []raft.ReadState)
		close(commitC)
		close(readStateC)

		store, _, err := newKVStore(
			snapshotter,
			make(chan *proposal),
			make(chan *readIndexRequest),
			commitC,
			readStateC,
		)
		if err != nil {
			t.Fatalf("newKVStore() error = %v", err)
		}
		if store.curRevision != 5 || store.readIndexInflight.appliedIndex != 8 {
			t.Fatalf("restored progress = revision %d, applied index %d; want 5 and 8", store.curRevision, store.readIndexInflight.appliedIndex)
		}
		got, err := store.LookupLocal("key")
		if err != nil {
			t.Fatalf("LookupLocal() restored key error = %v", err)
		}
		assertRecord(t, got, *wantSnapshot.KvStore["key"][0])
	})

	t.Run("rejects invalid snapshot data", func(t *testing.T) {
		snapshotter := newKVTestSnapshotter(t)
		saveKVTestSnapshot(t, snapshotter, 2, 1, []byte("invalid-json"))

		store, applyErrorC, err := newKVStore(
			snapshotter,
			make(chan *proposal),
			make(chan *readIndexRequest),
			make(chan *commit),
			make(chan []raft.ReadState),
		)
		if err == nil || !strings.Contains(err.Error(), "restore state machine snapshot") {
			t.Fatalf("newKVStore() error = %v, want snapshot restore error", err)
		}
		if store != nil || applyErrorC != nil {
			t.Fatal("newKVStore() returned initialized values after snapshot failure")
		}
	})
}

func TestKVStoreReadCommits(t *testing.T) {
	t.Run("applies commands in order and advances progress", func(t *testing.T) {
		store := newBareKVStore()
		doneC := make(chan struct{})
		commitC := make(chan *commit, 1)
		commitC <- &commit{
			data: []string{
				encodeKVTestCommand(t, command{CommandType: CommandPut, Key: "key", Val: "v1"}),
				encodeKVTestCommand(t, command{CommandType: CommandPut, Key: "key", Val: "v2"}),
				encodeKVTestCommand(t, command{CommandType: CommandDelete, Key: "key"}),
			},
			applyDoneC:   doneC,
			raftLogIndex: 9,
		}
		close(commitC)

		if err := store.readCommits(commitC); err != nil {
			t.Fatalf("readCommits() error = %v", err)
		}
		assertClosed(t, doneC, "commit completion")
		if store.readIndexInflight.appliedIndex != 9 {
			t.Fatalf("applied index = %d, want 9", store.readIndexInflight.appliedIndex)
		}
		if store.curRevision != 4 || len(store.kvStore["key"]) != 3 {
			t.Fatalf("applied state = revision %d, history %d; want revision 4, history 3", store.curRevision, len(store.kvStore["key"]))
		}
		if _, err := store.LookupLocal("key"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("LookupLocal() after committed delete error = %v, want %v", err, ErrNotFound)
		}
	})

	t.Run("progress-only commit closes completion channel", func(t *testing.T) {
		store := newBareKVStore()
		doneC := make(chan struct{})
		commitC := make(chan *commit, 1)
		commitC <- &commit{applyDoneC: doneC, raftLogIndex: 5}
		close(commitC)

		if err := store.readCommits(commitC); err != nil {
			t.Fatalf("readCommits() error = %v", err)
		}
		assertClosed(t, doneC, "progress-only commit completion")
		if store.readIndexInflight.appliedIndex != 5 || store.curRevision != 1 {
			t.Fatalf("progress-only state = applied %d, revision %d; want 5 and 1", store.readIndexInflight.appliedIndex, store.curRevision)
		}
	})

	t.Run("malformed command stops without acknowledging the batch", func(t *testing.T) {
		store := newBareKVStore()
		doneC := make(chan struct{})
		commitC := make(chan *commit, 1)
		commitC <- &commit{data: []string{"not-gob"}, applyDoneC: doneC, raftLogIndex: 4}
		close(commitC)

		err := store.readCommits(commitC)
		if err == nil || !strings.Contains(err.Error(), "decode committed state machine command") {
			t.Fatalf("readCommits() error = %v, want decoding error", err)
		}
		assertOpen(t, doneC, "failed commit completion")
		if store.readIndexInflight.appliedIndex != 0 {
			t.Fatalf("applied index after malformed command = %d, want 0", store.readIndexInflight.appliedIndex)
		}
	})

	t.Run("reloads a published snapshot", func(t *testing.T) {
		snapshotter := newKVTestSnapshotter(t)
		data, err := json.Marshal(kvSnap{
			CurRevision: 7,
			KvStore: map[string]value{
				"restored": {{Value: "yes", CreateRevision: 7, ModRevision: 7, Version: 1}},
			},
		})
		if err != nil {
			t.Fatalf("marshal snapshot: %v", err)
		}
		saveKVTestSnapshot(t, snapshotter, 11, 4, data)
		store := newBareKVStore()
		store.snapshotter = snapshotter
		store.applyPut("stale", "value")
		commitC := make(chan *commit, 1)
		commitC <- nil
		close(commitC)

		if err := store.readCommits(commitC); err != nil {
			t.Fatalf("readCommits() snapshot reload error = %v", err)
		}
		if store.curRevision != 7 || store.readIndexInflight.appliedIndex != 11 {
			t.Fatalf("reloaded progress = revision %d, applied %d; want 7 and 11", store.curRevision, store.readIndexInflight.appliedIndex)
		}
		if _, ok := store.kvStore["stale"]; ok {
			t.Fatal("snapshot reload kept stale state")
		}
		if got, err := store.LookupLocal("restored"); err != nil || got.Value != "yes" {
			t.Fatalf("LookupLocal() restored value = %#v, error %v", got, err)
		}
	})
}

func TestKVStorePutAndDelete(t *testing.T) {
	t.Run("round trips proposals through apply", func(t *testing.T) {
		proposeC := make(chan *proposal)
		store := newBareKVStore()
		store.proposeC = proposeC
		commandsC := make(chan []command, 1)
		serverErrC := make(chan error, 1)
		go func() {
			commands := make([]command, 0, 2)
			for range 2 {
				prop := <-proposeC
				cmd, err := decodeKVTestCommand(prop.data)
				if err != nil {
					prop.resultC <- err
					serverErrC <- err
					return
				}
				commands = append(commands, cmd)
				prop.resultC <- nil
				if err := store.apply(cmd); err != nil {
					serverErrC <- err
					return
				}
			}
			commandsC <- commands
			serverErrC <- nil
		}()

		putRecord, err := store.Put(context.Background(), "key", "value")
		if err != nil {
			t.Fatalf("Put() error = %v", err)
		}
		assertRecord(t, putRecord, record{Value: "value", CreateRevision: 2, ModRevision: 2, Version: 1})
		deleteRecord, err := store.Delete(context.Background(), "key")
		if err != nil {
			t.Fatalf("Delete() error = %v", err)
		}
		if !deleteRecord.Deleted || deleteRecord.ModRevision != 3 || deleteRecord.Version != 2 {
			t.Fatalf("Delete() record = %#v, want version 2 tombstone at revision 3", deleteRecord)
		}
		if err := receiveError(t, serverErrC); err != nil {
			t.Fatalf("proposal server error = %v", err)
		}
		commands := receiveValue(t, commandsC)
		if len(commands) != 2 || commands[0].CommandType != CommandPut || commands[1].CommandType != CommandDelete {
			t.Fatalf("decoded proposal commands = %#v", commands)
		}
		for i, cmd := range commands {
			if len(cmd.RequestID) != 32 {
				t.Errorf("command %d request ID length = %d, want 32", i, len(cmd.RequestID))
			}
		}
		if len(store.proposalInflight.chs) != 0 {
			t.Fatalf("proposal inflight count = %d, want 0", len(store.proposalInflight.chs))
		}
	})

	t.Run("returns proposal errors and cleans registration", func(t *testing.T) {
		proposeC := make(chan *proposal)
		store := newBareKVStore()
		store.proposeC = proposeC
		wantErr := errors.New("not leader")
		go func() {
			prop := <-proposeC
			prop.resultC <- wantErr
		}()

		_, err := store.Put(context.Background(), "key", "value")
		if !errors.Is(err, wantErr) || !strings.Contains(err.Error(), "propose command") {
			t.Fatalf("Put() error = %v, want wrapped proposal error", err)
		}
		if len(store.proposalInflight.chs) != 0 {
			t.Fatalf("proposal inflight count after error = %d, want 0", len(store.proposalInflight.chs))
		}
	})

	t.Run("cancellation before send has a known outcome", func(t *testing.T) {
		store := newBareKVStore()
		store.proposeC = make(chan *proposal)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := store.Put(ctx, "key", "value")
		if !errors.Is(err, context.Canceled) || errors.Is(err, ErrWriteOutcomeUnknown) {
			t.Fatalf("Put() error = %v, want context cancellation without unknown outcome", err)
		}
		if len(store.proposalInflight.chs) != 0 {
			t.Fatalf("proposal inflight count after cancellation = %d, want 0", len(store.proposalInflight.chs))
		}
	})

	t.Run("cancellation after send reports an unknown outcome", func(t *testing.T) {
		proposeC := make(chan *proposal)
		store := newBareKVStore()
		store.proposeC = proposeC
		ctx, cancel := context.WithCancel(context.Background())
		acceptedC := make(chan struct{})
		go func() {
			<-proposeC
			close(acceptedC)
		}()
		resultC := make(chan kvTestOperationResult, 1)
		go func() {
			rec, err := store.Put(ctx, "key", "value")
			resultC <- kvTestOperationResult{record: rec, err: err}
		}()

		assertClosedEventually(t, acceptedC, "proposal acceptance")
		cancel()
		result := receiveValue(t, resultC)
		if !errors.Is(result.err, ErrWriteOutcomeUnknown) || !errors.Is(result.err, context.Canceled) {
			t.Fatalf("Put() error = %v, want unknown outcome and context cancellation", result.err)
		}
		if len(store.proposalInflight.chs) != 0 {
			t.Fatalf("proposal inflight count after cancellation = %d, want 0", len(store.proposalInflight.chs))
		}
	})

	t.Run("cancellation while waiting for apply reports an unknown outcome", func(t *testing.T) {
		proposeC := make(chan *proposal)
		store := newBareKVStore()
		store.proposeC = proposeC
		ctx, cancel := context.WithCancel(context.Background())
		acceptedC := make(chan struct{})
		go func() {
			prop := <-proposeC
			prop.resultC <- nil
			close(acceptedC)
		}()
		resultC := make(chan kvTestOperationResult, 1)
		go func() {
			rec, err := store.Put(ctx, "key", "value")
			resultC <- kvTestOperationResult{record: rec, err: err}
		}()

		assertClosedEventually(t, acceptedC, "proposal acknowledgement")
		cancel()
		result := receiveValue(t, resultC)
		if !errors.Is(result.err, ErrWriteOutcomeUnknown) || !errors.Is(result.err, context.Canceled) {
			t.Fatalf("Put() error = %v, want unknown outcome and context cancellation", result.err)
		}
	})

	t.Run("rejects registration above capacity", func(t *testing.T) {
		store := newBareKVStore()
		store.proposeC = make(chan *proposal)
		store.proposalInflight = newProposalInflight(0)

		_, err := store.Put(context.Background(), "key", "value")
		if !errors.Is(err, errTooManyInflightRequests) || !strings.Contains(err.Error(), "register inflight request") {
			t.Fatalf("Put() error = %v, want inflight capacity error", err)
		}
	})
}

func TestKVStoreLookupLinearize(t *testing.T) {
	t.Run("waits for read index and applied progress", func(t *testing.T) {
		readIndexC := make(chan *readIndexRequest)
		store := newBareKVStore()
		store.readIndexC = readIndexC
		want := store.applyPut("key", "value")
		go func() {
			req := <-readIndexC
			req.resultC <- nil
			store.readIndexInflight.advanceApplied(7)
			store.readIndexInflight.updateReadIndex(string(req.requestCtx), 7)
		}()

		got, err := store.LookupLinearize(context.Background(), "key")
		if err != nil {
			t.Fatalf("LookupLinearize() error = %v", err)
		}
		assertRecord(t, got, want)
		if len(store.readIndexInflight.chs) != 0 {
			t.Fatalf("read inflight count after success = %d, want 0", len(store.readIndexInflight.chs))
		}
	})

	t.Run("returns ReadIndex request errors", func(t *testing.T) {
		readIndexC := make(chan *readIndexRequest)
		store := newBareKVStore()
		store.readIndexC = readIndexC
		wantErr := errors.New("read index unavailable")
		go func() {
			req := <-readIndexC
			req.resultC <- wantErr
		}()

		_, err := store.LookupLinearize(context.Background(), "key")
		if !errors.Is(err, wantErr) || !strings.Contains(err.Error(), "read index request") {
			t.Fatalf("LookupLinearize() error = %v, want wrapped ReadIndex error", err)
		}
		if len(store.readIndexInflight.chs) != 0 {
			t.Fatalf("read inflight count after request error = %d, want 0", len(store.readIndexInflight.chs))
		}
	})

	t.Run("cancellation before request send cleans registration", func(t *testing.T) {
		store := newBareKVStore()
		store.readIndexC = make(chan *readIndexRequest)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := store.LookupLinearize(ctx, "key")
		if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "read index request") {
			t.Fatalf("LookupLinearize() error = %v, want wrapped cancellation", err)
		}
		if len(store.readIndexInflight.chs) != 0 {
			t.Fatalf("read inflight count after cancellation = %d, want 0", len(store.readIndexInflight.chs))
		}
	})

	t.Run("cancellation while waiting for applied progress", func(t *testing.T) {
		readIndexC := make(chan *readIndexRequest)
		store := newBareKVStore()
		store.readIndexC = readIndexC
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		go func() {
			req := <-readIndexC
			req.resultC <- nil
		}()

		_, err := store.LookupLinearize(ctx, "key")
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "read linearizability timeout") {
			t.Fatalf("LookupLinearize() error = %v, want linearizability timeout", err)
		}
		if len(store.readIndexInflight.chs) != 0 {
			t.Fatalf("read inflight count after timeout = %d, want 0", len(store.readIndexInflight.chs))
		}
	})

	t.Run("rejects registration above capacity", func(t *testing.T) {
		store := newBareKVStore()
		store.readIndexC = make(chan *readIndexRequest)
		store.readIndexInflight = newReadIndexInflight(0)

		_, err := store.LookupLinearize(context.Background(), "key")
		if !errors.Is(err, errTooManyInflightRequests) || !strings.Contains(err.Error(), "register inflight request") {
			t.Fatalf("LookupLinearize() error = %v, want inflight capacity error", err)
		}
	})
}

func TestKVStoreReadStates(t *testing.T) {
	store := newBareKVStore()
	signalC, err := store.readIndexInflight.register("request-1")
	if err != nil {
		t.Fatalf("register read request: %v", err)
	}
	store.readIndexInflight.advanceApplied(4)
	readStateC := make(chan []raft.ReadState, 1)
	readStateC <- []raft.ReadState{
		{Index: 4, RequestCtx: []byte("request-1")},
		{Index: 2, RequestCtx: []byte("unknown")},
	}
	close(readStateC)

	store.readStates(readStateC)
	assertSignaled(t, signalC, "ReadState at the applied index")
	if len(store.readIndexInflight.chs) != 0 {
		t.Fatalf("read inflight count = %d, want 0", len(store.readIndexInflight.chs))
	}
}

type kvTestOperationResult struct {
	record record
	err    error
}

func newBareKVStore() *kvstore {
	return &kvstore{
		kvStore:           make(map[string]value),
		curRevision:       1,
		proposalInflight:  newProposalInflight(defaultMaxInflight),
		readIndexInflight: newReadIndexInflight(defaultMaxInflight),
	}
}

func newKVTestSnapshotter(t *testing.T) *snap.Snapshotter {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "snap")
	if err := os.Mkdir(dir, 0o750); err != nil {
		t.Fatalf("create snapshot directory: %v", err)
	}
	return snap.New(zap.NewNop(), dir)
}

func saveKVTestSnapshot(t *testing.T, snapshotter *snap.Snapshotter, index, term uint64, data []byte) {
	t.Helper()
	snapshot := &raftpb.Snapshot{
		Data: data,
		Metadata: &raftpb.SnapshotMetadata{
			Index: &index,
			Term:  &term,
			ConfState: &raftpb.ConfState{
				Voters: []uint64{1},
			},
		},
	}
	if err := snapshotter.SaveSnap(snapshot); err != nil {
		t.Fatalf("save test snapshot: %v", err)
	}
}

func encodeKVTestCommand(t *testing.T, cmd command) string {
	t.Helper()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		t.Fatalf("encode command: %v", err)
	}
	return buf.String()
}

func decodeKVTestCommand(data string) (command, error) {
	var cmd command
	err := gob.NewDecoder(bytes.NewBufferString(data)).Decode(&cmd)
	return cmd, err
}

func assertRecord(t *testing.T, got, want record) {
	t.Helper()
	if got != want {
		t.Fatalf("record = %#v, want %#v", got, want)
	}
}

func receiveApplyResult(t *testing.T, resultC <-chan *applyResult) *applyResult {
	t.Helper()
	select {
	case result := <-resultC:
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for apply result")
		return nil
	}
}

func receiveError(t *testing.T, errorC <-chan error) error {
	t.Helper()
	select {
	case err := <-errorC:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for error")
		return nil
	}
}

func receiveValue[T any](t *testing.T, valueC <-chan T) T {
	t.Helper()
	select {
	case value := <-valueC:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for value")
		var zero T
		return zero
	}
}

func assertClosed(t *testing.T, signalC <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signalC:
	default:
		t.Fatalf("%s channel is open, want closed", name)
	}
}

func assertOpen(t *testing.T, signalC <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signalC:
		t.Fatalf("%s channel is closed, want open", name)
	default:
	}
}

func assertClosedEventually(t *testing.T, signalC <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signalC:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}
