package kvstore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// TestEntriesToApply 验证已应用前缀会被过滤，同时拒绝与 appliedIndex 不连续的批次。
// 例如本地已应用到索引 2 时，[1,2,3,4] 应只返回 [3,4]，而 [4,5] 因缺少索引 3
// 必须报错。
func TestEntriesToApply(t *testing.T) {
	tests := []struct {
		name         string
		appliedIndex uint64
		indexes      []uint64
		wantIndexes  []uint64
		wantErr      bool
		nilInput     bool
		wantNil      bool
	}{
		{
			name:         "nil batch",
			appliedIndex: 2,
			nilInput:     true,
			wantNil:      true,
		},
		{
			name:         "empty batch",
			appliedIndex: 2,
		},
		{
			name:         "entire batch is new",
			appliedIndex: 2,
			indexes:      []uint64{3, 4},
			wantIndexes:  []uint64{3, 4},
		},
		{
			name:         "already applied prefix is removed",
			appliedIndex: 2,
			indexes:      []uint64{1, 2, 3, 4},
			wantIndexes:  []uint64{3, 4},
		},
		{
			name:         "entire batch was already applied",
			appliedIndex: 4,
			indexes:      []uint64{1, 2, 3, 4},
			wantNil:      true,
		},
		{
			name:         "gap after applied index",
			appliedIndex: 2,
			indexes:      []uint64{4, 5},
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := &raftNode{appliedIndex: tt.appliedIndex}
			var entries []*raftpb.Entry
			if !tt.nilInput {
				entries = make([]*raftpb.Entry, 0, len(tt.indexes))
				for _, index := range tt.indexes {
					entries = append(entries, testEntry(index, raftpb.EntryNormal, nil))
				}
			}

			got, err := rc.entriesToApply(entries)
			if (err != nil) != tt.wantErr {
				t.Fatalf("entriesToApply() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if (got == nil) != tt.wantNil {
				t.Fatalf("entriesToApply() nil = %v, want %v", got == nil, tt.wantNil)
			}
			if len(got) != len(tt.wantIndexes) {
				t.Fatalf("entriesToApply() returned %d entries, want %d", len(got), len(tt.wantIndexes))
			}
			for i, entry := range got {
				if entry.GetIndex() != tt.wantIndexes[i] {
					t.Errorf("entry %d index = %d, want %d", i, entry.GetIndex(), tt.wantIndexes[i])
				}
			}
		})
	}
}

func TestPublishEntries(t *testing.T) {
	t.Run("publishes normal entries in order", func(t *testing.T) {
		commitC := make(chan *commit, 1)
		rc := &raftNode{
			commitC:      commitC,
			stopc:        make(chan struct{}),
			appliedIndex: 1,
		}
		entries := []*raftpb.Entry{
			testEntry(2, raftpb.EntryNormal, []byte("put-a")),
			testEntry(3, raftpb.EntryNormal, nil),
			testEntry(4, raftpb.EntryNormal, []byte("put-b")),
		}

		doneC, err := rc.publishEntries(entries)
		if err != nil {
			t.Fatalf("publishEntries() error = %v", err)
		}
		if doneC == nil {
			t.Fatal("publishEntries() returned a nil completion channel")
		}

		published := <-commitC
		if len(published.data) != 2 || published.data[0] != "put-a" || published.data[1] != "put-b" {
			t.Fatalf("published data = %q, want [put-a put-b]", published.data)
		}
		if published.raftLogIndex != 4 {
			t.Fatalf("published Raft log index = %d, want 4", published.raftLogIndex)
		}
		if rc.appliedIndex != 4 {
			t.Fatalf("applied index = %d, want 4", rc.appliedIndex)
		}

		close(published.applyDoneC)
		select {
		case <-doneC:
		default:
			t.Fatal("returned completion channel differs from the published channel")
		}
	})

	t.Run("empty normal entry publishes a progress-only commit", func(t *testing.T) {
		commitC := make(chan *commit, 1)
		rc := &raftNode{commitC: commitC, stopc: make(chan struct{}), appliedIndex: 4}

		doneC, err := rc.publishEntries([]*raftpb.Entry{
			testEntry(5, raftpb.EntryNormal, nil),
		})
		if err != nil {
			t.Fatalf("publishEntries() error = %v", err)
		}
		if doneC == nil {
			t.Fatal("publishEntries() returned a nil completion channel for an empty entry")
		}
		published := <-commitC
		if len(published.data) != 0 {
			t.Fatalf("published data count = %d, want 0", len(published.data))
		}
		if published.raftLogIndex != 5 {
			t.Fatalf("published Raft log index = %d, want 5", published.raftLogIndex)
		}
		if rc.appliedIndex != 5 {
			t.Fatalf("applied index = %d, want 5", rc.appliedIndex)
		}
		close(published.applyDoneC)
		select {
		case <-doneC:
		default:
			t.Fatal("progress-only commit did not use the returned completion channel")
		}
	})

	t.Run("stop cancels a blocked commit publication", func(t *testing.T) {
		stopC := make(chan struct{})
		close(stopC)
		rc := &raftNode{
			commitC:      make(chan *commit),
			stopc:        stopC,
			appliedIndex: 7,
		}

		doneC, err := rc.publishEntries([]*raftpb.Entry{
			testEntry(8, raftpb.EntryNormal, []byte("put")),
		})
		if err != nil {
			t.Fatalf("publishEntries() error = %v", err)
		}
		if doneC != nil {
			t.Fatal("publishEntries() returned a completion channel after stop")
		}
		if rc.appliedIndex != 7 {
			t.Fatalf("applied index after canceled publication = %d, want 7", rc.appliedIndex)
		}
	})

	t.Run("rejects malformed configuration changes", func(t *testing.T) {
		rc := &raftNode{stopc: make(chan struct{}), appliedIndex: 9}
		_, err := rc.publishEntries([]*raftpb.Entry{
			testEntry(10, raftpb.EntryConfChange, []byte{0xff}),
		})
		if err == nil || !strings.Contains(err.Error(), "decode committed config change at index 10") {
			t.Fatalf("publishEntries() error = %v, want config decoding error", err)
		}
		if rc.appliedIndex != 9 {
			t.Fatalf("applied index after malformed config change = %d, want 9", rc.appliedIndex)
		}
	})

	t.Run("applies a valid configuration change", func(t *testing.T) {
		wantConfState := &raftpb.ConfState{Voters: []uint64{1, 2}}
		node := &fakeRaftNode{confState: wantConfState}
		commitC := make(chan *commit, 1)
		rc := &raftNode{
			id:      1,
			node:    node,
			stopc:   make(chan struct{}),
			commitC: commitC,
		}
		cc := &raftpb.ConfChange{
			Type:   testPtr(raftpb.ConfChangeAddNode),
			NodeId: testPtr(uint64(2)),
		}
		data, err := proto.Marshal(cc)
		if err != nil {
			t.Fatalf("marshal config change: %v", err)
		}

		doneC, err := rc.publishEntries([]*raftpb.Entry{
			testEntry(1, raftpb.EntryConfChange, data),
		})
		if err != nil {
			t.Fatalf("publishEntries() error = %v", err)
		}
		if doneC == nil {
			t.Fatal("configuration-only batch returned a nil completion channel")
		}
		if node.appliedConfChange == nil || node.appliedConfChange.AsV2().Changes[0].GetNodeId() != 2 {
			t.Fatal("configuration change was not passed to raft.Node")
		}
		if rc.confState != wantConfState {
			t.Fatal("configuration state was not updated from raft.Node")
		}
		if rc.appliedIndex != 1 {
			t.Fatalf("applied index = %d, want 1", rc.appliedIndex)
		}
		published := <-commitC
		if len(published.data) != 0 || published.raftLogIndex != 1 {
			t.Fatalf("configuration progress commit = (data %q, index %d), want (empty, 1)", published.data, published.raftLogIndex)
		}
		close(published.applyDoneC)
		select {
		case <-doneC:
		default:
			t.Fatal("configuration progress commit did not use the returned completion channel")
		}
	})
}

func TestPublishReadStates(t *testing.T) {
	t.Run("empty states need no consumer", func(t *testing.T) {
		rc := &raftNode{}
		if err := rc.publishReadStates(nil); err != nil {
			t.Fatalf("publishReadStates(nil) error = %v", err)
		}
	})

	t.Run("non-empty states require a consumer", func(t *testing.T) {
		rc := &raftNode{}
		err := rc.publishReadStates([]raft.ReadState{{Index: 3}})
		if err == nil || !strings.Contains(err.Error(), "without a configured consumer") {
			t.Fatalf("publishReadStates() error = %v, want missing consumer error", err)
		}
	})

	t.Run("forwards states unchanged", func(t *testing.T) {
		readStatesC := make(chan []raft.ReadState, 1)
		rc := &raftNode{readStatesC: readStatesC, stopc: make(chan struct{})}
		want := []raft.ReadState{{Index: 7, RequestCtx: []byte("request-1")}}

		if err := rc.publishReadStates(want); err != nil {
			t.Fatalf("publishReadStates() error = %v", err)
		}
		got := <-readStatesC
		if len(got) != 1 || got[0].Index != 7 || string(got[0].RequestCtx) != "request-1" {
			t.Fatalf("published read states = %#v, want %#v", got, want)
		}
	})

	t.Run("stop cancels a blocked publication", func(t *testing.T) {
		stopC := make(chan struct{})
		close(stopC)
		rc := &raftNode{readStatesC: make(chan []raft.ReadState), stopc: stopC}
		if err := rc.publishReadStates([]raft.ReadState{{Index: 1}}); err != nil {
			t.Fatalf("publishReadStates() after stop error = %v", err)
		}
	})
}

func TestLoadSnapshot(t *testing.T) {
	t.Run("ignores snapshot files without a WAL", func(t *testing.T) {
		root := t.TempDir()
		snapDir := filepath.Join(root, "snap")
		if err := os.Mkdir(snapDir, 0o750); err != nil {
			t.Fatalf("create snapshot directory: %v", err)
		}
		snapshotter := snap.New(zap.NewNop(), snapDir)
		if err := snapshotter.SaveSnap(testSnapshot(2, 1, &raftpb.ConfState{}, []byte("orphan"))); err != nil {
			t.Fatalf("save orphan snapshot: %v", err)
		}
		rc := &raftNode{
			waldir:      filepath.Join(root, "missing-wal"),
			snapdir:     snapDir,
			snapshotter: snapshotter,
		}

		got, err := rc.loadSnapshot()
		if err != nil {
			t.Fatalf("loadSnapshot() error = %v", err)
		}
		if !raft.IsEmptySnap(got) {
			t.Fatalf("loadSnapshot() = %#v, want empty snapshot", got)
		}
	})

	t.Run("ignores a snapshot newer than committed WAL state", func(t *testing.T) {
		root := t.TempDir()
		snapDir := filepath.Join(root, "snap")
		if err := os.Mkdir(snapDir, 0o750); err != nil {
			t.Fatalf("create snapshot directory: %v", err)
		}
		snapshotter := snap.New(zap.NewNop(), snapDir)
		snapshot := testSnapshot(4, 2, &raftpb.ConfState{}, []byte("uncommitted"))
		if err := snapshotter.SaveSnap(snapshot); err != nil {
			t.Fatalf("save snapshot: %v", err)
		}

		walDir := filepath.Join(root, "wal")
		w, err := wal.Create(zap.NewNop(), walDir, nil)
		if err != nil {
			t.Fatalf("create WAL: %v", err)
		}
		if err := w.SaveSnapshot(&walpb.Snapshot{
			Index:     testPtr(uint64(4)),
			Term:      testPtr(uint64(2)),
			ConfState: &raftpb.ConfState{Voters: []uint64{1}},
		}); err != nil {
			t.Fatalf("save WAL snapshot marker: %v", err)
		}
		if err := w.Save(&raftpb.HardState{Commit: testPtr(uint64(3))}, nil); err != nil {
			t.Fatalf("save WAL hard state: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close WAL: %v", err)
		}

		rc := &raftNode{waldir: walDir, snapdir: snapDir, snapshotter: snapshotter}
		got, err := rc.loadSnapshot()
		if err != nil {
			t.Fatalf("loadSnapshot() error = %v", err)
		}
		if !raft.IsEmptySnap(got) {
			t.Fatalf("loadSnapshot() = index %d, want empty snapshot", got.GetMetadata().GetIndex())
		}
	})
}

func TestOpenWALCreatesMissingDirectory(t *testing.T) {
	walDir := filepath.Join(t.TempDir(), "new-wal")
	rc := &raftNode{waldir: walDir}

	w, err := rc.openWAL(&raftpb.Snapshot{})
	if err != nil {
		t.Fatalf("openWAL() error = %v", err)
	}
	if _, _, _, err := w.ReadAll(); err != nil {
		t.Fatalf("read newly created WAL: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close WAL: %v", err)
	}
	if !wal.Exist(walDir) {
		t.Fatal("openWAL() did not create a WAL directory")
	}
}

func TestReplayWALRestoresSnapshotHardStateAndEntries(t *testing.T) {
	root := t.TempDir()
	snapDir := filepath.Join(root, "snap")
	if err := os.Mkdir(snapDir, 0o750); err != nil {
		t.Fatalf("create snapshot directory: %v", err)
	}
	snapshotter := snap.New(zap.NewNop(), snapDir)
	confState := &raftpb.ConfState{Voters: []uint64{1}}
	snapshot := testSnapshot(2, 1, confState, []byte("state-at-2"))
	if err := snapshotter.SaveSnap(snapshot); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	walDir := filepath.Join(root, "wal")
	w, err := wal.Create(zap.NewNop(), walDir, nil)
	if err != nil {
		t.Fatalf("create WAL: %v", err)
	}
	walSnapshot := &walpb.Snapshot{
		Index:     testPtr(uint64(2)),
		Term:      testPtr(uint64(1)),
		ConfState: confState,
	}
	if err := w.SaveSnapshot(walSnapshot); err != nil {
		t.Fatalf("save WAL snapshot marker: %v", err)
	}
	hardState := &raftpb.HardState{
		Term:   testPtr(uint64(2)),
		Vote:   testPtr(uint64(1)),
		Commit: testPtr(uint64(3)),
	}
	if err := w.Save(hardState, []*raftpb.Entry{testEntryWithTerm(3, 2)}); err != nil {
		t.Fatalf("save WAL state and entry: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close WAL before replay: %v", err)
	}

	rc := &raftNode{
		id:          1,
		waldir:      walDir,
		snapdir:     snapDir,
		snapshotter: snapshotter,
	}
	replayedWAL, err := rc.replayWAL()
	if err != nil {
		t.Fatalf("replayWAL() error = %v", err)
	}
	t.Cleanup(func() {
		if err := replayedWAL.Close(); err != nil {
			t.Errorf("close replayed WAL: %v", err)
		}
	})

	gotSnapshot, err := rc.raftStorage.Snapshot()
	if err != nil {
		t.Fatalf("read restored memory snapshot: %v", err)
	}
	if gotSnapshot.GetMetadata().GetIndex() != 2 || string(gotSnapshot.GetData()) != "state-at-2" {
		t.Fatalf("restored snapshot = (index %d, data %q), want (2, %q)", gotSnapshot.GetMetadata().GetIndex(), gotSnapshot.GetData(), "state-at-2")
	}
	gotHardState, gotConfState, err := rc.raftStorage.InitialState()
	if err != nil {
		t.Fatalf("read restored initial state: %v", err)
	}
	if gotHardState.GetTerm() != 2 || gotHardState.GetVote() != 1 || gotHardState.GetCommit() != 3 {
		t.Fatalf("restored hard state = %#v, want term 2, vote 1, commit 3", gotHardState)
	}
	if voters := gotConfState.GetVoters(); len(voters) != 1 || voters[0] != 1 {
		t.Fatalf("restored voters = %v, want [1]", voters)
	}
	lastIndex, err := rc.raftStorage.LastIndex()
	if err != nil {
		t.Fatalf("read restored last index: %v", err)
	}
	if lastIndex != 3 {
		t.Fatalf("restored last index = %d, want 3", lastIndex)
	}
	term, err := rc.raftStorage.Term(3)
	if err != nil {
		t.Fatalf("read restored term at index 3: %v", err)
	}
	if term != 2 {
		t.Fatalf("restored term at index 3 = %d, want 2", term)
	}
}

func TestPublishSnapshot(t *testing.T) {
	t.Run("ignores an empty snapshot", func(t *testing.T) {
		rc := &raftNode{appliedIndex: 3}
		if err := rc.publishSnapshot(&raftpb.Snapshot{}); err != nil {
			t.Fatalf("publishSnapshot(empty) error = %v", err)
		}
		if rc.appliedIndex != 3 {
			t.Fatalf("applied index = %d, want 3", rc.appliedIndex)
		}
	})

	t.Run("rejects a stale snapshot", func(t *testing.T) {
		commitC := make(chan *commit, 1)
		rc := &raftNode{
			commitC:      commitC,
			stopc:        make(chan struct{}),
			appliedIndex: 5,
		}
		err := rc.publishSnapshot(testSnapshot(5, 2, nil, nil))
		if err == nil || !strings.Contains(err.Error(), "not newer than applied index 5") {
			t.Fatalf("publishSnapshot() error = %v, want stale snapshot error", err)
		}
		select {
		case <-commitC:
			t.Fatal("stale snapshot published a reload signal")
		default:
		}
	})

	t.Run("publishes reload signal and advances state", func(t *testing.T) {
		commitC := make(chan *commit, 1)
		confState := &raftpb.ConfState{Voters: []uint64{1, 2}}
		rc := &raftNode{
			commitC:       commitC,
			stopc:         make(chan struct{}),
			appliedIndex:  5,
			snapshotIndex: 4,
		}

		if err := rc.publishSnapshot(testSnapshot(8, 3, confState, []byte("state"))); err != nil {
			t.Fatalf("publishSnapshot() error = %v", err)
		}
		select {
		case reload := <-commitC:
			if reload != nil {
				t.Fatalf("snapshot reload signal = %#v, want nil", reload)
			}
		default:
			t.Fatal("snapshot reload signal was not published")
		}
		if rc.appliedIndex != 8 || rc.snapshotIndex != 8 {
			t.Fatalf("published indexes = (applied %d, snapshot %d), want (8, 8)", rc.appliedIndex, rc.snapshotIndex)
		}
		if rc.confState != confState {
			t.Fatal("published configuration state differs from snapshot metadata")
		}
	})

	t.Run("stop cancels a blocked reload signal", func(t *testing.T) {
		stopC := make(chan struct{})
		close(stopC)
		rc := &raftNode{
			commitC:       make(chan *commit),
			stopc:         stopC,
			appliedIndex:  2,
			snapshotIndex: 2,
		}
		if err := rc.publishSnapshot(testSnapshot(3, 1, nil, nil)); err != nil {
			t.Fatalf("publishSnapshot() after stop error = %v", err)
		}
		if rc.appliedIndex != 2 || rc.snapshotIndex != 2 {
			t.Fatalf("indexes changed after canceled publication: applied %d, snapshot %d", rc.appliedIndex, rc.snapshotIndex)
		}
	})
}

func TestMaybeTriggerSnapshot(t *testing.T) {
	t.Run("rejects non-monotonic progress", func(t *testing.T) {
		rc := &raftNode{appliedIndex: 4, snapshotIndex: 5}
		err := rc.maybeTriggerSnapshot(nil)
		if err == nil || !strings.Contains(err.Error(), "applied index 4 is behind snapshot index 5") {
			t.Fatalf("maybeTriggerSnapshot() error = %v, want non-monotonic progress error", err)
		}
	})

	t.Run("does nothing at the threshold", func(t *testing.T) {
		called := false
		rc := &raftNode{
			appliedIndex:  12,
			snapshotIndex: 10,
			snapCount:     2,
			getSnapshot: func() ([]byte, error) {
				called = true
				return nil, nil
			},
		}
		if err := rc.maybeTriggerSnapshot(make(chan struct{})); err != nil {
			t.Fatalf("maybeTriggerSnapshot() error = %v", err)
		}
		if called {
			t.Fatal("state snapshot was requested at, rather than above, the threshold")
		}
	})

	t.Run("stop cancels waiting for state machine application", func(t *testing.T) {
		stopC := make(chan struct{})
		close(stopC)
		called := false
		rc := &raftNode{
			appliedIndex:  3,
			snapshotIndex: 1,
			snapCount:     1,
			stopc:         stopC,
			getSnapshot: func() ([]byte, error) {
				called = true
				return nil, nil
			},
		}
		if err := rc.maybeTriggerSnapshot(make(chan struct{})); err != nil {
			t.Fatalf("maybeTriggerSnapshot() after stop error = %v", err)
		}
		if called {
			t.Fatal("state snapshot was requested after stop canceled the wait")
		}
	})

	t.Run("wraps state serialization errors", func(t *testing.T) {
		wantErr := errors.New("encode failed")
		rc := &raftNode{
			appliedIndex:  3,
			snapshotIndex: 1,
			snapCount:     1,
			getSnapshot: func() ([]byte, error) {
				return nil, wantErr
			},
		}
		err := rc.maybeTriggerSnapshot(nil)
		if !errors.Is(err, wantErr) || !strings.Contains(err.Error(), "serialize state machine snapshot") {
			t.Fatalf("maybeTriggerSnapshot() error = %v, want wrapped serialization error", err)
		}
		if rc.snapshotIndex != 1 {
			t.Fatalf("snapshot index after failure = %d, want 1", rc.snapshotIndex)
		}
	})

	t.Run("persists and compacts a successful snapshot", func(t *testing.T) {
		root := t.TempDir()
		snapDir := filepath.Join(root, "snap")
		if err := os.Mkdir(snapDir, 0o750); err != nil {
			t.Fatalf("create snapshot directory: %v", err)
		}
		walDir := filepath.Join(root, "wal")
		w, err := wal.Create(zap.NewNop(), walDir, nil)
		if err != nil {
			t.Fatalf("create WAL: %v", err)
		}
		t.Cleanup(func() {
			if err := w.Close(); err != nil {
				t.Errorf("close WAL: %v", err)
			}
		})

		storage := raft.NewMemoryStorage()
		if err := storage.Append([]*raftpb.Entry{
			testEntryWithTerm(1, 1),
			testEntryWithTerm(2, 1),
			testEntryWithTerm(3, 2),
		}); err != nil {
			t.Fatalf("append memory entries: %v", err)
		}

		oldCatchUp := snapshotCatchUpEntriesN
		snapshotCatchUpEntriesN = 1
		t.Cleanup(func() { snapshotCatchUpEntriesN = oldCatchUp })

		confState := &raftpb.ConfState{Voters: []uint64{1}}
		snapshotter := snap.New(zap.NewNop(), snapDir)
		rc := &raftNode{
			appliedIndex:  3,
			snapshotIndex: 1,
			snapCount:     1,
			getSnapshot: func() ([]byte, error) {
				return []byte("state-at-3"), nil
			},
			confState:   confState,
			raftStorage: storage,
			wal:         w,
			snapshotter: snapshotter,
		}

		applyDoneC := make(chan struct{})
		close(applyDoneC)
		if err := rc.maybeTriggerSnapshot(applyDoneC); err != nil {
			t.Fatalf("maybeTriggerSnapshot() error = %v", err)
		}
		if rc.snapshotIndex != 3 {
			t.Fatalf("snapshot index = %d, want 3", rc.snapshotIndex)
		}

		loaded, err := snapshotter.Load()
		if err != nil {
			t.Fatalf("load persisted snapshot: %v", err)
		}
		if loaded.GetMetadata().GetIndex() != 3 || loaded.GetMetadata().GetTerm() != 2 {
			t.Fatalf("persisted snapshot metadata = (index %d, term %d), want (3, 2)", loaded.GetMetadata().GetIndex(), loaded.GetMetadata().GetTerm())
		}
		if string(loaded.GetData()) != "state-at-3" {
			t.Fatalf("persisted snapshot data = %q, want %q", loaded.GetData(), "state-at-3")
		}
		if got := loaded.GetMetadata().GetConfState().GetVoters(); len(got) != 1 || got[0] != 1 {
			t.Fatalf("persisted voters = %v, want [1]", got)
		}
		firstIndex, err := storage.FirstIndex()
		if err != nil {
			t.Fatalf("read compacted first index: %v", err)
		}
		if firstIndex != 3 {
			t.Fatalf("first retained log index = %d, want 3", firstIndex)
		}
	})
}

func TestProcessMessages(t *testing.T) {
	currentConfState := &raftpb.ConfState{Voters: []uint64{1, 2}}
	oldConfState := &raftpb.ConfState{Voters: []uint64{1}}
	snapshotMessage := &raftpb.Message{
		Type: testPtr(raftpb.MsgSnap),
		Snapshot: &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
			ConfState: oldConfState,
			Index:     testPtr(uint64(4)),
		}},
	}
	appendMessage := &raftpb.Message{
		Type: testPtr(raftpb.MsgApp),
		Snapshot: &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
			ConfState: oldConfState,
		}},
	}
	input := []*raftpb.Message{snapshotMessage, appendMessage}
	rc := &raftNode{confState: currentConfState}

	got := rc.processMessages(input)
	if len(got) != 2 || got[0] != snapshotMessage || got[1] != appendMessage {
		t.Fatalf("processMessages() returned unexpected messages: %#v", got)
	}
	if snapshotMessage.GetSnapshot().GetMetadata().GetConfState() != currentConfState {
		t.Fatal("snapshot message configuration was not replaced")
	}
	if appendMessage.GetSnapshot().GetMetadata().GetConfState() != oldConfState {
		t.Fatal("non-snapshot message configuration was changed")
	}

	got[0] = nil
	if input[0] == nil {
		t.Fatal("returned slice shares its pointer array with the input")
	}
}

func TestStartRaftRejectsInvalidMemberConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		id      int
		peers   []string
		wantErr string
	}{
		{name: "zero member id", id: 0, peers: []string{"http://127.0.0.1:9001"}, wantErr: "outside peer range"},
		{name: "member id above peer count", id: 2, peers: []string{"http://127.0.0.1:9001"}, wantErr: "outside peer range"},
		{name: "malformed URL", id: 1, peers: []string{"%"}, wantErr: "parse local Raft URL"},
		{name: "URL without scheme and host", id: 1, peers: []string{"/raft"}, wantErr: "must include scheme and host"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := &raftNode{id: tt.id, peers: tt.peers}
			err := rc.startRaft()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("startRaft() error = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestRaftTransportCallbacksDelegateToNode(t *testing.T) {
	wantErr := errors.New("step failed")
	node := &fakeRaftNode{stepErr: wantErr}
	rc := &raftNode{node: node}
	message := &raftpb.Message{Type: testPtr(raftpb.MsgHeartbeat)}
	ctx := context.Background()

	if err := rc.Process(ctx, message); !errors.Is(err, wantErr) {
		t.Fatalf("Process() error = %v, want %v", err, wantErr)
	}
	if node.stepContext != ctx || node.stepMessage != message {
		t.Fatal("Process() did not pass the original context and message to raft.Node")
	}
	if rc.IsIDRemoved(9) {
		t.Fatal("IsIDRemoved() = true, want false")
	}

	rc.ReportUnreachable(2)
	if node.unreachableID != 2 {
		t.Fatalf("ReportUnreachable() id = %d, want 2", node.unreachableID)
	}
	rc.ReportSnapshot(3, raft.SnapshotFailure)
	if node.snapshotID != 3 || node.snapshotStatus != raft.SnapshotFailure {
		t.Fatalf("ReportSnapshot() = (%d, %v), want (3, %v)", node.snapshotID, node.snapshotStatus, raft.SnapshotFailure)
	}
}

func testEntry(index uint64, entryType raftpb.EntryType, data []byte) *raftpb.Entry {
	return &raftpb.Entry{
		Index: testPtr(index),
		Type:  testPtr(entryType),
		Data:  data,
	}
}

func testEntryWithTerm(index, term uint64) *raftpb.Entry {
	entry := testEntry(index, raftpb.EntryNormal, nil)
	entry.Term = testPtr(term)
	return entry
}

func testSnapshot(index, term uint64, confState *raftpb.ConfState, data []byte) *raftpb.Snapshot {
	return &raftpb.Snapshot{
		Data: data,
		Metadata: &raftpb.SnapshotMetadata{
			Index:     testPtr(index),
			Term:      testPtr(term),
			ConfState: confState,
		},
	}
}

func testPtr[T any](value T) *T {
	return &value
}

type fakeRaftNode struct {
	confState         *raftpb.ConfState
	appliedConfChange raftpb.ConfChangeI
	stepContext       context.Context
	stepMessage       *raftpb.Message
	stepErr           error
	unreachableID     uint64
	snapshotID        uint64
	snapshotStatus    raft.SnapshotStatus
}

func (*fakeRaftNode) Tick() {}

func (*fakeRaftNode) Campaign(context.Context) error { return nil }

func (*fakeRaftNode) Propose(context.Context, []byte) error { return nil }

func (*fakeRaftNode) ProposeConfChange(context.Context, raftpb.ConfChangeI) error { return nil }

func (n *fakeRaftNode) Step(ctx context.Context, message *raftpb.Message) error {
	n.stepContext = ctx
	n.stepMessage = message
	return n.stepErr
}

func (*fakeRaftNode) Ready() <-chan raft.Ready { return nil }

func (*fakeRaftNode) Advance() {}

func (n *fakeRaftNode) ApplyConfChange(change raftpb.ConfChangeI) *raftpb.ConfState {
	n.appliedConfChange = change
	return n.confState
}

func (*fakeRaftNode) TransferLeadership(context.Context, uint64, uint64) {}

func (*fakeRaftNode) ForgetLeader(context.Context) error { return nil }

func (*fakeRaftNode) ReadIndex(context.Context, []byte) error { return nil }

func (*fakeRaftNode) Status() raft.Status { return raft.Status{} }

func (n *fakeRaftNode) ReportUnreachable(id uint64) { n.unreachableID = id }

func (n *fakeRaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	n.snapshotID = id
	n.snapshotStatus = status
}

func (*fakeRaftNode) Stop() {}
