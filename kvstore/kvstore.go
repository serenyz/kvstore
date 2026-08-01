package kvstore

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"

	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

var (
	// ErrNotFound 表示目标键在指定版本不存在或当时已被删除。
	ErrNotFound = errors.New("key not found at revision")
	// ErrFutureRevision 表示查询版本大于本节点当前已经应用的逻辑版本。
	ErrFutureRevision = errors.New("requested revision is in the future")
	// ErrWriteOutcomeUnknown 表示上下文取消时命令可能已经进入 Raft，调用方无法据此
	// 判断写入最终会提交还是失败；重试前应查询状态或使用业务幂等键去重。
	ErrWriteOutcomeUnknown = errors.New("write outcome unknown")
)

// kvstore 是保存完整键历史的 Raft 复制状态机。
//
// 写操作先编码为 command 并经 proposeC 进入 Raft，只有已提交日志才会修改 kvStore。
// 相同命令序列会在所有节点生成相同的 revision 和历史。LookupLocal 不执行共识读取；
// 当 Follower 尚未追上提交位置时，它返回的可能是较旧状态。
type kvstore struct {
	// proposeC 传递普通业务命令；readIndexC 传递一致性读取屏障请求。
	proposeC   chan<- *proposal
	readIndexC chan<- *readIndexRequest

	// mu 串行化历史查询、日志应用、快照生成和快照恢复。
	mu sync.Mutex

	// kvStore 保存每个键按版本排列的记录；curRevision 是全局最新逻辑版本。
	kvStore     map[string]value
	curRevision revision

	// 两个 inflight 表分别关联写命令应用回执和 ReadIndex 完成信号。
	proposalInflight  *proposalInflight
	readIndexInflight *readIndexInflight

	// snapshotter 从节点的快照目录加载最近持久化的业务状态。
	snapshotter *snap.Snapshotter
}

// newKVStore 构造状态机，并在启动日志消费者前恢复最新可用快照。
//
// 该顺序保证恢复基线不会覆盖刚应用的新日志。成功后返回只读错误通道；后台日志
// 解码、应用或快照恢复失败会写入该通道，正常停止不会写入值。
func newKVStore(
	snapshotter *snap.Snapshotter,
	proposeC chan<- *proposal,
	readIndexC chan<- *readIndexRequest,
	commitC <-chan *commit,
	readStateC <-chan []raft.ReadState,
) (*kvstore, <-chan error, error) {
	s := &kvstore{
		proposeC:          proposeC,
		readIndexC:        readIndexC,
		kvStore:           make(map[string]value),
		curRevision:       1,
		proposalInflight:  newProposalInflight(defaultMaxInflight),
		readIndexInflight: newReadIndexInflight(defaultMaxInflight),
		snapshotter:       snapshotter,
	}

	snapshot, err := s.loadSnapshot()
	if err != nil {
		return nil, nil, fmt.Errorf("load state machine snapshot: %w", err)
	}

	if snapshot != nil {
		// Data 保存 kvSnap 的 JSON；索引、任期和成员配置位于 Metadata。
		log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			return nil, nil, fmt.Errorf("restore state machine snapshot: %w", err)
		}
		s.readIndexInflight.advanceApplied(snapshot.GetMetadata().GetIndex())
	}

	// readCommits 是状态机的唯一日志消费者，保证所有命令严格按提交顺序执行。
	applyErrorC := make(chan error, 1)
	go func() {
		if err := s.readCommits(commitC); err != nil {
			applyErrorC <- err
		}
	}()

	go s.readStates(readStateC)

	return s, applyErrorC, nil
}

// Put 通过 Raft 写入 key 和 val，并等待该命令在本节点完成应用。
// 返回的 record 是新生成的键版本；上下文在提案进入 Raft 后取消时可能返回
// ErrWriteOutcomeUnknown。
func (s *kvstore) Put(
	ctx context.Context,
	key, val string,
) (record, error) {
	return s.proposeAndWait(ctx, command{
		CommandType: CommandPut,
		Key:         key,
		Val:         val,
	})
}

// Delete 通过 Raft 删除 key，并等待本节点应用对应日志。
// 删除不存在或已经删除的键是成功的无操作，返回零值或现有墓碑，且不推进 revision。
func (s *kvstore) Delete(
	ctx context.Context,
	key string,
) (record, error) {
	return s.proposeAndWait(ctx, command{
		CommandType: CommandDelete,
		Key:         key,
	})
}

// proposeAndWait 为命令分配请求标识、编码并提交，然后等待状态机应用回执。
//
// 取消语义由命令是否已经交给 Raft 决定：入队前取消可确定命令未提交；入队后取消
// 只能返回 ErrWriteOutcomeUnknown，因为日志仍可能在后台获得多数派确认并被应用。
func (s *kvstore) proposeAndWait(
	ctx context.Context,
	cmd command,
) (record, error) {
	requestID, err := newRequestID()
	if err != nil {
		return record{}, fmt.Errorf("generate request ID: %w", err)
	}
	cmd.RequestID = requestID

	// 序列化发生在注册和提案之前，此处失败时可确定状态机没有接触该命令。
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		return record{}, fmt.Errorf("encode command: %w", err)
	}

	// 只有可发送的命令才占用有限的回执槽位。
	applyResultC, err := s.proposalInflight.register(requestID)
	if err != nil {
		return record{}, fmt.Errorf("register inflight request: %w", err)
	}
	defer s.proposalInflight.remove(requestID, applyResultC)

	// 首个 select 决定命令是否离开调用协程；ctx 分支意味着尚未进入提案通道。
	proposeResultC := make(chan error, 1)
	select {
	case s.proposeC <- &proposal{ctx: ctx, data: buf.String(), resultC: proposeResultC}:
	case <-ctx.Done():
		return record{}, ctx.Err()
	}

	select {
	case err := <-proposeResultC:
		if err != nil {
			return record{}, fmt.Errorf("propose command: %w", err)
		}
	case <-ctx.Done():
		return record{}, fmt.Errorf(
			"%w: %w",
			ErrWriteOutcomeUnknown,
			ctx.Err(),
		)
	}

	// Propose 返回成功仍不等于提交成功，必须继续等到 readCommits 投递应用结果。
	select {
	case result := <-applyResultC:
		return unwrapApplyResult(result)
	case <-ctx.Done():
		return record{}, fmt.Errorf(
			"%w: %w",
			ErrWriteOutcomeUnknown,
			ctx.Err(),
		)
	}
}

// LookupLinearize 发起 Raft ReadIndex 请求，并在收到进程内完成信号后读取 key。
// 其设计目标是等到本地应用水位达到 ReadState.Index；最终保证依赖
// readIndexInflight.maybeSignal 对目标索引与应用索引的比较正确。
func (s *kvstore) LookupLinearize(ctx context.Context, key string) (record, error) {
	requestID, err := newRequestID()
	if err != nil {
		return record{}, fmt.Errorf("generate request ID: %w", err)
	}

	sig, err := s.readIndexInflight.register(requestID)
	if err != nil {
		return record{}, fmt.Errorf("register inflight request: %w", err)
	}
	defer s.readIndexInflight.remove(requestID, sig)

	if err := s.requestReadIndex(ctx, []byte(requestID)); err != nil {
		return record{}, fmt.Errorf("read index request: %w", err)
	}

	select {
	case <-sig:
		return s.LookupLocal(key)
	case <-ctx.Done():
		return record{}, fmt.Errorf("read linearizability timeout: %w", ctx.Err())
	}
}

// requestReadIndex 将唯一上下文提交给 raft.Node.ReadIndex，并等待同步调用结果。
// raft.Node 接受请求后，真正的安全读取索引会异步出现在 Ready.ReadStates 中。
func (s *kvstore) requestReadIndex(ctx context.Context, requestCtx []byte) error {
	resultC := make(chan error, 1)
	req := &readIndexRequest{
		ctx:        ctx,
		requestCtx: requestCtx,
		resultC:    resultC,
	}

	select {
	case s.readIndexC <- req:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-resultC:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// LookupLocal 返回 key 在本节点当前已应用版本上的记录。
//
// 空字符串是合法值，因此存在性由 error 判断：写入 key="/x", val="" 后会返回
// Value 为空且 error 为 nil；不存在或最新记录为墓碑时返回 ErrNotFound。
func (s *kvstore) LookupLocal(key string) (record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	history := s.kvStore[key]
	if len(history) == 0 {
		return record{}, ErrNotFound
	}

	latest := history[len(history)-1]
	if latest.Deleted {
		return record{}, ErrNotFound
	}
	return *latest, nil
}

// LookupAtRevision 查询 key 在 target 版本时可见的最后一条记录。
//
// NoneRevision 等价于当前版本。历史通过二分查找定位第一个 ModRevision 大于 target
// 的记录，再取其前一项。例如修改版本依次为 2、5、9，查询版本 7 会返回版本 5；
// 若该记录是墓碑，则返回 ErrNotFound。
func (s *kvstore) LookupAtRevision(key string, target revision) (record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if target == NoneRevision {
		target = s.curRevision
	}

	if target > s.curRevision {
		return record{}, ErrFutureRevision
	}

	history := s.kvStore[key]
	if len(history) == 0 {
		return record{}, ErrNotFound
	}

	index := sort.Search(len(history), func(i int) bool { return history[i].ModRevision > target })
	if index == 0 {
		return record{}, ErrNotFound
	}

	rec := history[index-1]
	if rec.Deleted {
		return record{}, ErrNotFound
	}
	return *rec, nil
}

// readCommits 顺序消费已提交日志，并在每批业务命令应用完成后关闭 applyDoneC。
//
// nil 批次是远端快照已经持久化的控制消息，要求状态机整体重新加载快照。非 nil
// 批次中的 data 按 Raft 日志顺序执行；关闭完成通道后，Raft 层才能确信对应业务
// 状态可用于制作索引一致的快照。
func (s *kvstore) readCommits(commitC <-chan *commit) error {
	for commit := range commitC {
		if commit == nil {
			// 快照是完整状态替换；恢复完成后再继续应用后续增量日志。
			snapshot, err := s.loadSnapshot()
			if err != nil {
				return fmt.Errorf("reload state machine snapshot: %w", err)
			}

			if snapshot != nil {
				log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := s.recoverFromSnapshot(snapshot.GetData()); err != nil {
					return fmt.Errorf("restore published state machine snapshot: %w", err)
				}
			}
			s.readIndexInflight.advanceApplied(snapshot.GetMetadata().GetIndex())
			continue
		}

		for _, data := range commit.data {
			// 已提交载荷无法解码属于状态机协议错误；跳过它会让节点产生不同状态。
			var command command
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&command); err != nil {
				return fmt.Errorf("decode committed state machine command: %w", err)
			}

			if err := s.apply(command); err != nil {
				return err
			}
		}
		s.readIndexInflight.advanceApplied(commit.raftLogIndex)

		// 完成通道由状态机独占关闭；close 可同时唤醒所有等待该批次的协程。
		close(commit.applyDoneC)
	}
	return nil
}

// readStates 消费 Ready.ReadStates，并把每个请求标识对应的安全读取索引写入等待堆。
func (s *kvstore) readStates(readStateC <-chan []raft.ReadState) {
	for states := range readStateC {
		for _, state := range states {
			s.readIndexInflight.updateReadIndex(string(state.RequestCtx), state.Index)
		}
	}
}

// apply 将一条已提交命令分派给确定性的状态转换，并完成原始写请求的回执。
// 未知命令类型不能被忽略，否则不同版本的节点可能悄然产生分歧。
func (s *kvstore) apply(cmd command) error {
	switch cmd.CommandType {
	case CommandPut:
		rec := s.applyPut(cmd.Key, cmd.Val)
		s.proposalInflight.complete(cmd.RequestID, &applyResult{
			record: rec,
			err:    nil,
		})
		return nil
	case CommandDelete:
		rec := s.applyDelete(cmd.Key)
		s.proposalInflight.complete(cmd.RequestID, &applyResult{
			record: rec,
			err:    nil,
		})
		return nil

	default:
		return fmt.Errorf("apply unknown command type %d", cmd.CommandType)
	}
}

// applyPut 追加一个写入版本并推进全局 revision。
// 首次写入从 Version=1 开始；后续写入（包括墓碑后的再次写入）沿用最初的
// CreateRevision，并在上一条记录基础上递增 Version。
func (s *kvstore) applyPut(key, val string) record {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.curRevision++
	rev := s.curRevision
	history := s.kvStore[key]
	var rec record
	if len(history) == 0 {
		rec = record{
			Value:          val,
			CreateRevision: rev,
			ModRevision:    rev,
			Version:        1,
			Deleted:        false,
		}
	} else {
		previous := history[len(history)-1]
		rec = record{
			Value:          val,
			CreateRevision: previous.CreateRevision,
			ModRevision:    rev,
			Version:        previous.Version + 1,
			Deleted:        false,
		}
	}
	s.kvStore[key] = append(history, &rec)

	return rec
}

// applyDelete 为现存键追加墓碑并推进 revision。
// 对从未存在或最新版本已经是墓碑的键重复删除不会生成新历史记录。
func (s *kvstore) applyDelete(key string) record {
	s.mu.Lock()
	defer s.mu.Unlock()

	history := s.kvStore[key]
	if len(history) == 0 {
		return record{}
	}

	previous := history[len(history)-1]
	if previous.Deleted {
		return *previous
	}

	s.curRevision++
	rev := s.curRevision
	tombstone := record{
		Value:          "",
		CreateRevision: previous.CreateRevision,
		ModRevision:    rev,
		Version:        previous.Version + 1,
		Deleted:        true,
	}
	s.kvStore[key] = append(history, &tombstone)
	return tombstone
}

// getSnapshot 在同一个临界区内将当前 revision 和全部键历史编码为 JSON。
//
// 锁覆盖 json.Marshal，而不只是复制 map 头；否则并发追加历史时可能得到版本号与
// 数据不匹配的快照。结果由 Raft 层写入 raftpb.Snapshot.Data。
func (s *kvstore) getSnapshot() ([]byte, error) {
	s.mu.Lock()
	kvsnap := &kvSnap{
		CurRevision: s.curRevision,
		KvStore:     s.kvStore,
	}
	defer s.mu.Unlock()
	return json.Marshal(kvsnap)
}

// loadSnapshot 从 snapshotter 读取最新快照，并把首次启动时的 ErrNoSnapshot 转换为
// nil 快照和 nil 错误。
//
// 其他错误保留给调用方处理，因为损坏文件或读取失败都不能安全地退化为空状态启动。
func (s *kvstore) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := s.snapshotter.Load()
	if errors.Is(err, snap.ErrNoSnapshot) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return snapshot, nil
}

// recoverFromSnapshot 解析 kvSnap，并以其中的版本和历史整体替换内存状态。
//
// JSON 在加锁前完成校验，因此解析失败不会破坏当前状态。成功恢复采用替换而非合并：
// 若快照仅包含键 a，则旧内存中不在快照内的键 b 会被移除。
func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	var kvsnap kvSnap
	if err := json.Unmarshal(snapshot, &kvsnap); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.kvStore = kvsnap.KvStore
	s.curRevision = kvsnap.CurRevision
	return nil
}
