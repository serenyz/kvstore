// Package kvstore 提供基于 etcd/raft 的版本化内存键值状态机及其 HTTP 服务。
package kvstore

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// raftNode 负责把 etcd/raft 协议状态机连接到持久化、业务应用和节点间传输。
//
// raft.Node 产生 Ready，但不会替应用程序写 WAL、发送消息或执行业务命令；这些工作
// 由 raftNode 按安全顺序完成。该类型还集中维护 appliedIndex、snapshotIndex 和
// ConfState，使日志发布与快照压缩使用一致的进度基准。
type raftNode struct {
	// proposeC 接收普通业务提案；只有随后出现在 CommittedEntries 中的命令才会应用。
	proposeC <-chan *proposal

	// confChangC 接收成员配置提案，字段名沿用项目中的既有拼写。
	confChangC <-chan *confChangeProposal

	// readIndexC 输入一致性读取屏障；readStatesC 输出 Raft 确认的读取索引。
	readIndexC  <-chan *readIndexRequest
	readStatesC chan<- []raft.ReadState

	// commitC 把已提交业务命令或 nil 快照重载信号发送给 kvstore。
	commitC chan<- *commit

	// errorC 是 Raft 后台生命周期的最终错误和完成通知通道。
	errorC chan<- error

	// id 是从 1 开始的本节点成员 ID。
	id int

	// peers 按成员 ID 顺序保存 Raft HTTP 地址，即 peers[id-1] 属于成员 id。
	peers []string

	// join 为 true 时按已有状态重启节点，不用 peers 引导新的初始配置。
	join bool

	// waldir 保存 HardState 和日志增量。
	waldir string

	// snapdir 保存带 Raft 元数据的完整业务快照。
	snapdir string

	// getSnapshot 获取业务数据；CreateSnapshot 负责补充索引、任期和成员配置。
	getSnapshot func() ([]byte, error)

	// confState 是最近应用的成员配置，也是生成/发送快照时的成员关系基准。
	confState *raftpb.ConfState

	// snapshotIndex 是最近一次完成快照流程的日志索引。
	snapshotIndex uint64

	// appliedIndex 是 publishEntries 已处理到的最大提交索引。
	//
	// 它可领先 snapshotIndex，例如 appliedIndex=125、snapshotIndex=100 表示日志已经
	// 发布到 125，而磁盘快照只覆盖到 100。普通日志刚发送至 commitC 时该字段就会
	// 前移；若需要立即制作快照，仍须等待对应 applyDoneC 关闭。
	appliedIndex uint64

	// node 是异步 Raft 协议句柄，负责提案、Step、Ready 和逻辑时钟。
	node raft.Node

	// raftStorage 是运行期日志视图；它不提供进程重启后的持久性。
	raftStorage *raft.MemoryStorage

	// wal 持久化 Raft HardState、日志和快照检查点。
	wal *wal.WAL

	// snapshotter 在 snapdir 中校验、读取和写入完整快照文件。
	snapshotter *snap.Snapshotter

	// httpErrorC 将节点间 HTTP 服务的意外退出报告给主循环。
	httpErrorC chan error

	// snapCount 是 appliedIndex 与 snapshotIndex 允许累积的差值阈值。
	snapCount uint64

	// transport 通过 rafthttp 与其他成员交换协议消息和快照。
	transport *rafthttp.Transport

	// stopc 关闭后停止 Raft 主循环及可能阻塞的内部发送。
	stopc chan struct{}

	// httpstopc 请求停止节点间 HTTP 服务。
	httpstopc chan struct{}

	// httpdonec 在 HTTP Serve 协程退出时关闭，供资源回收流程等待。
	httpdonec chan struct{}
}

// defaultSnapshotCount 是尝试生成相邻快照前允许累积的日志索引差。
//
// 当前比较条件是“严格大于”，因此从索引 100 的快照开始，阈值为 10000 时要等到
// appliedIndex 超过 10100 才触发。该值只决定快照频率，不决定压缩后保留的日志量。
var defaultSnapshotCount uint64 = 10000

// newRaftNode 完成磁盘恢复、协议节点创建和 Raft HTTP 监听后再返回。
//
// commitC 发布已提交业务批次（nil 表示重载快照），readStatesC 发布 ReadIndex 结果，
// errorC 报告后台最终错误，snapshotter 供状态机加载同一快照目录。成员 ID 必须能
// 映射到 peers[id-1]；例如 id=2 使用第二个地址作为本地 Raft 监听地址。
func newRaftNode(
	id int, peers []string, join bool,
	getSnapshot func() ([]byte, error),
	proposeC <-chan *proposal,
	confChangeC <-chan *confChangeProposal,
	readIndexC <-chan *readIndexRequest,
) (<-chan *commit, <-chan []raft.ReadState, <-chan error, *snap.Snapshotter, error) {
	// 无缓冲提交通道让 Raft 发布速度受状态机接收速度约束。
	commitC := make(chan *commit)
	errorC := make(chan error, 1)
	readStatesC := make(chan []raft.ReadState)
	// 每个成员使用独立的相对目录；id=2 对应 kvstore-2/ 和 kvstore-2-snap/。
	rc := raftNode{
		proposeC:    proposeC,
		confChangC:  confChangeC,
		readIndexC:  readIndexC,
		readStatesC: readStatesC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,
		waldir:      fmt.Sprintf("kvstore-%d", id),
		snapdir:     fmt.Sprintf("kvstore-%d-snap", id),
		getSnapshot: getSnapshot,

		snapCount:  defaultSnapshotCount,
		stopc:      make(chan struct{}),
		httpstopc:  make(chan struct{}),
		httpdonec:  make(chan struct{}),
		httpErrorC: make(chan error, 1),
	}

	if err := rc.startRaft(); err != nil {
		return nil, nil, nil, nil, err
	}
	return commitC, readStatesC, errorC, rc.snapshotter, nil
}

// saveSnap 依次保存完整快照、WAL 检查点并释放已覆盖 WAL 段的锁。
//
// walpb.Snapshot 只是 Index、Term 和 ConfState 组成的恢复锚点，业务 Data 位于独立
// 快照文件。文件必须先于检查点落盘：若第二步失败，只会留下一个未被 WAL 认可的
// 孤儿文件；若顺序相反，恢复过程可能找到一个没有完整数据的有效检查点。
//
// 例如 Index=120、Term=7 的快照保存截至日志 120 的键历史，WAL 只记录对应定位
// 信息；重启后加载该文件，再重放 120 之后的日志。
func (rc *raftNode) saveSnap(snap *raftpb.Snapshot) error {
	// WAL 标记不复制可能较大的业务 Data。
	walSnap := &walpb.Snapshot{
		Index:     snap.GetMetadata().Index,
		Term:      snap.GetMetadata().Term,
		ConfState: snap.GetMetadata().ConfState,
	}

	// 只有完整文件成功写入后，才发布对应的 WAL 恢复锚点。
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return fmt.Errorf("save snapshot file at index %d: %w", snap.GetMetadata().GetIndex(), err)
	}

	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return fmt.Errorf("save WAL snapshot marker at index %d: %w", walSnap.GetIndex(), err)
	}

	// 两类数据均持久化后，快照索引之前的 WAL 文件才具备安全回收条件。
	if err := rc.wal.ReleaseLockTo(snap.GetMetadata().GetIndex()); err != nil {
		return fmt.Errorf("release WAL lock through index %d: %w", snap.GetMetadata().GetIndex(), err)
	}
	return nil
}

// entriesToApply 返回 CommittedEntries 中尚未发布的连续后缀。
//
// Ready 可能与本地进度重叠，重复执行非幂等命令会破坏状态。例如 appliedIndex=8、
// 输入 [7,8,9,10] 时只返回 [9,10]。若输入从 10 开始，则索引 9 缺失，函数报错，
// 而不是跨过空洞继续应用。
//
// 该检查只验证批次起点；批次内部索引连续性由 Raft Ready 契约保证。
func (rc *raftNode) entriesToApply(ents []*raftpb.Entry) ([]*raftpb.Entry, error) {
	// 保留 nil 与非 nil 空切片的原始形态。
	if len(ents) == 0 {
		return ents, nil
	}

	// 使用首条日志的全局索引计算重叠前缀长度。
	firstIdx := ents[0].GetIndex()
	if firstIdx > rc.appliedIndex+1 {
		// 新后缀只能从 appliedIndex+1 开始，任何更大值都代表本地缺少已提交日志。
		return nil, fmt.Errorf(
			"committed entries start at index %d after applied index %d",
			firstIdx,
			rc.appliedIndex,
		)
	}

	// applied 是输入中落在本地进度以内的条目数量。
	var applied uint64
	if firstIdx <= rc.appliedIndex {
		applied = rc.appliedIndex - firstIdx + 1
	}
	if applied < uint64(len(ents)) {
		return ents[applied:], nil
	}

	return nil, nil
}

// publishEntries 处理一段新的已提交日志，并将其中的普通命令组成一个状态机批次。
//
// 空 EntryNormal 常由新 Leader 用于提交当前任期，不进入业务状态机；非空普通条目
// 保持原顺序发送到 commitC；EntryConfChange 则在此处更新协议成员和传输地址。
// 返回通道在业务批次应用完成后关闭，没有普通命令时返回 nil。
func (rc *raftNode) publishEntries(ents []*raftpb.Entry) (<-chan struct{}, error) {
	if len(ents) == 0 {
		return nil, nil
	}

	// 以条目总数作为容量上界，避免普通命令集中时重复扩容。
	data := make([]string, 0, len(ents))
	for i := range ents {
		switch ents[i].GetType() {
		case raftpb.EntryNormal:
			if len(ents[i].GetData()) == 0 {
				// 空普通条目仍推进 Raft 索引，但不表示一次键值操作。
				break
			}

			// 原始字节以 string 承载，gob 解码留给唯一的状态机消费者完成。
			s := string(ents[i].GetData())
			data = append(data, s)
		case raftpb.EntryConfChange:
			// 无法解析已提交配置日志时必须停止；跳过会导致节点使用不同投票集合。
			var cc raftpb.ConfChange
			if err := proto.Unmarshal(ents[i].Data, &cc); err != nil {
				return nil, fmt.Errorf(
					"decode committed config change at index %d: %w",
					ents[i].GetIndex(),
					err,
				)
			}

			// 协议成员关系与 HTTP 对等连接分别由 raft.Node 和 transport 维护。
			rc.confState = rc.node.ApplyConfChange(&cc)
			switch cc.GetType() {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					// 例如 Context="http://127.0.0.1:9023" 为成员 3 建立传输连接。
					rc.transport.AddPeer(types.ID(cc.GetNodeId()), []string{string(cc.Context)})
				}

			case raftpb.ConfChangeRemoveNode:
				if cc.GetNodeId() == uint64(rc.id) {
					// 自移除只记录事件；当前实现不会在这里主动终止本进程。
					log.Printf("member %d removed from cluster", rc.id)
				} else {
					// 其他成员移除后同步清理其 rafthttp 连接。
					rc.transport.RemovePeer(types.ID(cc.GetNodeId()))
				}
			}
		}
	}

	var applyDoneC chan struct{}
	// 状态机持有关闭权限，主循环只保留接收方向用于快照前同步。
	applyDoneC = make(chan struct{}, 1)
	select {
	case rc.commitC <- &commit{
		data:         data,
		applyDoneC:   applyDoneC,
		raftLogIndex: ents[len(ents)-1].GetIndex(),
	}:
	case <-rc.stopc:
		// 节点关闭优先于继续等待已经停止的状态机消费者。
		return nil, nil
	}

	// 所有条目类型都推进发布进度；业务数据真正可见的时点由 applyDoneC 表示。
	rc.appliedIndex = ents[len(ents)-1].GetIndex()
	return applyDoneC, nil
}

// publishReadStates 将本批 Ready 中的 ReadIndex 结果转交给业务状态机。
// 未配置消费者却收到结果属于装配错误；停止过程中放弃发送则属于正常退出。
func (rc *raftNode) publishReadStates(rdStates []raft.ReadState) error {
	if len(rdStates) == 0 {
		return nil
	}
	if rc.readStatesC == nil {
		return errors.New("received Raft read state without a configured consumer")
	}
	select {
	case rc.readStatesC <- rdStates:
	case <-rc.stopc:
		return nil
	}
	return nil
}

// loadSnapshot 选择同时存在于快照目录和有效 WAL 标记集合中的最新快照。
//
// 快照文件和 WAL 标记分开写入，故障后可能只剩前者。例如文件索引为 120、但 WAL
// 最新有效标记是 100 时，只能恢复到两者共同认可的 100。没有任何快照是正常启动
// 状态，以空 raftpb.Snapshot 返回；I/O 或校验错误则阻止启动。
func (rc *raftNode) loadSnapshot() (*raftpb.Snapshot, error) {
	if wal.Exist(rc.waldir) {
		// ValidSnapshotEntries 排除未被 WAL 提交进度认可的标记。
		walSnaps, err := wal.ValidSnapshotEntries(zap.NewExample(), rc.waldir)
		if err != nil {
			return nil, fmt.Errorf("list valid WAL snapshots in %q: %w", rc.waldir, err)
		}

		// Snapshotter 从候选标记中寻找可读取且匹配的最新完整文件。
		snapshot, err := rc.snapshotter.LoadNewestAvailable(walSnaps)
		if errors.Is(err, snap.ErrNoSnapshot) {
			return &raftpb.Snapshot{}, nil
		}
		if err != nil {
			return nil, fmt.Errorf("load newest snapshot from %q: %w", rc.snapdir, err)
		}
		if snapshot == nil {
			return &raftpb.Snapshot{}, nil
		}
		return snapshot, nil
	}

	// 没有 WAL 就没有可信的快照检查点，不能单独采用快照目录中的文件。
	return &raftpb.Snapshot{}, nil
}

// openWAL 从 snapshot 的 Index/Term 检查点打开 WAL，若目录不存在则先初始化。
//
// 例如检查点 {Index:120, Term:7} 使 ReadAll 返回该快照之后仍需恢复的记录；空快照
// 使用零检查点，从 WAL 起点读取。函数只定位日志段，不把内容装入 MemoryStorage。
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) (*wal.WAL, error) {
	if !wal.Exist(rc.waldir) {
		// 0750 允许所有者读写、同组只读，拒绝其他用户访问节点日志。
		if err := os.Mkdir(rc.waldir, 0o750); err != nil {
			return nil, fmt.Errorf("create WAL directory %q: %w", rc.waldir, err)
		}

		// 首次创建后重新 Open，使新旧节点统一走检查点定位路径。
		w, err := wal.Create(zap.NewExample(), rc.waldir, nil)
		if err != nil {
			return nil, fmt.Errorf("create WAL in %q: %w", rc.waldir, err)
		}
		if err := w.Close(); err != nil {
			return nil, fmt.Errorf("close newly created WAL in %q: %w", rc.waldir, err)
		}
	}

	// 零值 walpb.Snapshot 表示不跳过任何已有日志段。
	walsnap := walpb.Snapshot{}
	if snapshot != nil && snapshot.GetMetadata() != nil {
		// 定位只需要 Index 和 Term，ConfState 从完整快照恢复。
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}

	log.Printf("loading WAL at term %d and index %d", walsnap.GetTerm(), walsnap.GetIndex())

	// 找不到匹配检查点意味着快照与 WAL 恢复链断裂，不能退化为从头启动。
	w, err := wal.Open(zap.NewExample(), rc.waldir, &walsnap)
	if err != nil {
		return nil, fmt.Errorf(
			"open WAL %q at term %d index %d: %w",
			rc.waldir,
			walsnap.GetTerm(),
			walsnap.GetIndex(),
			err,
		)
	}
	return w, nil
}

// replayWAL 用最新可信快照及其后续 WAL 内容重建 raft.MemoryStorage。
//
// 恢复顺序是 Snapshot、HardState、Entries。假设快照覆盖到 100、WAL 还有 101～120，
// MemoryStorage 先建立索引 100 的基线，再恢复任期/投票/提交位置，最后追加剩余日志；
// 任一步失败都会关闭刚打开的 WAL。
func (rc *raftNode) replayWAL() (w *wal.WAL, retErr error) {
	log.Printf("replaying WAL of member %d", rc.id)

	// 快照选择和 WAL 打开共享同一个恢复锚点。
	snapshot, err := rc.loadSnapshot()
	if err != nil {
		return nil, err
	}
	w, err = rc.openWAL(snapshot)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr == nil || w == nil {
			return
		}
		if err := w.Close(); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close WAL after recovery failure: %w", err))
		}
		w = nil
	}()

	// 项目没有使用 WAL 自定义 metadata，只恢复 HardState 和日志条目。
	_, st, ents, err := w.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read WAL %q: %w", rc.waldir, err)
	}

	// 新建存储避免把恢复结果叠加到未知的旧内存状态上。
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil && !raft.IsEmptySnap(snapshot) {
		// 已筛选的非空快照先建立压缩后的日志基线。
		if err := rc.raftStorage.ApplySnapshot(snapshot); err != nil {
			return nil, fmt.Errorf("apply recovered snapshot: %w", err)
		}
	}

	// HardState 恢复 Term、Vote 和 Commit，不包含业务键值。
	if err := rc.raftStorage.SetHardState(st); err != nil {
		return nil, fmt.Errorf("restore Raft hard state: %w", err)
	}

	// 最后追加检查点之后的日志，以满足 RestartNode 的存储视图。
	if err := rc.raftStorage.Append(ents); err != nil {
		return nil, fmt.Errorf("append recovered WAL entries: %w", err)
	}
	return w, nil
}

// finish 按网络、Raft、状态机通道、WAL 的顺序回收资源，并关闭 errorC。
//
// serveChannels 保证只调用一次。若 runErr 和 WAL Close 同时失败，errors.Join 保留
// 两个原因；正常退出不向 errorC 写值，调用方通过通道关闭感知完成。
func (rc *raftNode) finish(runErr error) {
	rc.stopHTTP()
	rc.node.Stop()
	close(rc.commitC)
	if err := rc.wal.Close(); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("close WAL: %w", err))
	}
	if runErr != nil {
		rc.errorC <- runErr
	}
	close(rc.errorC)
}

// startRaft 验证本地成员配置，恢复持久化状态并启动协议与传输协程。
//
// 对外可见的 goroutine 最后启动，因此此前发生的 URL、目录、WAL 或监听错误都会同步
// 返回。延迟清理覆盖“WAL 已打开但传输层启动失败”等部分初始化场景。
func (rc *raftNode) startRaft() (retErr error) {
	if rc.id < 1 || rc.id > len(rc.peers) {
		return fmt.Errorf("member ID %d is outside peer range [1,%d]", rc.id, len(rc.peers))
	}

	localURL, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		return fmt.Errorf("parse local Raft URL %q: %w", rc.peers[rc.id-1], err)
	}
	if localURL.Scheme == "" || localURL.Host == "" {
		return fmt.Errorf("local Raft URL %q must include scheme and host", rc.peers[rc.id-1])
	}

	// 快照可能含完整业务值，目录权限不向其他用户开放。
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0o750); err != nil {
			return fmt.Errorf("create snapshot directory %q: %w", rc.snapdir, err)
		}
	}

	// Snapshotter 是无状态文件访问器，业务数据仍由 kvstore 持有。
	rc.snapshotter = snap.New(zap.NewExample(), rc.snapdir)

	// replayWAL 会在首次启动时创建目录，故必须预先记住是否存在历史 WAL。
	oldwal := wal.Exist(rc.waldir)
	if w, err := rc.replayWAL(); err != nil {
		return err
	} else {
		rc.wal = w
	}
	started := false
	defer func() {
		if started {
			return
		}
		if rc.node != nil {
			rc.node.Stop()
		}
		if rc.wal != nil {
			if err := rc.wal.Close(); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("close WAL after startup failure: %w", err))
			}
		}
	}()

	// 新集群的初始成员 ID 由 peers 位置生成，例如三个地址对应 {1,2,3}。
	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	// 100ms 逻辑 tick 下，心跳约每 100ms 一次，选举超时基准约为 1s。
	c := &raft.Config{
		ID:            uint64(rc.id),
		ElectionTick:  10,
		HeartbeatTick: 1,
		Storage:       rc.raftStorage,
		// 限制一条发送消息中日志载荷的近似上限。
		MaxSizePerMsg: 1024 * 1024,
		// 限制每个 Follower 未确认 Append 消息的流水线深度。
		MaxInflightMsgs: 256,
		// 对 Leader 尚未提交的日志总量施加约 1 GiB 的背压上限。
		MaxUncommittedEntriesSize: 1 << 30,
	}

	// 历史 WAL 或 join 模式都从已有协议状态恢复；只有全新引导才提交 rpeers。
	if oldwal || rc.join {
		rc.node = raft.RestartNode(c)
	} else {
		rc.node = raft.StartNode(c, rpeers)
	}

	// 该 Transport 专用于成员间协议流量。固定 ClusterID 使它拒绝其他集群的消息。
	rc.transport = &rafthttp.Transport{
		Logger:      zap.NewExample(),
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewExample(), strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}
	ln, err := newStoppableListener(localURL.Host, rc.httpstopc)
	if err != nil {
		return fmt.Errorf("listen for Raft HTTP on %q: %w", localURL.Host, err)
	}

	// 先启动传输内部组件，再注册所有远端 peer；本成员不需要回环连接。
	if err := rc.transport.Start(); err != nil {
		if closeErr := ln.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close Raft listener after startup failure: %w", closeErr))
		}
		return err
	}

	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}

	// serveRaft 接收网络消息，serveChannels 驱动 Ready 和持久化，两者并行运行。
	go rc.serveRaft(ln)
	go rc.serveChannels()
	started = true
	return nil
}

// stopHTTP 先停止对等传输，再关闭监听信号并等待 Serve 协程确认退出。
//
// 请求/确认两阶段同步保证 finish 返回前端口和传输连接已经释放。
func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

// publishSnapshot 通知业务状态机加载已经持久化并应用到 MemoryStorage 的新快照。
//
// saveSnap 处理磁盘耐久性，本方法处理运行期可见性。它通过 nil commit 触发 kvstore
// 从同一 Snapshotter 重载，并把成员配置、快照水位和应用水位推进到快照索引。
// 不严格新于 appliedIndex 的快照会导致状态倒退，因而被拒绝。
func (rc *raftNode) publishSnapshot(snapshotToSave *raftpb.Snapshot) error {
	// Ready.Snapshot 的零值不代表状态变化。
	if raft.IsEmptySnap(snapshotToSave) {
		return nil
	}

	snapshotIndex := snapshotToSave.GetMetadata().GetIndex()
	log.Printf("publishing snapshot at index %d", snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", snapshotIndex)

	// 例如本地进度为 120 时，索引 100 或 120 都不是可发布的新基线。
	if snapshotIndex <= rc.appliedIndex {
		return fmt.Errorf(
			"received snapshot index %d is not newer than applied index %d",
			snapshotIndex,
			rc.appliedIndex,
		)
	}

	// nil 是通道协议中的“整体重载”标记，与空业务批次含义不同。
	select {
	case rc.commitC <- nil:
	case <-rc.stopc:
		return nil
	}

	// 完整快照同时确定业务进度和成员配置，三个字段必须原子式连续更新。
	rc.confState = snapshotToSave.Metadata.GetConfState()
	rc.snapshotIndex = snapshotIndex
	rc.appliedIndex = snapshotIndex
	return nil
}

// snapshotCatchUpEntriesN 是内存压缩后为增量追赶保留的最近日志数量。
//
// 值为 10000 时，落后不足该窗口的 Follower 通常仍可接收 Append；更旧的节点需要
// 通过快照恢复。它与触发快照的 defaultSnapshotCount 相互独立。
var snapshotCatchUpEntriesN uint64 = 10000

// maybeTriggerSnapshot 在应用水位超过快照阈值后生成持久化快照并压缩内存日志。
//
// publishEntries 会先推进 appliedIndex 再由另一个协程修改 map，因此触发快照时必须
// 等待 applyDoneC。否则可能生成 Metadata.Index=200、业务数据却只应用到 199 的
// 无效恢复基线。未达到阈值时无需等待该通道。
func (rc *raftNode) maybeTriggerSnapshot(applyDoneC <-chan struct{}) error {
	// 先验证单调性，避免无符号减法下溢把异常进度误判为巨大差值。
	if rc.appliedIndex < rc.snapshotIndex {
		return fmt.Errorf(
			"applied index %d is behind snapshot index %d",
			rc.appliedIndex,
			rc.snapshotIndex,
		)
	}
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return nil
	}

	if applyDoneC != nil {
		// 停止信号使关闭过程不依赖状态机一定能返回确认。
		select {
		case <-applyDoneC:
		case <-rc.stopc:
			return nil
		}
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)

	// 回调只产生业务 JSON，Raft 元数据由下一步根据内存日志补齐。
	data, err := rc.getSnapshot()
	if err != nil {
		return fmt.Errorf("serialize state machine snapshot: %w", err)
	}

	// CreateSnapshot 将业务数据绑定到 appliedIndex 处的任期和当前 ConfState。
	snapshot, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, rc.confState, data)
	if err != nil {
		return fmt.Errorf("create Raft snapshot at index %d: %w", rc.appliedIndex, err)
	}

	// 在快照具备耐久性之前，不能丢弃唯一可用于恢复的历史日志。
	if err := rc.saveSnap(snapshot); err != nil {
		return err
	}

	// 例如 appliedIndex=25000 且窗口为 10000，只压缩到 15000，保留其后的增量日志。
	compactIndex := uint64(1)
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}

	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		// 重复压缩返回 ErrCompacted，可按已达到目标处理。
		if !errors.Is(err, raft.ErrCompacted) {
			return fmt.Errorf("compact Raft log at index %d: %w", compactIndex, err)
		}
	} else {
		log.Printf("compacted log at index %d", compactIndex)
	}

	// 全部步骤成功后才更新阈值基准，失败会在后续 Ready 中重新尝试。
	rc.snapshotIndex = rc.appliedIndex
	return nil
}

// serveChannels 是单个成员的 Ready 处理与生命周期主循环。
//
// 主协程串行处理 Tick、Ready、网络错误和停止信号；内部提案协程负责调用 Propose、
// ProposeConfChange 与 ReadIndex。单线程推进 Ready 可避免 WAL、MemoryStorage、
// appliedIndex 和 snapshotIndex 被不同批次交错修改。
func (rc *raftNode) serveChannels() {
	// 启动进度统一取自恢复后的内存快照，确保三个水位属于同一恢复基线。
	snapshot, err := rc.raftStorage.Snapshot()
	if err != nil {
		rc.finish(fmt.Errorf("load initial in-memory Raft snapshot: %w", err))
		return
	}
	rc.confState = snapshot.GetMetadata().ConfState
	rc.snapshotIndex = snapshot.GetMetadata().GetIndex()
	rc.appliedIndex = snapshot.GetMetadata().GetIndex()

	// raft.Node 使用逻辑 tick；墙钟定时器只负责按 100ms 推进一步。
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// 独立协程使输入通道消费不必等待当前 Ready 完成磁盘 I/O。
	go func() {
		// 配置变更 ID 只保证本进程运行期内单调且不重复。
		confChangeCount := uint64(0)

		// 已关闭的输入被置 nil，从 select 中移除；三类输入全部结束后才广播停止。
		for rc.proposeC != nil || rc.confChangC != nil || rc.readIndexC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					// 对 nil 通道的收发永远阻塞，可安全禁用该分支。
					rc.proposeC = nil
				} else {
					// 这里回报的是 node.Propose 调用结果，最终提交仍取决于 Leader 和多数派。
					prop.resultC <- rc.node.Propose(prop.ctx, []byte(prop.data))
				}
			case cc, ok := <-rc.confChangC:
				if !ok {
					rc.confChangC = nil
				} else {
					confChangeCount++
					// ID 在进入 Raft 前写入，以关联配置变更日志。
					cc.confChange.Id = new(confChangeCount)
					cc.resultC <- rc.node.ProposeConfChange(cc.ctx, cc.confChange)
				}
			case rctx, ok := <-rc.readIndexC:
				if !ok {
					rc.readIndexC = nil
				} else {
					rctx.resultC <- rc.node.ReadIndex(rctx.ctx, rctx.requestCtx)
				}
			}
		}

		// 关闭 stopc 会同时解除主循环和阻塞中的发布操作。
		close(rc.stopc)
	}()

	for {
		select {
		case <-ticker.C:
			// Tick 可能触发选举、心跳或复制重试，但不直接执行磁盘 I/O。
			rc.node.Tick()
		case rd, ok := <-rc.node.Ready():
			if !ok {
				rc.finish(errors.New("raft Ready channel closed unexpectedly"))
				return
			}
			// 每批 Ready 遵循“持久化 → 更新存储 → 发送 → 应用 → Advance”的顺序。
			if !raft.IsEmptySnap(rd.Snapshot) {
				// 完整快照在进入 MemoryStorage 前先写入恢复链。
				if err := rc.saveSnap(rd.Snapshot); err != nil {
					rc.finish(err)
					return
				}
			}

			// nil HardState 告诉 WAL 本批只有 Entries 需要保存。
			var hs *raftpb.HardState
			if !raft.IsEmptyHardState(rd.HardState) {
				hs = rd.HardState
			}

			// 对外发送前落盘，保证崩溃恢复后仍能兑现已发送消息所宣称的状态。
			if err := rc.wal.Save(hs, rd.Entries); err != nil {
				rc.finish(fmt.Errorf("persist Raft hard state and entries: %w", err))
				return
			}
			if !raft.IsEmptySnap(rd.Snapshot) {
				// 磁盘和协议内存先采用快照，之后业务状态机再从快照目录重载。
				if err := rc.raftStorage.ApplySnapshot(rd.Snapshot); err != nil {
					rc.finish(fmt.Errorf("apply Ready snapshot to memory storage: %w", err))
					return
				}

				if err := rc.publishSnapshot(rd.Snapshot); err != nil {
					rc.finish(err)
					return
				}

			}

			// Entries 是新增但未必提交的日志；CommittedEntries 才可执行到业务状态机。
			if err := rc.raftStorage.Append(rd.Entries); err != nil {
				rc.finish(fmt.Errorf("append Ready entries to memory storage: %w", err))
				return
			}

			// processMessages 仅对快照消息补齐成员配置，再交给异步传输层。
			rc.transport.Send(rc.processMessages(rd.Messages))

			// 去掉与 appliedIndex 重叠的提交前缀，防止状态机重复执行命令。
			entries, err := rc.entriesToApply(rd.CommittedEntries)
			if err != nil {
				rc.finish(err)
				return
			}
			applyDoneC, err := rc.publishEntries(entries)
			if err != nil {
				rc.finish(err)
				return
			}

			// 仅在本批触发快照时同步等待应用；常规批次允许状态机与后续 Ready 流水执行。
			if err := rc.maybeTriggerSnapshot(applyDoneC); err != nil {
				rc.finish(err)
				return
			}

			err = rc.publishReadStates(rd.ReadStates)
			if err != nil {
				rc.finish(err)
				return
			}

			// Advance 移交本批 Ready 的所有权，之后不得再依赖其中的可变切片。
			rc.node.Advance()
		case err, ok := <-rc.transport.ErrorC:
			// 传输错误按节点级故障处理；关闭或 nil 值会转换为可诊断错误。
			if !ok {
				err = errors.New("raft transport error channel closed unexpectedly")
			} else if err == nil {
				err = errors.New("raft transport reported a nil error")
			}
			rc.finish(err)
			return
		case err := <-rc.httpErrorC:
			rc.finish(err)
			return
		case <-rc.stopc:
			// 所有输入通道结束触发正常资源回收，不向 errorC 写入错误。
			rc.finish(nil)
			return
		}
	}
}

// processMessages 用当前 confState 覆盖每条 MsgSnap 中的成员配置。
//
// Append 和 Heartbeat 原样返回。快照接收方会以 Snapshot.Metadata.ConfState 恢复
// 投票集合，因此发送前必须与本节点已应用的配置保持一致。
func (rc *raftNode) processMessages(ms []*raftpb.Message) []*raftpb.Message {
	// 仅分配新的切片头和底层指针数组，消息对象本身仍与输入共享。
	var messages []*raftpb.Message
	for i := 0; i < len(ms); i++ {
		if ms[i].GetType() == raftpb.MsgSnap {
			ms[i].Snapshot.Metadata.ConfState = rc.confState
		}
		messages = append(messages, ms[i])
	}
	return messages
}

// serveRaft 使用已经创建的监听器承载成员间 rafthttp Handler。
//
// 预先监听使端口占用等错误能在 startRaft 中同步返回。运行后若 Serve 意外结束，
// 错误经 httpErrorC 交给主循环；由 httpstopc 或 stopc 导致的退出视为预期关闭。
func (rc *raftNode) serveRaft(ln *stoppableListener) {
	defer close(rc.httpdonec)
	// Handler 完成协议校验与解码，并通过 Process 将消息送入 raft.Node。
	err := (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
		// 主动网络关闭通常表现为 Serve 返回错误，但无需上报。
		return
	case <-rc.stopc:
		return
	default:
	}

	if err == nil {
		err = errors.New("raft HTTP server stopped without an error")
	}
	rc.httpErrorC <- fmt.Errorf("serve Raft HTTP: %w", err)
}

// Process 实现 rafthttp.Raft，将已经解码的对等消息交给 raft.Node.Step。
func (rc *raftNode) Process(ctx context.Context, m *raftpb.Message) error {
	return rc.node.Step(ctx, m)
}

// IsIDRemoved 实现 rafthttp.Raft；当前传输层不会在接收阶段拒绝历史成员 ID。
//
// 已提交移除日志会由 publishEntries 调用 RemovePeer 清理主动连接，但本方法没有维护
// 一份持久化的 removed-ID 集合，因此始终返回 false。
func (rc *raftNode) IsIDRemoved(_ uint64) bool { return false }

// ReportUnreachable 实现 rafthttp.Raft，把对等节点暂时不可达反馈给复制进度状态机。
//
// 该事件影响重试和探测，不会自动执行成员移除。
func (rc *raftNode) ReportUnreachable(id uint64) { rc.node.ReportUnreachable(id) }

// ReportSnapshot 实现 rafthttp.Raft，反馈一次向 Follower 发送快照的最终状态。
//
// 成功时 Raft 可把该 Follower 的复制进度移到快照之后；失败时保留重试所需状态。
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}
