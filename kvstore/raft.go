// Package kvstore 实现一个由 Raft 复制日志驱动的键值存储。
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

// commit 表示一批已经由 Raft 集群确认、等待应用到本地状态机的日志数据。
//
// “已提交”只说明多数派已经确认这些日志，不代表本地键值状态机已经执行完毕。
// 例如，data 中依次包含“设置 x=1”和“设置 x=2”时，状态机必须按切片顺序执行，
// 最终 x 才会等于 2；全部执行成功后，再通过 applyDoneC 通知 Raft 处理流程。
type commit struct {
	// data 保存待应用命令的编码结果，元素顺序与 Raft 日志的提交顺序一致。
	data []string

	// applyDoneC 用于通知提交方：data 已全部应用到状态机。
	//
	// 这里使用 chan<- 限定 commit 的使用者只能发送或关闭通道，不能从中接收。
	// 当前状态机通过 close(applyDoneC) 完成通知；关闭通道还能同时唤醒所有等待者。
	applyDoneC chan<- struct{}
}

// raftNode 封装单个 Raft 成员运行所需的通信通道、持久化状态和网络组件。
//
// raft.Node 只负责 Raft 协议状态机；业务命令的执行、WAL 与快照的落盘，以及节点间
// 消息传输，均由 raftNode 周边组件完成。把这些状态集中在此处，可以保证日志提交、
// 应用和快照截断使用同一组索引。
type raftNode struct {
	// proposeC 接收上层提交的业务命令。命令进入此通道不等于已经提交，
	// 只有 Raft 达成多数派共识后，它才会经 commitC 交给状态机执行。
	proposeC <-chan string

	// confChangC 接收成员增删等集群配置变更。配置变更也必须作为 Raft 日志复制，
	// 不能直接修改 peers 或 confState，否则不同节点可能看到不一致的成员关系。
	confChangC <-chan *raftpb.ConfChange

	// commitC 按日志顺序向业务状态机输出已经提交的命令批次。
	commitC chan<- *commit

	// errorC 向上层报告 Raft 后台流程中无法自行恢复的异步错误。
	errorC chan<- error

	// id 是当前成员在 Raft 集群中的唯一编号。
	id int

	// peers 保存集群成员的 Raft HTTP 地址，用于初始化节点间通信。
	// 例如，三个本地成员可以使用不同端口的三个 http://127.0.0.1:<port> 地址。
	peers []string

	// join 表示当前成员是否加入一个已经存在的集群；false 通常表示按初始成员列表
	// 启动新集群，true 则不能再次把初始成员提议为一套新的集群配置。
	join bool

	// waldir 是预写日志（WAL）目录。WAL 用于在进程重启后恢复尚未被快照覆盖的日志。
	waldir string

	// snapdir 是快照目录。快照保存某一日志索引处完整的业务状态机数据。
	snapdir string

	// getSnapshot 在制作快照时序列化当前业务状态机。
	// 返回值只包含业务数据；快照的日志索引、任期和成员配置由 Raft 元数据提供。
	getSnapshot func() ([]byte, error)

	// confState 是最近一次已应用配置变更产生的成员状态，制作快照时需要把它写入
	// Snapshot.Metadata，以便节点仅凭快照也能恢复当时的集群成员关系。
	confState *raftpb.ConfState

	// snapshotIndex 是最近一次成功持久化的快照所覆盖的最大日志索引。
	snapshotIndex uint64

	// appliedIndex 是当前处理流程已经推进到的最大已提交日志索引。
	//
	// appliedIndex 与 snapshotIndex 不同：前者描述“已经执行到哪里”，后者描述
	// “已经持久化到哪里”。例如 snapshotIndex=100、appliedIndex=120 表示状态机已
	// 执行到 120，但当前快照只覆盖到 100；下一次快照可把 101～120 一并纳入。
	//
	// publishEntries 会先把普通日志交给业务状态机，再把此字段推进到批次末尾；
	// 当 publishEntries 返回的完成通道非 nil 时，调用方仍需等待该通道关闭，才能
	// 把此索引当作业务状态已经真正执行完毕的边界。
	appliedIndex uint64

	// node 是 etcd/raft 提供的异步协议状态机句柄，用于提交提案、处理网络消息，
	// 并读取包含待持久化日志和已提交日志的 Ready。
	node raft.Node

	// raftStorage 保存 Raft 算法运行时需要读取的日志、HardState 和快照。
	// 它是内存存储，不替代 WAL；重启时仍需从磁盘快照和 WAL 重建其中的内容。
	raftStorage *raft.MemoryStorage

	// wal 是已打开的预写日志，用于持久化 HardState、日志条目和快照检查点。
	wal *wal.WAL

	// snapshotter 负责将完整的 raftpb.Snapshot 写入 snapdir 或从中加载。
	snapshotter *snap.Snapshotter

	// snapshotterReady 在快照组件初始化完成后将其发布给依赖方，避免依赖方在
	// snapdir 尚未准备好时读取快照。
	snapshotterReady chan *snap.Snapshotter

	// snapCount 是触发周期性快照的已应用日志数量阈值。
	// 例如上次快照索引为 100、snapCount 为 10 时，累计应用约 10 条新日志后，
	// 快照检查逻辑便可决定是否制作新快照；具体是否包含临界值由调用处的比较条件决定。
	snapCount uint64

	// transport 负责通过 HTTP 向其他 Raft 成员收发协议消息。
	transport *rafthttp.Transport

	// stopc 用于请求停止 Raft 主处理流程。
	stopc chan struct{}

	// httpstopc 用于请求停止 Raft HTTP 服务。
	httpstopc chan struct{}

	// httpdonec 在 Raft HTTP 服务完全退出后发出确认，使关闭流程可以等待网络资源
	// 释放完毕，而不是在发出停止请求后立即返回。
	httpdonec chan struct{}
}

// defaultSnapshotCount 是两次快照之间允许累计的已应用日志条数。
//
// 默认 10000 表示在上次快照后新增日志超过约一万条时尝试制作新快照。它控制
// 快照频率，不等同于快照后在内存中保留多少条日志；后者由 snapshotCatchUpEntriesN
// 单独控制。
var defaultSnapshotCount uint64 = 10000

// newRaftNode 构造并异步启动一个 Raft 成员。
//
// 返回的三个只读通道分别用于：
//   - commitC：向业务状态机发布已提交命令或 nil 快照重载信号；
//   - errorC：报告 Raft 后台不可恢复错误；
//   - snapshotterReady：发布初始化完成的快照读写器。
//
// id 按当前实现从 1 开始，并与 peers 中的顺序对应。例如 peers[0] 属于节点 1，
// peers[1] 属于节点 2。join=true 表示加入或重启现有集群，不使用 peers 再次引导
// 一套新的初始成员配置。
func newRaftNode(id int, peers []string, join bool, getSnapshot func() ([]byte, error), proposeC <-chan string, confChangeC <-chan *raftpb.ConfChange) (<-chan *commit, <-chan error, <-chan *snap.Snapshotter) {
	// 无缓冲通道让 Raft 发布流程与状态机消费形成背压，避免无限堆积提交批次。
	commitC := make(chan *commit)
	errorC := make(chan error)

	// WAL 和快照目录是相对于进程当前工作目录的节点专属目录。例如 id=2 时分别为
	// kvstore-2 与 kvstore-2-snap，避免同一环境中的不同成员互相覆盖数据。
	rc := raftNode{
		proposeC:    proposeC,
		confChangC:  confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,
		waldir:      fmt.Sprintf("kvstore-%d", id),
		snapdir:     fmt.Sprintf("kvstore-%d-snap", id),
		getSnapshot: getSnapshot,

		snapCount: defaultSnapshotCount,
		stopc:     make(chan struct{}),
		httpstopc: make(chan struct{}),
		httpdonec: make(chan struct{}),
	}

	// 注意：snapshotterReady 字段当前未在结构体字面量中 make。nil 通道既不能
	// 发送也不能接收，startRaft 发布 snapshotter 时会永久阻塞；这里保留现有实现。
	// 正常设计应在启动 goroutine 前初始化该通道。
	go rc.startRaft()
	return commitC, errorC, rc.snapshotterReady
}

// saveSnap 按“快照文件、WAL 快照检查点、释放旧 WAL 锁”的顺序持久化快照。
//
// WAL 中的 walpb.Snapshot 只记录索引、任期和成员配置，不包含业务状态数据；
// 完整数据由 snapshotter 写入独立的快照文件。先写快照文件再写 WAL 检查点很重要：
// 即使写 WAL 失败，磁盘上最多留下一个尚未被 WAL 引用的快照；反过来则可能让恢复
// 流程读到一个找不到对应快照文件的检查点。
//
// 例如快照覆盖到索引 120、任期为 7 时，快照文件保存索引 120 时的键值数据，
// WAL 检查点保存 {Index: 120, Term: 7, ConfState: ...}。重启恢复时先加载该快照，
// 再重放索引大于 120 的 WAL 日志，无需从第一条日志重新执行。
func (rc *raftNode) saveSnap(snap *raftpb.Snapshot) error {
	// 从完整快照中提取 WAL 恢复定位所需的最小元数据。
	walSnap := &walpb.Snapshot{
		Index:     snap.GetMetadata().Index,
		Term:      snap.GetMetadata().Term,
		ConfState: snap.GetMetadata().ConfState,
	}

	// 先保证完整快照已经落盘，再让 WAL 声明该快照可用于恢复。
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}

	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}

	// 快照和检查点均保存成功后，旧 WAL 段才不再需要一直持有文件锁；
	// 后续清理流程可以据此回收已被快照覆盖的历史 WAL 文件。
	return rc.wal.ReleaseLockTo(snap.GetMetadata().GetIndex())
}

// entriesToApply 从一批已提交日志中剔除本节点已经应用过的前缀。
//
// Raft 的 Ready 可能再次携带一部分已经处理过的日志，因此不能直接重复应用整个
// ents，否则“账户余额增加 10”之类的非幂等命令会执行两次。该方法同时检查日志
// 是否连续：新批次的第一条日志最多只能是 appliedIndex+1，不能在中间留下空洞。
//
// 例如 appliedIndex=102，ents 的索引为 [101, 102, 103, 104]，前两条已经应用，
// 因而应返回索引为 [103, 104] 的后缀。
func (rc *raftNode) entriesToApply(ents []*raftpb.Entry) (nents []*raftpb.Entry) {
	// 空批次无需过滤；直接返回还能保留调用方传入的 nil/空切片形态。
	if len(ents) == 0 {
		return ents
	}

	// Entry.Index 是 Raft 日志中的全局索引，不是 ents 内部从零开始的切片下标。
	firstIdx := ents[0].GetIndex()
	if firstIdx > rc.appliedIndex+1 {
		// firstIdx 大于期望的下一索引说明日志不连续。例如 appliedIndex=10、
		// firstIdx=12 时缺少索引 11，继续应用会破坏状态机的确定性。
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%]+1", firstIdx, rc.appliedIndex)
	}

	// appliedIndex-firstIdx+1 表示批次开头已有多少条日志被应用过。
	// 只有该数量小于批次长度时，ents 中才还包含需要应用的新日志。
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		// 注意：切片下标应是相对于 firstIdx 的偏移量，而 appliedIndex 是全局日志
		// 索引。以上述 [101, 102, 103, 104] 为例，正确偏移量是 2；直接把全局索引
		// 103 用作切片下标会越界。这里保留现有表达式，仅对这一易混淆点作出说明。
		nents = ents[rc.appliedIndex+1:]
	}

	return nents
}

// publishEntries 按顺序处理已提交日志，并把普通业务命令批量发布给上层状态机。
//
// 返回的只读通道用于等待业务命令全部应用完成；批次中没有业务命令时返回 nil。
// 第二个返回值表示发布流程能否继续：true 表示本批次已成功交付或无需交付，false
// 表示节点正在停止，或者当前成员已被配置变更移出集群。
//
// Raft 日志分为两类：EntryNormal 的 Data 是业务命令，EntryConfChange 的 Data
// 是成员配置变更。两者都占用日志索引，但只有非空的普通日志需要发送到 commitC。
func (rc *raftNode) publishEntries(ents []*raftpb.Entry) (<-chan struct{}, bool) {
	if len(ents) == 0 {
		return nil, true
	}

	// 最多每条日志都包含一条业务命令，预分配容量可避免追加时反复扩容。
	data := make([]string, 0, len(ents))
	for i := range ents {
		switch ents[i].GetType() {
		case raftpb.EntryNormal:
			if len(ents[i].GetData()) == 0 {
				// Leader 当选后可能提交不含业务数据的空日志，用于确认其任期内的
				// 提交位置；该日志参与推进索引，但不应交给键值状态机执行。
				break
			}

			// string 与 []byte 的转换保留原始字节内容，这里不负责解码业务命令；
			// 真正的反序列化和执行由 commitC 的消费者完成。
			s := string(ents[i].GetData())
			data = append(data, s)
		case raftpb.EntryConfChange:
			// 配置变更使用 protobuf 编码。无法解码意味着已提交日志损坏或生产端
			// 编码不匹配，当前实现选择立即终止，而不是跳过后造成成员视图分裂。
			var cc raftpb.ConfChange
			if err := proto.Unmarshal(ents[i].Data, &cc); err != nil {
				log.Fatalf("failed to unmarshal conf change: %v", ents[i])
			}

			// ApplyConfChange 只更新 Raft 协议层的投票成员集合，并返回制作快照时
			// 需要保存的 ConfState；HTTP 地址仍需在 transport 中单独维护。
			rc.confState = rc.node.ApplyConfChange(&cc)
			switch cc.GetType() {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					// 本实现约定 Context 保存新成员的 Raft HTTP 地址。例如新增
					// NodeID=3 时，Context 可为 "http://127.0.0.1:12380"。
					rc.transport.AddPeer(types.ID(cc.GetNodeId()), []string{string(cc.Context)})
				}

			case raftpb.ConfChangeRemoveNode:
				if cc.GetNodeId() == uint64(rc.id) {
					// 当前成员一旦被共识日志移除，就不能继续参与投票或复制日志。
					log.Panicln("I've been removed from the cluster! Shutting down.")
					return nil, false
				}

				// 协议层成员关系已在 ApplyConfChange 中更新，此处再断开对应的
				// HTTP 对等连接，避免继续向已移除成员发送 Raft 消息。
				rc.transport.RemovePeer(types.ID(cc.GetNodeId()))
			}
		}
	}

	var applyDoneC chan struct{}
	if len(data) > 0 {
		// commit 中把通道暴露为只发送方向，而本方法保留双向引用并以只接收方向
		// 返回给调用者：状态机负责关闭它，Raft 处理流程负责等待它。
		applyDoneC = make(chan struct{}, 1)
		select {
		case rc.commitC <- &commit{
			data:       data,
			applyDoneC: applyDoneC,
		}:
		case <-rc.stopc:
			// commitC 无消费者时，stopc 仍能打断阻塞发送并结束节点。
			return nil, false
		}
	}

	// 配置变更和空普通日志虽然不会进入 data，也同样属于已经处理的日志，所以直接
	// 使用本批次最后一条日志的索引。若 applyDoneC 非 nil，调用方仍需等待状态机确认。
	rc.appliedIndex = ents[len(ents)-1].GetIndex()
	return applyDoneC, true
}

// loadSnapshot 加载同时被快照目录和 WAL 检查点认可的最新快照。
//
// 快照文件与 WAL 分开持久化，磁盘上可能出现没有对应 WAL 检查点的“孤儿快照”。
// 例如快照文件已写到索引 120，但写 WAL 检查点失败，恢复时就不能贸然采用 120；
// LoadNewestAvailable 会从有效 WAL 快照记录中选择能够匹配的最新快照文件。
//
// 该方法把“没有快照”视为正常的首次启动状态；其他读取或校验错误会直接终止进程。
func (rc *raftNode) loadSnapshot() *raftpb.Snapshot {
	if wal.Exist(rc.waldir) {
		// 只有索引不大于 WAL 最新已提交 HardState 的快照记录才属于有效候选项。
		walSnaps, err := wal.ValidSnapshotEntries(zap.NewExample(), rc.waldir)
		if err != nil {
			log.Fatalf("kvstore: error listing snapshots (%v)", err)
		}

		// 从新到旧寻找元数据与有效 WAL 记录匹配的快照；损坏或不匹配的文件
		// 不会被当作恢复基准。
		snapshot, err := rc.snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil && !errors.Is(err, snap.ErrNoSnapshot) {
			log.Fatalf("kvstore: error loading snapshot (%v)", err)
		}
		return snapshot
	}

	// WAL 尚不存在表示没有可验证的历史快照，返回空快照统一后续启动流程。
	// 空快照的 Metadata 为 nil，区别于包含索引和任期的有效快照。
	return &raftpb.Snapshot{}
}

// openWAL 打开与给定快照衔接的 WAL；首次启动时会先创建一份空 WAL。
//
// snapshot 决定 WAL 的读取起点。例如快照元数据为 Index=120、Term=7 时，
// 后续 ReadAll 应从该检查点继续恢复索引大于 120 的日志。传入空快照则从 WAL
// 起点恢复。该方法只负责创建或打开文件，真正读取记录由后续恢复流程完成。
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) {
		// 目录权限 0750 表示所有者可读、写、进入，同组用户可读和进入，其他用户
		// 无权限。os.Mkdir 只创建最后一级目录，因此 waldir 的父目录必须已存在。
		if err := os.Mkdir(rc.waldir, 0o750); err != nil {
			log.Fatalf("kvstore: cannot create dir for wal (%v)", err)
		}

		// wal.Create 初始化 WAL 必需的元数据和首个日志段。创建后先关闭，再通过
		// wal.Open 以统一路径按照指定快照检查点重新打开。
		w, err := wal.Create(zap.NewExample(), rc.waldir, nil)
		if err != nil {
			log.Fatalf("kvstore: create wal error (%v)", err)
		}
		if err := w.Close(); err != nil {
			log.Fatalf("kvstore: close wal error (%v)", err)
		}
	}

	// 零值检查点 {Index: 0, Term: 0} 表示没有可用快照，需要从 WAL 起点读取。
	walsnap := walpb.Snapshot{}
	if snapshot.GetMetadata() != nil {
		// 打开 WAL 只需用索引和任期定位对应检查点；ConfState 已保存在完整快照中。
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}

	log.Printf("loading WAL at term %d and index %d", walsnap.GetTerm(), walsnap.GetIndex())

	// wal.Open 会定位包含检查点的日志段，并准备从该快照之后读取；若 WAL 中找不到
	// 匹配的 Index/Term，继续恢复可能产生错误状态，因此当前实现直接终止。
	w, err := wal.Open(zap.NewExample(), rc.waldir, &walsnap)
	if err != nil {
		log.Fatalf("kvstore: error loading wal (%v)", err)
	}
	return w
}

// replayWAL 从最新有效快照和其后的 WAL 记录重建 Raft 内存存储。
//
// 恢复顺序必须是“快照 → HardState → 后续日志”。例如快照覆盖到索引 100，
// WAL 还包含 101～120，则 MemoryStorage 先应用索引 100 的快照，再设置持久化的
// term/vote/commit，最后追加 101～120，供 raft.RestartNode 继续运行。
func (rc *raftNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", rc.id)

	// loadSnapshot 只选择有有效 WAL 检查点背书的快照；openWAL 从该检查点打开。
	snapshot := rc.loadSnapshot()
	w := rc.openWAL(snapshot)

	// ReadAll 返回 WAL 元数据、HardState 和日志。此项目没有使用创建 WAL 时的
	// 自定义 metadata，因此忽略第一个返回值。
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("kvstore: failed to read WAL (%v)", err)
	}

	// MemoryStorage 是 Raft 协议运行期读取日志的来源，必须与磁盘恢复结果一致。
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		// ApplySnapshot 把内存日志基线推进到快照索引。当前实现忽略其错误，
		// 调用方必须确保只恢复有效且不落后于当前状态的快照。
		rc.raftStorage.ApplySnapshot(snapshot)
	}

	// HardState 包含当前任期、已投票成员和提交索引，不属于业务快照数据。
	rc.raftStorage.SetHardState(st)

	// ents 仅包含打开快照检查点之后仍需保留的 Raft 日志。
	rc.raftStorage.Append(ents)
	return w
}

// writeError 发布后台错误并停止节点。
//
// 先停止 HTTP 并关闭 commitC，可让业务状态机退出提交消费循环；随后发送 errorC，
// 使等待方能够区分正常关闭与异常退出。
func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

// startRaft 初始化磁盘状态、Raft 协议节点和节点间 HTTP 传输。
//
// 该方法由 newRaftNode 在独立 goroutine 中调用；初始化完成后，serveRaft 负责
// 网络服务，serveChannels 负责 Tick、Ready、提案和关闭流程。
func (rc *raftNode) startRaft() {
	// 快照目录不存在时创建。0750 不允许其他用户访问持久化的业务数据。
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0o750); err != nil {
			log.Fatalf("kvstore: cannot create dir for snapshot (%v)", err)
		}
	}

	// Snapshotter 本身不持有业务状态，只负责 snapdir 中快照文件的读写和校验。
	rc.snapshotter = snap.New(zap.NewExample(), rc.snapdir)

	// 必须在 replayWAL 之前记录 WAL 是否原本存在，因为 replayWAL 在首次启动时会
	// 创建 WAL；该布尔值决定后面使用 RestartNode 还是 StartNode。
	oldwal := wal.Exist(rc.waldir)
	rc.wal = rc.replayWAL()

	// 发布快照组件，使 kvstore 可以先恢复业务快照再消费提交日志。
	// snapshotterReady 若为 nil，此发送会永久阻塞，后续初始化都不会发生。
	rc.snapshotterReady <- rc.snapshotter

	// 当前示例把 peers 的切片位置映射为从 1 开始的 Raft ID。
	// 例如 len(peers)=3 会得到初始成员 {1, 2, 3}。
	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}

	// Tick 的实际时间由 serveChannels 中 100ms 的 ticker 决定：
	// HeartbeatTick=1 约为 100ms，ElectionTick=10 约为 1s。
	c := &raft.Config{
		ID:            uint64(rc.id),
		ElectionTick:  10,
		HeartbeatTick: 1,
		Storage:       rc.raftStorage,
		// 单条 Raft 网络消息最多携带约 1 MiB 日志数据。
		MaxSizePerMsg: 1024 * 1024,
		// 每个 Follower 最多允许 256 条正在传输但尚未确认的追加消息。
		MaxInflightMsgs: 256,
		// Leader 最多积压约 1 GiB 尚未提交的日志载荷，防止提案无限占用内存。
		MaxUncommittedEntriesSize: 1 << 30,
	}

	// 有旧 WAL 时必须按恢复出的 HardState 和日志继续运行。join=true 同样不能
	// 重新提交初始 peers，否则会把加入现有集群误当作创建新集群。
	if oldwal || rc.join {
		rc.node = raft.RestartNode(c)
	} else {
		rc.node = raft.StartNode(c, rpeers)
	}

	// rafthttp.Transport 只负责 Raft 协议消息，不承载面向客户端的键值 HTTP API。
	// ClusterID 用于拒绝来自其他逻辑集群的消息；同一集群所有成员必须保持一致。
	rc.transport = &rafthttp.Transport{
		Logger:      zap.NewExample(),
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewExample(), strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}

	// Start 初始化传输层内部 goroutine；随后为除自己以外的初始成员配置地址。
	rc.transport.Start()
	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}

	// 网络接收和 Raft 状态推进相互独立：前者把消息交给 Process，后者从 Ready
	// 持久化结果并发送出站消息。
	go rc.serveRaft()
	go rc.serveChannels()
}

// stop 执行没有附带错误的节点关闭流程。
//
// 与 writeError 不同，它只关闭 errorC，不发送错误值。通道关闭会通知所有等待者
// 节点生命周期结束。该方法及 writeError 都要求只调用一次，否则重复 close 会 panic。
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

// stopHTTP 停止 rafthttp 传输层，并等待 HTTP Serve goroutine 完全退出。
//
// httpstopc 是停止请求，httpdonec 是完成确认；两阶段握手避免节点在监听端口和连接
// 尚未释放时就继续执行后续关闭操作。
func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

// publishSnapshot 把一个从其他节点收到的新快照发布给业务状态机。
//
// 此处的“发布”不同于 saveSnap：saveSnap 负责磁盘持久化，publishSnapshot 负责
// 通知 kvstore 重新加载该快照，并同步本节点的成员配置与应用进度。只有索引严格
// 大于 appliedIndex 的快照才代表更新的状态。
func (rc *raftNode) publishSnapshot(snapshotToSave *raftpb.Snapshot) {
	// 空快照是 Ready 的正常零值，不包含需要发布的状态。
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	// 注意：这里打印的是更新前的 rc.snapshotIndex，而不是 snapshotToSave 的索引；
	// 因此“publishing”日志可能显示旧值，defer 日志则会读取更新后的新值。
	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rc.snapshotIndex)

	// 应用旧快照会让状态机倒退。例如当前已应用到 120 时，索引 100 的快照必须拒绝。
	if snapshotToSave.GetMetadata().GetIndex() <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.GetIndex(), rc.appliedIndex)
	}

	// nil commit 是 kvstore.readCommits 约定的控制信号：从 snapshotter 重新加载
	// 完整业务状态，而不是把 nil 当作一批空日志。
	rc.commitC <- nil

	// 快照是截至其 Metadata.Index 的完整状态，因此三个恢复基准需要同步推进。
	rc.confState = snapshotToSave.Metadata.GetConfState()
	rc.snapshotIndex = snapshotToSave.Metadata.GetIndex()
	rc.appliedIndex = snapshotToSave.Metadata.GetIndex()
}

// snapshotCatchUpEntriesN 是制作快照后仍保留在 MemoryStorage 中的历史日志窗口。
//
// 保留最近约 10000 条日志可以让稍微落后的 Follower 通过增量日志追赶；只有落后
// 超过该窗口的节点才需要接收体积通常更大的完整快照。
var snapshotCatchUpEntriesN uint64 = 10000

// maybeTriggerSnapshot 在累计日志超过阈值时制作业务快照并压缩内存日志。
//
// applyDoneC 对应本批次业务命令的应用确认。制作快照前必须等待它关闭，否则
// appliedIndex 可能已经推进，但 getSnapshot 读到的 map 仍未包含对应命令，
// 最终生成“元数据索引比业务数据更新”的不一致快照。
func (rc *raftNode) maybeTriggerSnapshot(applyDoneC <-chan struct{}) {
	// uint64 减法要求 appliedIndex >= snapshotIndex；正常流程通过单调递增索引维持
	// 该不变量。差值不超过阈值时，继续保留增量日志比频繁制作快照更经济。
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	if applyDoneC != nil {
		// 同时监听 stopc，避免状态机不再确认时阻塞节点关闭。
		select {
		case <-applyDoneC:
		case <-rc.stopc:
			return
		}
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)

	// getSnapshot 序列化业务 map；它不包含 Raft 的索引、任期或 ConfState。
	data, err := rc.getSnapshot()
	if err != nil {
		log.Panic(err)
	}

	// CreateSnapshot 把业务数据与 appliedIndex 处的 Raft 任期、成员配置组合成
	// 完整快照。confState 使接收方无需重放更早的成员变更日志。
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, rc.confState, data)
	if err != nil {
		panic(err)
	}

	// 必须先成功持久化，才能压缩被该快照覆盖的内存日志。
	if err := rc.saveSnap(snap); err != nil {
		panic(err)
	}

	// 默认至少从索引 1 开始；当 appliedIndex 足够大时，只回收追赶窗口之前的日志。
	// 例如 appliedIndex=25000、窗口=10000，则 compactIndex=15000。
	compactIndex := uint64(1)
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}

	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		// raft.ErrCompacted 通常表示目标索引早已被压缩，可视为幂等结果；其他错误
		// 才更值得终止。当前判断恰好只对 ErrCompacted 执行 panic，并忽略其他错误，
		// 此处保留既有实现，仅明确该行为。
		if errors.Is(err, raft.ErrCompacted) {
			panic(err)
		}
	} else {
		log.Printf("compacted log at index %d", compactIndex)
	}

	// 只有快照保存和内存压缩流程结束后，才推进下一轮快照的比较基准。
	rc.snapshotIndex = rc.appliedIndex
}

// serveChannels 驱动 Raft 的核心事件循环。
//
// 它同时负责四类事件：定时 Tick、Node.Ready 的持久化与发布、传输层错误、节点停止。
// 另一个内部 goroutine 把业务提案和配置变更送入 raft.Node。除状态机 map 外，
// raftNode 的大部分推进状态都集中在此流程中顺序修改。
func (rc *raftNode) serveChannels() {
	// MemoryStorage.Snapshot 返回当前恢复基线。启动时三项状态都从同一个快照元数据
	// 初始化，避免成员配置、快照索引和应用索引彼此不一致。
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.GetMetadata().ConfState
	rc.snapshotIndex = snap.GetMetadata().GetIndex()
	rc.appliedIndex = snap.GetMetadata().GetIndex()

	// serveChannels 是 WAL 运行期所有者；事件循环退出时关闭文件和相关锁。
	defer rc.wal.Close()

	// 每个 tick 为 100ms，结合 Config 中的 HeartbeatTick 和 ElectionTick 换算实际周期。
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// 提案接收独立于 Ready 处理，避免外部发送者被磁盘写入和网络发送长期阻塞。
	go func() {
		// ConfChange.Id 用于区分配置变更提案；计数器在本次进程生命周期内递增。
		confChangeCount := uint64(0)

		// 当前条件要求两个输入通道都仍然有效：其中任意一个关闭并被置为 nil 后，
		// 循环都会退出并停止整个节点，而不是继续单独消费另一个通道。
		for rc.proposeC != nil && rc.confChangC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					// nil 通道在 select 中永远不可用，常用于禁用已经关闭的分支。
					rc.proposeC = nil
				} else {
					// Propose 只把命令送入本地 Raft；能否提交取决于当前是否有 Leader
					// 以及能否联系多数派。context.TODO 不提供取消或超时。
					rc.node.Propose(context.TODO(), []byte(prop))
				}
			case cc, ok := <-rc.confChangC:
				if !ok {
					rc.confChangC = nil
				} else {
					confChangeCount++
					// 同一运行期内为配置变更分配递增 ID，然后作为 Raft 日志提议。
					cc.Id = new(confChangeCount)
					rc.node.ProposeConfChange(context.TODO(), cc)
				}

			}
		}

		// 两类提案输入中任意一类结束都会触发节点整体关闭。
		close(rc.stopc)
	}()

	for {
		select {
		case <-ticker.C:
			// Tick 推进逻辑时钟，可能触发心跳、选举超时或重新发送消息。
			rc.node.Tick()
		case rd := <-rc.node.Ready():
			// Ready 是一批必须按 Raft 约定处理的工作：持久化状态和日志、应用快照、
			// 发送消息、发布已提交条目，最后调用 Advance 确认处理完成。
			if !raft.IsEmptySnap(rd.Snapshot) {
				// 先把收到或生成的快照持久化。当前实现忽略 saveSnap 返回的错误；
				// 若磁盘写入失败，后续继续 Advance 可能丢失恢复所需状态。
				rc.saveSnap(rd.Snapshot)
			}

			// 空 HardState 表示本批次的 term/vote/commit 没有变化；向 WAL 传 nil
			// 可以只保存 rd.Entries，而无需重复写入零值 HardState。
			var hs *raftpb.HardState
			if !raft.IsEmptyHardState(rd.HardState) {
				hs = rd.HardState
			}

			// 在发送依赖这些状态的网络消息前，必须先把 HardState 和新日志写入 WAL。
			// 当前实现没有检查 Save 的错误，生产代码通常应把失败交给 writeError。
			rc.wal.Save(hs, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				// 磁盘保存成功后再更新内存存储，并通知业务状态机加载完整快照。
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}

			// rd.Entries 是尚未加入 MemoryStorage 的不稳定日志；它不同于下面已经
			// 达成多数派提交、可交给状态机执行的 rd.CommittedEntries。
			rc.raftStorage.Append(rd.Entries)

			// 日志和 HardState 已持久化后才发送出站消息，避免对外宣称本节点拥有
			// 一份崩溃后无法恢复的状态。
			rc.transport.Send(rc.processMessages(rd.Messages))

			// 过滤重复日志，再按类型处理业务命令和成员配置变更。
			applyDoneC, ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries))
			if !ok {
				rc.stop()
				return
			}

			// 只有达到快照阈值时 maybeTriggerSnapshot 才等待 applyDoneC；未达到阈值
			// 时会直接返回，因此当前流程可能在业务状态机真正执行完本批次前 Advance。
			rc.maybeTriggerSnapshot(applyDoneC)

			// Advance 告诉 raft.Node：当前 Ready 已处理，可以释放内部引用并产生下一批。
			rc.node.Advance()
		case err := <-rc.transport.ErrorC:
			// 传输层错误通常意味着节点间通信服务无法继续，按异常流程关闭并上报。
			rc.writeError(err)
			return
		case <-rc.stopc:
			// 提案输入关闭或外部停止信号触发正常关闭。
			rc.stop()
			return
		}
	}
}

// processMessages 为待发送的快照消息补充最新成员配置。
//
// 普通 Append、Heartbeat 等消息保持不变。MsgSnap 的 ConfState 必须与快照代表的
// 集群成员关系一致，否则接收方恢复数据后可能使用错误的投票成员集合。
func (rc *raftNode) processMessages(ms []*raftpb.Message) []*raftpb.Message {
	// 结果切片复用原有消息指针，只新建切片头；对 MsgSnap 的修改也会反映到 ms。
	var messages []*raftpb.Message
	for i := 0; i < len(ms); i++ {
		if ms[i].GetType() == raftpb.MsgSnap {
			ms[i].Snapshot.Metadata.ConfState = rc.confState
		}
		messages = append(messages, ms[i])
	}
	return messages
}

// serveRaft 在当前成员的 peer 地址上提供 rafthttp 服务。
//
// peers 使用从 1 开始的节点 ID，因此节点 id 对应 peers[id-1]。该 HTTP 服务仅
// 收发 Raft 协议消息，客户端 GET/PUT 请求由 serveHTTPKVAPI 的独立端口处理。
func (rc *raftNode) serveRaft() {
	// url.Parse 负责拆出 scheme 和 Host；监听器只需要 host:port 部分。
	// id 必须处于 [1, len(peers)]，否则 peers[id-1] 会发生切片越界。
	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("kvstore: Failed parsing URL (%v)", err)
	}

	// stoppableListener 使 stopHTTP 可以通过关闭 httpstopc 中断阻塞的 Accept。
	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("kvstore: Failed to listen rafthttp (%v)", err)
	}

	// Transport.Handler 校验并解码对端 Raft 消息，随后通过 Process 交给 raft.Node。
	// http.Server.Serve 在监听器停止时会返回一个非 nil 错误，因此需结合 httpstopc
	// 判断它是预期关闭还是意外故障。
	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
		// 收到停止信号后的 Serve 错误属于正常关闭。
	default:
		log.Fatalf("kvstore: Failed to serve rafthttp (%v)", err)
	}

	// 通知 stopHTTP：服务循环已经退出，监听资源可以视为释放完成。
	close(rc.httpdonec)
}

// Process 实现 rafthttp.Raft.Process，把收到的协议消息交给本地 Raft 状态机。
func (rc *raftNode) Process(ctx context.Context, m *raftpb.Message) error {
	return rc.node.Step(ctx, m)
}

// IsIDRemoved 实现 rafthttp.Raft.IsIDRemoved。
//
// 当前实现始终返回 false，表示传输层不会仅凭本回调拒绝某个历史成员 ID；
// 成员移除后的连接清理由 publishEntries 显式调用 transport.RemovePeer 完成。
func (rc *raftNode) IsIDRemoved(_ uint64) bool { return false }

// ReportUnreachable 实现 rafthttp.Raft.ReportUnreachable，通知 Raft 某成员暂时不可达。
//
// Raft 会据此调整向该 Follower 复制日志的进度状态，但不会自动把它移出集群。
func (rc *raftNode) ReportUnreachable(id uint64) { rc.node.ReportUnreachable(id) }

// ReportSnapshot 实现 rafthttp.Raft.ReportSnapshot，报告一次快照发送成功或失败。
//
// 该反馈让 Leader 决定把对应 Follower 的复制状态推进到快照之后，还是稍后重试。
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}
