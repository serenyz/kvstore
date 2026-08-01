package kvstore

import (
	"context"

	"go.etcd.io/raft/v3/raftpb"
)

// revision 是状态机为每次有效修改分配的单调递增逻辑版本号。
// 它与 Raft 日志索引含义不同：一条不改变数据的删除命令也会占用日志索引，
// 但不会推进 revision。
type revision uint64

// NoneRevision 表示调用方未指定历史版本，此时查询使用状态机的当前版本。
const NoneRevision revision = 0

// record 描述某个键在一次修改之后的完整状态。
//
// 例如键 k 首次写入 v1 后，其 Version 为 1；再次写入 v2 时会保留
// CreateRevision，同时更新 ModRevision 并把 Version 增加到 2。删除操作会生成
// Deleted 为 true 的墓碑记录，而不是直接丢弃历史。
type record struct {
	// Value 是该版本保存的字符串值；墓碑记录中的值为空字符串。
	Value string
	// CreateRevision 是该键首次出现时的逻辑版本。
	CreateRevision revision
	// ModRevision 是生成当前记录时的逻辑版本。
	ModRevision revision
	// Version 是该键累计产生的记录数，从 1 开始。
	Version uint64
	// Deleted 标记该记录是否为删除墓碑。
	Deleted bool
}

// value 按 ModRevision 升序保存一个键的全部历史记录。
type value []*record

// proposal 是一次等待提交给 raft.Node 的普通业务提案。
// resultC 只返回 Propose 调用是否接收成功，不代表日志已被多数派提交或应用。
type proposal struct {
	ctx     context.Context
	data    string
	resultC chan error
}

// confChangeProposal 封装成员增删提案及其同步调用结果。
type confChangeProposal struct {
	ctx        context.Context
	confChange *raftpb.ConfChange
	resultC    chan error
}

// readIndexRequest 封装一次 Raft ReadIndex 请求。
// requestCtx 会原样出现在对应的 raft.ReadState 中，用于关联并发读取。
type readIndexRequest struct {
	ctx        context.Context
	requestCtx []byte
	resultC    chan error
}

// CommandType 标识复制日志中业务命令的操作类型。
type CommandType uint64

const (
	// CommandPut 写入或覆盖一个键，并生成新的历史版本。
	CommandPut CommandType = iota
	// CommandDelete 为已存在且尚未删除的键追加墓碑记录。
	CommandDelete
)

// command 是通过 gob 编码并由 Raft 复制的状态机命令。
// RequestID 用于把命令最终的应用结果交还给发起该命令的请求。
type command struct {
	CommandType CommandType
	RequestID   string
	Key         string
	Val         string
}

// applyResult 保存一条业务命令应用后的记录或错误。
type applyResult struct {
	record record
	err    error
}

// kvSnap 是业务状态机写入 Raft 快照 Data 字段的 JSON 数据结构。
// 它同时保存全局逻辑版本和各键历史，确保恢复后仍可执行历史版本查询。
type kvSnap struct {
	CurRevision revision
	KvStore     map[string]value
}

// commit 是从 Raft 协调层交给业务状态机的一批已提交普通日志。
//
// Raft 的 committed 只表示日志已经满足共识条件，本地 map 可能尚未执行它。状态机
// 必须依次处理 data，并在全部完成后关闭 applyDoneC。比如同一批包含 x=1、x=2，
// 顺序应用后的结果必须是 x=2，不能并发执行或交换顺序。
type commit struct {
	// data 保留普通日志的提交顺序，每项都是一条 gob 编码的 command。
	data []string

	// applyDoneC 的方向约束把关闭/发送所有权交给状态机；raftLogIndex 记录 data 中
	// 最后一条业务日志的 Raft 索引，供 ReadIndex 等待逻辑推进本地应用水位。
	applyDoneC   chan<- struct{}
	raftLogIndex uint64
}
