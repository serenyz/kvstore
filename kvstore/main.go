package kvstore

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
)

// Main 运行单个 kvstore 成员，并把启动或运行期错误记录为致命错误。
// 独立 cmd/kvstore 程序调用该入口。
func Main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// run 解析节点参数，装配 Raft、业务状态机和客户端 gRPC 服务，并协调关闭顺序。
//
// 例如以 -id=2 和三个 -cluster 地址启动时，节点使用地址列表中的第二项提供 Raft
// 通信，并在 -port 指定的端口提供键值 API。退出时先关闭三类输入通道，让 Raft
// 主循环停止，再持续读取 raftErrorC，直到后台资源全部释放。
func run() error {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value gRPC server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	proposeC := make(chan *proposal)
	confChangC := make(chan *confChangeProposal)
	readIndexC := make(chan *readIndexRequest)
	var closeInputsOnce sync.Once
	// 多条错误路径可能同时要求停止输入，sync.Once 保证这些通道只被关闭一次。
	closeInputs := func() {
		closeInputsOnce.Do(func() {
			close(proposeC)
			close(confChangC)
			close(readIndexC)
		})
	}
	defer closeInputs()

	var kvs *kvstore
	// Raft 在状态机完成构造后才能请求业务快照；提前调用返回显式错误。
	getSnapshot := func() ([]byte, error) {
		if kvs == nil {
			return nil, errors.New("state machine is not initialized")
		}
		return kvs.getSnapshot()
	}
	commitC, readStatesC, raftErrorC, snapshotter, err := newRaftNode(
		*id,
		strings.Split(*cluster, ","),
		*join,
		getSnapshot,
		proposeC,
		confChangC,
		readIndexC,
	)
	if err != nil {
		return fmt.Errorf("start Raft node: %w", err)
	}

	var stateMachineErrorC <-chan error
	kvs, stateMachineErrorC, err = newKVStore(snapshotter, proposeC, readIndexC, commitC, readStatesC)
	if err != nil {
		// newRaftNode 已经启动后台流程，关闭输入并等待 errorC 结束，避免资源泄漏。
		closeInputs()
		return fmt.Errorf("initialize key-value store: %w", err)
	}
	// RPC 服务结束后，无论原因如何都停止 Raft 输入，并等待节点完成最终清理。
	serveErr := serveRPCAPI(kvs, *kvport, confChangC, raftErrorC, stateMachineErrorC)
	closeInputs()
	return serveErr
}
