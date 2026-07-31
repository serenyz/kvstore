package kvstore

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync"

	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/raft/v3/raftpb"
)

// kvstore 是由 Raft 已提交日志驱动的内存键值状态机。
//
// 所有节点只要以相同顺序应用相同的 kv 命令，就会得到相同的 kvStore 内容。
// 业务写入不能直接修改 map，而必须先经 proposeC 提交给 Raft；读取则访问本节点
// 当前已经应用的状态，因此它不天然等价于一次经过 Leader 确认的线性一致读。
type kvstore struct {
	// proposeC 把序列化后的写命令发送给 Raft 提案流程。
	proposeC chan<- string

	// mu 保护 kvStore。状态机应用日志、HTTP 查询和快照序列化可能并发发生，
	// 因而所有 map 访问都必须持锁，避免 Go map 的并发读写错误。
	mu sync.Mutex

	// kvStore 保存本节点已经应用的键值状态。
	kvStore map[string]string

	// snapshotter 用于读取 Raft 层已经持久化的完整业务快照。
	snapshotter *snap.Snapshotter
}

// kv 是写入命令的传输结构，使用 gob 在提案方与状态机之间编码和解码。
//
// 字段必须导出，否则 encoding/gob 无法编码它们。例如 kv{Key: "name",
// Val: "alice"} 表示把键 name 更新为 alice。
type kv struct {
	Key string
	Val string
}

// newKVStore 创建键值状态机，先从已有快照恢复，再异步消费 Raft 提交日志。
//
// proposeC 用于发送新写入；commitC 只包含已经被 Raft 提交的命令；errorC 报告
// 后台 Raft 流程的终止错误。恢复必须先于 readCommits 启动，否则新日志可能先于
// 快照应用，导致新状态随后被旧快照覆盖。
func newKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *commit, errorC <-chan error) *kvstore {
	s := &kvstore{
		proposeC:    proposeC,
		kvStore:     make(map[string]string),
		snapshotter: snapshotter,
	}

	// 这里直接调用 Load，并把包括 snap.ErrNoSnapshot 在内的所有错误都视为致命错误；
	// 与下方 loadSnapshot 将“无快照”转换为 (nil, nil) 的行为不同。
	snapshot, err := snapshotter.Load()
	if err != nil {
		panic(err)
	}

	if snapshot != nil {
		// Snapshot.Data 是 getSnapshot 生成的业务 JSON；Metadata 则属于 Raft，
		// 记录该业务状态对应的日志索引、任期和成员配置。
		log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			log.Panic(err)
		}
	}

	// 提交日志在独立 goroutine 中顺序应用，避免构造函数阻塞等待整个节点生命周期。
	go s.readCommits(commitC, errorC)

	// 注意：按照构造函数的返回类型和上面的初始化过程，这里通常应返回 s；
	// 当前代码返回 nil，调用方若直接使用返回值会发生空指针问题。此处仅作说明，
	// 不在注释任务中改变既有行为。
	return s
}

// Lookup 返回本节点状态机中 key 当前对应的值，以及该键是否存在。
//
// 第二个返回值用于区分“不存在”和“存在但值为空字符串”。例如 ("", false)
// 表示没有该键，而 ("", true) 表示该键存在且值就是空字符串。
func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.kvStore[key]
	return v, ok
}

// Propose 把一次键值更新编码为 Raft 提案。
//
// 该方法只保证命令已经写入 proposeC，不保证它已经被多数派提交或被本地状态机
// 应用。例如 Propose("x", "1") 返回后立刻 Lookup("x")，仍可能读到旧值。
func (s *kvstore) Propose(k string, v string) {
	// strings.Builder 实现 io.Writer，可直接承接 gob 的二进制编码结果；
	// Go string 也能保存任意字节，并不要求内容是 UTF-8 文本。
	var buf strings.Builder
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}

	// 若 Raft 提案消费者尚未启动且通道没有缓冲，此发送会阻塞，从而形成自然背压。
	s.proposeC <- buf.String()
}

// readCommits 按 Raft 提交顺序执行命令，并在每个批次完成后通知提交方。
//
// commitC 中的 nil 是控制信号，不是空命令：它要求状态机重新加载磁盘快照。
// 非 nil commit 的 data 必须保持原有顺序；批次全部执行后关闭 applyDoneC，
// Raft 层才能安全地以最新业务状态制作快照。
func (s *kvstore) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		if commit == nil {
			// Raft 层接收到远端快照后会发送 nil，通知业务状态机用快照整体替换
			// 当前 map。快照加载完成后继续消费其后的增量日志。
			snapshot, err := s.snapshotter.Load()
			if err != nil {
				log.Panic(err)
			}

			if snapshot != nil {
				log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := s.recoverFromSnapshot(snapshot.GetData()); err != nil {
					log.Panic(err)
				}
			}
			continue
		}

		for _, data := range commit.data {
			// 每个 data 都应是 Propose 中 gob 编码的一条 kv 命令。任何解码失败
			// 都表示复制日志中的业务载荷不符合协议，跳过会造成节点状态不一致。
			var dataKv kv
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&dataKv); err != nil {
				log.Fatalf("kvstore: could not decode message (%v)", err)
			}

			s.mu.Lock()
			s.kvStore[dataKv.Key] = dataKv.Val
			s.mu.Unlock()
		}

		// close 用作一次性广播：所有等待该批次完成的接收者都会立即被唤醒。
		// 每个 applyDoneC 只能关闭一次，因此其所有权属于此状态机消费流程。
		close(commit.applyDoneC)
	}

	// commitC 关闭表示不会再有已提交日志。随后读取 errorC，以区分正常关闭与
	// Raft 后台错误；若 errorC 已正常关闭，则 ok 为 false，不执行 Fatal。
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

// getSnapshot 在持锁状态下把整个键值状态机序列化为 JSON。
//
// 锁保证快照对应一个一致时刻。例如并发写入 x 和 y 时，不会得到只包含某次
// map 修改一半的状态。返回的字节会被放入 raftpb.Snapshot.Data。
func (s *kvstore) getSnapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.kvStore)
}

// loadSnapshot 读取最新业务快照，并把“尚无快照”规范化为 (nil, nil)。
//
// 调用方应同时检查 snapshot 和 err：nil snapshot 加 nil error 表示正常首次启动，
// 非 nil error 才表示快照文件损坏或底层 I/O 失败。
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

// recoverFromSnapshot 用 JSON 快照整体替换当前键值状态。
//
// 先在锁外反序列化可以缩短临界区；只有解析成功后才加锁替换 map，因此损坏快照
// 不会破坏当前仍可用的内存状态。例如 {"x":"1"} 会恢复为只包含 x=1 的新 map，
// 而不是与旧 map 做增量合并。
func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.kvStore = store
	return nil
}
