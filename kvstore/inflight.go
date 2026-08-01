package kvstore

import (
	"container/heap"
	"errors"
	"math"
	"sync"
)

var (
	// errTooManyInflightRequests 表示并发等待中的请求已经达到容量上限。
	errTooManyInflightRequests = errors.New("too many inflight requests")
	// errRequestAlreadyRegistered 表示请求标识在当前等待集合中发生冲突。
	errRequestAlreadyRegistered = errors.New("request already registered")
)

// defaultMaxInflight 限制单个节点同时保留的写回执和 ReadIndex 等待项数量。
const defaultMaxInflight = 1000

// proposalInflight 按请求标识保存尚未收到状态机应用结果的写请求。
//
// 表中的通道容量为 1，因此 HTTP 请求即使已经超时并停止接收，状态机仍可投递一次
// 最终结果而不会阻塞日志应用流程。mu 保护注册、完成和超时清理之间的并发访问。
type proposalInflight struct {
	chs         map[string]chan *applyResult
	mu          sync.Mutex
	maxInflight uint64
}

// newProposalInflight 创建一个最多容纳 maxInflight 个写请求的回执表。
func newProposalInflight(maxInflight uint64) *proposalInflight {
	return &proposalInflight{
		chs:         make(map[string]chan *applyResult),
		maxInflight: maxInflight,
	}
}

// register 为 id 注册一次性结果通道。
// 容量超限或 id 已存在时不会修改现有映射。
func (p *proposalInflight) register(id string) (<-chan *applyResult, error) {
	resultC := make(chan *applyResult, 1)

	p.mu.Lock()
	defer p.mu.Unlock()
	if uint64(len(p.chs))+1 > p.maxInflight {
		return nil, errTooManyInflightRequests
	}

	if _, ok := p.chs[id]; ok {
		return nil, errRequestAlreadyRegistered
	}
	p.chs[id] = resultC
	return resultC, nil
}

// complete 原子地移除 id，并向原注册者投递最终应用结果。
// 未找到 id 表示请求已经取消或清理，此时结果会被丢弃。
func (p *proposalInflight) complete(id string, result *applyResult) {
	p.mu.Lock()
	resultC, ok := p.chs[id]
	if ok {
		delete(p.chs, id)
	}
	p.mu.Unlock()

	if ok {
		resultC <- result
	}
}

// remove 仅在 id 仍指向 expected 时取消注册。
//
// expected 检查可避免延迟清理误删同名的新请求：旧请求 A 超时后，若 id 已经被请求 B
// 重新注册，A 的 defer 不应移除 B 的结果通道。
func (p *proposalInflight) remove(id string, expected <-chan *applyResult) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if cur, ok := p.chs[id]; ok && expected == cur {
		delete(p.chs, id)
	}
}

// readIndexWithSignal 保存一次线性读取需要等待的 Raft 日志索引和完成信号。
type readIndexWithSignal struct {
	index uint64
	ch    chan struct{}
}

// minHeap 按 readIndexWithSignal.index 维护请求标识的最小堆。
// chs 存放请求数据，ids 是堆数组，positions 支持按标识进行 O(log n) 删除和修复。
type minHeap struct {
	chs       map[string]*readIndexWithSignal
	ids       []string
	positions map[string]int
}

// Len 实现 heap.Interface。
func (h *minHeap) Len() int {
	return len(h.ids)
}

// Less 实现 heap.Interface，使堆顶始终是目标索引最小的请求。
func (h *minHeap) Less(i, j int) bool {
	return h.chs[h.ids[i]].index < h.chs[h.ids[j]].index
}

// Swap 实现 heap.Interface，并同步维护两个请求的新位置。
func (h *minHeap) Swap(i, j int) {
	h.ids[i], h.ids[j] = h.ids[j], h.ids[i]
	h.positions[h.ids[i]] = i
	h.positions[h.ids[j]] = j
}

// Push 实现 heap.Interface，把请求标识追加到堆尾；上浮由 container/heap 完成。
func (h *minHeap) Push(x any) {
	id := x.(string)
	h.positions[id] = len(h.ids)
	h.ids = append(h.ids, id)
}

// Pop 实现 heap.Interface，移除由 container/heap 调整到切片末尾的请求标识。
func (h *minHeap) Pop() any {
	n := len(h.ids)
	id := h.ids[n-1]
	h.ids[n-1] = ""
	h.ids = h.ids[:n-1]
	delete(h.positions, id)
	return id
}

// readIndexInflight 关联 ReadIndex 请求、Raft 返回的读取索引和等待信号。
// chs 与 heap.chs 指向同一个 map；所有字段都由 mu 统一保护。
type readIndexInflight struct {
	heap         *minHeap
	chs          map[string]*readIndexWithSignal
	mu           sync.Mutex
	appliedIndex uint64
	maxInflight  uint64
}

// newReadIndexInflight 创建带容量限制的 ReadIndex 等待集合。
func newReadIndexInflight(maxInflight uint64) *readIndexInflight {
	chs := make(map[string]*readIndexWithSignal)
	positions := make(map[string]int)
	h := &minHeap{chs: chs, positions: positions}
	heap.Init(h)
	return &readIndexInflight{
		chs:         chs,
		heap:        h,
		maxInflight: maxInflight,
	}
}

// register 登记一个尚未取得 ReadState 的请求。
// 初始索引设为 math.MaxUint64，收到 Raft 回执后再由 updateReadIndex 更新。
func (r *readIndexInflight) register(id string) (<-chan struct{}, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if uint64(r.heap.Len())+1 > r.maxInflight {
		return nil, errTooManyInflightRequests
	}

	if _, ok := r.heap.chs[id]; ok {
		return nil, errRequestAlreadyRegistered
	}

	rdxCh := make(chan struct{}, 1)
	sig := &readIndexWithSignal{
		index: math.MaxUint64,
		ch:    rdxCh,
	}
	r.chs[id] = sig
	heap.Push(r.heap, id)
	return rdxCh, nil
}

// updateReadIndex 写入 Raft 为 id 确认的安全读取索引，并恢复堆序。
// id 已被取消或堆位置不存在时返回 false。
func (r *readIndexInflight) updateReadIndex(id string, index uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	item, ok := r.chs[id]
	if !ok {
		return false
	}

	position, ok := r.heap.positions[id]
	if !ok {
		return false
	}

	item.index = index
	heap.Fix(r.heap, position)
	r.maybeSignalReady()
	return true
}

// maybeSignal 依据当前已应用日志索引检查等待集合，并向满足比较条件的请求发送信号。
// 信号通道带有一个缓冲，即使请求方已因超时返回也不会阻塞状态机应用协程。
func (r *readIndexInflight) maybeSignalReady() {
	for r.heap.Len() > 0 {
		top := r.heap.ids[0]
		if r.chs[top].index > r.appliedIndex {
			break
		}

		top = heap.Pop(r.heap).(string)
		r.chs[top].ch <- struct{}{}
		delete(r.chs, top)
	}
}

// remove 取消仍与 expected 通道匹配的 ReadIndex 请求，并同步更新堆和映射。
func (r *readIndexInflight) remove(id string, expected <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if t, ok := r.chs[id]; ok && t.ch == expected {
		heap.Remove(r.heap, r.heap.positions[id])
		delete(r.chs, id)
	}
}

func (r *readIndexInflight) advanceApplied(index uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if index > r.appliedIndex {
		r.appliedIndex = index
	}

	r.maybeSignalReady()
}
