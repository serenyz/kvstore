package kvstore

import (
	"errors"
	"net"
	"time"
)

// stoppableListener 为 Raft HTTP 服务提供可取消的 TCP Accept。
//
// net.TCPListener.AcceptTCP 本身不能同时等待外部通道，因此 Accept 使用辅助协程
// 接收连接，并在调用方关闭 stopc 时主动返回。每个成功连接都会启用 TCP keep-alive，
// 以便传输层最终发现已经失效的对等节点连接。
type stoppableListener struct {
	// 嵌入监听器以复用 net.Listener 所需的 Close 和 Addr 方法。
	*net.TCPListener

	// stopc 由 Raft 节点关闭，用于广播网络服务停止事件。
	stopc <-chan struct{}
}

// newStoppableListener 监听 addr，并将底层 TCP 监听器与 stopc 组合。
//
// 例如 "127.0.0.1:9021" 仅接受本机连接，而 ":9021" 接受所有网络接口上的连接。
// network 固定为 tcp，因此 net.Listen 成功后返回值的具体类型是 *net.TCPListener。
func newStoppableListener(addr string, stopc <-chan struct{}) (*stoppableListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &stoppableListener{ln.(*net.TCPListener), stopc}, nil
}

// Accept 实现 net.Listener，并等待连接、监听错误或 stopc 三类事件中的第一个。
//
// 两个结果通道都带一个缓冲。这样即使 stopc 先触发、Accept 已经返回，仍在
// AcceptTCP 中阻塞的协程也能在监听器随后关闭时投递结果并退出。
func (ln stoppableListener) Accept() (c net.Conn, err error) {
	// 分离成功连接和错误，使 select 的每个分支保持单一含义。
	connc := make(chan *net.TCPConn, 1)
	errc := make(chan error, 1)

	go func() {
		// 将不可取消的 AcceptTCP 转成可参与 select 的结果通道。
		tc, err := ln.AcceptTCP()
		if err != nil {
			errc <- err
			return
		}
		connc <- tc
	}()
	select {
	case <-ln.stopc:
		// Serve 收到非 nil 错误后结束；上层关闭监听器会解除辅助协程的 AcceptTCP。
		return nil, errors.New("server stopped")
	case err := <-errc:
		return nil, err
	case tc := <-connc:
		// 三分钟是 TCP keep-alive 探测周期，并不限制一次 Raft HTTP 请求的执行时间。
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(3 * time.Minute)
		return tc, nil
	}
}
