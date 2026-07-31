package kvstore

import (
	"errors"
	"net"
	"time"
)

// stoppableListener 是可通过 stopc 中断 Accept 的 TCP 监听器。
//
// 普通 net.TCPListener.Accept 会一直阻塞到新连接到达或监听器关闭。这里把
// AcceptTCP 放进 goroutine，并同时等待 stopc，使 Raft HTTP 服务可以响应节点
// 关闭信号；成功建立的连接还会启用 TCP keep-alive。
type stoppableListener struct {
	// 嵌入 TCPListener，使 stoppableListener 复用 Close、Addr 和 AcceptTCP。
	*net.TCPListener

	// stopc 只用于接收关闭信号；通常由拥有者通过 close(stopc) 广播停止。
	stopc <-chan struct{}
}

// newStoppableListener 在 addr 上创建 TCP 监听器，并绑定外部停止信号。
//
// 例如 addr="127.0.0.1:12380" 只监听本机回环地址；addr=":12380" 则监听
// 所有可用网络接口。由于 network 固定为 "tcp"，成功结果可以断言为 *net.TCPListener。
func newStoppableListener(addr string, stopc <-chan struct{}) (*stoppableListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &stoppableListener{ln.(*net.TCPListener), stopc}, nil
}

// Accept 等待一个新 TCP 连接、底层监听错误或停止信号三者之一。
//
// 该签名实现 net.Listener。内部结果通道容量为 1，保证外层因 stopc 返回后，
// AcceptTCP goroutine 即使稍后结束，也能写入结果并退出，不会再因无人接收而阻塞。
func (ln stoppableListener) Accept() (c net.Conn, err error) {
	// 连接和错误分开传递，便于 select 明确区分成功与失败。
	connc := make(chan *net.TCPConn, 1)
	errc := make(chan error, 1)

	go func() {
		// AcceptTCP 本身不可直接选择 stopc，因此放入辅助 goroutine。
		tc, err := ln.AcceptTCP()
		if err != nil {
			errc <- err
			return
		}
		connc <- tc
	}()
	select {
	case <-ln.stopc:
		// net/http.Server.Serve 收到此错误后会结束服务并关闭监听器，进而唤醒仍在
		// AcceptTCP 中等待的辅助 goroutine。
		return nil, errors.New("server stopped")
	case err := <-errc:
		return nil, err
	case tc := <-connc:
		// keep-alive 用于发现长时间无业务流量但对端已经异常断开的 Raft 连接。
		// 3 分钟是探测间隔，不是应用层请求超时。
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(3 * time.Minute)
		return tc, nil
	}
}
