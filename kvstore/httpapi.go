package kvstore

import (
	"io"
	"log"
	"net/http"
	"strconv"

	"go.etcd.io/raft/v3/raftpb"
)

// httpKVAPI 把 HTTP 请求转换为键值查询、Raft 写提案或成员配置变更。
//
// API 约定如下：
//   - PUT /foo，请求体为 bar：提议写入键 /foo、值 bar；
//   - GET /foo：读取本节点当前已经应用的 /foo；
//   - POST /3，请求体为节点地址：提议新增 ID 为 3 的成员；
//   - DELETE /3：提议移除 ID 为 3 的成员。
type httpKVAPI struct {
	// store 提供本地读取和业务写提案能力。
	store *kvstore

	// confChangeC 把成员增删请求交给 Raft 配置变更流程。
	confChangeC chan<- *raftpb.ConfChange
}

// ServeHTTP 根据请求方法处理键值操作或集群成员变更。
//
// PUT、POST 和 DELETE 都采用“乐观响应”：请求成功写入内部通道后即返回 204，
// 不等待 Raft 多数派提交。因此 204 只表示节点接受了请求，不保证变更已经生效。
// GET 则直接读取本节点状态机，Follower 落后时可能返回旧值。
func (h *httpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// RequestURI 保留开头的 "/"，并且可能包含查询字符串。例如请求
	// /name?lang=zh 会把整个 "/name?lang=zh" 当作键，而不只是 URL.Path。
	key := r.RequestURI

	// 无论处理成功还是提前返回，都关闭请求体以便 HTTP 连接复用。
	defer r.Body.Close()

	switch r.Method {
	case http.MethodPut:
		// PUT 的完整请求体就是值；当前实现没有大小限制，大请求会整体读入内存。
		v, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}

		h.store.Propose(key, string(v))

		// 这里只完成 Raft 提案，没有等待提交确认；紧接着 GET 同一键仍可能读到旧值。
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		// Lookup 的 ok 可以区分键不存在和键存在但值为空。
		if v, ok := h.store.Lookup(key); ok {
			// 写响应失败通常表示客户端已断开；当前简单示例忽略该错误。
			w.Write([]byte(v))
		} else {
			http.Error(w, "Failed to GET", http.StatusNotFound)
		}
	case http.MethodPost:
		// POST 请求体约定为待添加节点的 Raft HTTP 地址，例如
		// http://127.0.0.1:12380；地址会放入 ConfChange.Context 随日志复制。
		url, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		// 去掉路径开头的 "/" 后解析节点 ID。base=0 既接受十进制 3，也接受
		// 带前缀的十六进制 0x3；位数 64 与 Raft 的 uint64 NodeID 一致。
		nodeID, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on POST", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:    raftpb.ConfChangeAddNode.Enum(),
			NodeId:  new(nodeID),
			Context: url,
		}

		// 该发送可能因无消费者而阻塞；成功发送仍不等于配置变更已经提交。
		h.confChangeC <- &cc
		w.WriteHeader(http.StatusNoContent)
	case http.MethodDelete:
		// DELETE /3 表示通过一条 Raft 配置日志移除成员 3。
		nodeID, err := strconv.ParseUint(key[1:], 0, 64)
		if err != nil {
			log.Printf("Failed to convert ID for conf change (%v)\n", err)
			http.Error(w, "Failed on DELETE", http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:   raftpb.ConfChangeRemoveNode.Enum(),
			NodeId: new(nodeID),
		}
		h.confChangeC <- &cc

		// 与 PUT、POST 相同，此处不等待多数派提交配置变更。
		w.WriteHeader(http.StatusNoContent)
	default:
		// 多个 Allow 头字段会由 net/http 作为同名多值响应头发送给客户端。
		w.Header().Set("Allow", http.MethodPut)
		w.Header().Add("Allow", http.MethodGet)
		w.Header().Add("Allow", http.MethodPost)
		w.Header().Add("Allow", http.MethodDelete)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// serveHTTPKVAPI 启动面向客户端的键值 HTTP 服务，并等待 Raft 后台流程结束。
//
// port 监听所有网络接口上的对应端口，例如 port=8080 会使用地址 ":8080"。
// 服务本身在独立 goroutine 中运行；当前函数阻塞读取 errorC，从而让上层生命周期
// 与 Raft 节点保持一致。
func serveHTTPKVAPI(kv *kvstore, port int, confChangeC chan<- *raftpb.ConfChange, errorC <-chan error) {
	srv := http.Server{
		Addr: ":" + strconv.Itoa(port),
		Handler: &httpKVAPI{
			store:       kv,
			confChangeC: confChangeC,
		},
	}

	go func() {
		// ListenAndServe 正常关闭时也会返回 http.ErrServerClosed；当前实现没有
		// 单独区分正常关闭，而是把任何返回错误都作为致命错误处理。
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// errorC 关闭且没有错误时 ok=false，函数正常返回；收到错误则终止进程。
	// 注意 kvstore.readCommits 也读取同一个 errorC，多个消费者会竞争单条错误消息。
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}
