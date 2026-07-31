package kvstore

import (
	"flag"
	"strings"

	"go.etcd.io/raft/v3/raftpb"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-val server port")
	join := flag.Bool("join", false, "join an existing cluster")

	proposeC := make(chan string)
	defer close(proposeC)
	confChangC := make(chan *raftpb.ConfChange)
	defer close(confChangC)

	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	commitC, errorC, snapshotterReady := newRaftNode(*id, strings.Split(*cluster, ","), *join, getSnapshot, proposeC, confChangC)

	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)
	serveHTTPKVAPI(kvs, *kvport, confChangC, errorC)
}
