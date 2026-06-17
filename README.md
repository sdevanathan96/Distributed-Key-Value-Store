# distributed-kv

A small distributed key value store written in Go. Writes are replicated across a cluster of nodes using the Raft consensus algorithm, data is stored on disk in an LSM tree, and clients talk to it over gRPC.

It runs today as one replicated group of nodes. A way to split data across several groups for scale is designed and described below, but not built.

## What it does

- Stores key value pairs and copies every write to every node, so data survives a node going down.
- Offers a small API over gRPC: set a key, read a key, delete a key, and check cluster status.
- Persists data to disk and recovers it on restart.
- Sends a client to the right node automatically when it connects to the wrong one.

## How it works

A node is one process. A three node cluster is three processes that elect a leader among themselves and keep their copies of the data in sync.

```
  client  (gRPC: set / read / delete / status)
     |
     v
  +------------------------------------------------------+
  | one node (one process)                               |
  |                                                      |
  |   gRPC API on the client port                        |
  |     routes the key to a shard                        |
  |       shard: handles the request                     |
  |         Raft  <-- talks to peer nodes -->  other nodes
  |         storage (LSM tree on disk)                   |
  +------------------------------------------------------+
```

Each node listens on two ports: one for talking to peer nodes, and one for the client API. By convention the client port is the peer port plus one.

**Writing a key.** The write has to go to the leader. If a client sends it to a follower, the follower replies with the leader's address and the client retries there. The leader records the write in its replicated log and sends it to the other nodes. Once a majority of nodes have it, the write is committed, saved to the leader's storage, and only then is the client told it succeeded.

**Reading a key.** The leader answers from its own storage directly, without going through the log. This is fast. See the consistency note in the decisions below for the trade-off.

## Architecture decisions

The choices that shape the system, and why they were made.

### A write is confirmed only after it is saved, not just after the cluster agrees on it

When the leader tells a client a write succeeded, the data is already written to the leader's own storage. So if you read the same key from that leader immediately after, you will see your write. Confirming earlier, as soon as the cluster agreed, would be faster but a quick follow up read could miss the write, because saving to storage lags agreement.

### Only the leader serves reads, straight from local storage

Reads skip the replication log, which keeps them fast. The trade-off is that reads can be slightly out of date, and if the leader changes between your write and your read, the new leader may not have saved your write yet, so a read there could miss it. Stronger read consistency is a planned addition.

### Clients are redirected, never proxied

A write sent to a follower is refused, with the leader's address attached to the error. The client retries against the leader. The cluster never forwards the write internally on the client's behalf. This keeps each node simple and makes routing visible to the client.

### Retried writes are safe

If a client times out and retries, the same write might be stored twice. Because setting or deleting a key twice has the same result as doing it once, this causes no harm, so the system does not need to track and remove duplicate requests.

### A node that cannot save its state stops

Before it acts, a node must durably record its Raft state (its term, its vote, and its log). If that disk write fails, the node can no longer participate safely, so it shuts down rather than risk corrupting the agreement among nodes.

### Storage is an LSM tree

Writes go to an append only log and an in memory table, which is flushed to sorted files on disk and later merged in the background. This favors fast writes and is the same approach used by stores like Cassandra and RocksDB. Reads check the in memory table first, then the files on disk, using a small in memory index and a bloom filter to avoid unnecessary disk reads.

### One replicated group now, sharding designed for later

All data lives in a single Raft group. To grow past what one group can hold, the design splits keys across several groups using a consistent hash ring. This is left unbuilt on purpose: on a small cluster it would behave identically to what exists today, so it only earns its complexity at larger scale.

## Build and run

The Go module is in the `distributed-kv` directory.

```bash
cd distributed-kv
go build ./...
```

### Run a local three node cluster

Each node needs a unique id, a peer address, a data directory, and the addresses of the other nodes. The client API binds the peer port plus one. Space the peer ports by at least two so the client ports do not overlap.

```bash
go run ./cmd/node \
  --node-id=node-0 --raft-addr=127.0.0.1:9000 --data-dir=/tmp/kv0 \
  --peers=node-1=127.0.0.1:9010,node-2=127.0.0.1:9020   # client API on 127.0.0.1:9001

go run ./cmd/node \
  --node-id=node-1 --raft-addr=127.0.0.1:9010 --data-dir=/tmp/kv1 \
  --peers=node-0=127.0.0.1:9000,node-2=127.0.0.1:9020   # client API on 127.0.0.1:9011

go run ./cmd/node \
  --node-id=node-2 --raft-addr=127.0.0.1:9020 --data-dir=/tmp/kv2 \
  --peers=node-0=127.0.0.1:9000,node-1=127.0.0.1:9010   # client API on 127.0.0.1:9021
```

A command line client is the next thing to be built. Until then the gRPC API can be driven with any gRPC tool. A write sent to a follower comes back with the leader's address attached, so the client knows where to retry.

## Testing

Tests run with the race detector and check for leaked goroutines.

```bash
# Everything
go test -race ./...

# The consensus layer, repeated to catch timing flakiness
go test -race -count=10 ./internal/raft/...

# End to end: single node, and a three node cluster with leader redirect
go test -race -count=5 -run TestKVEndToEnd ./internal/server/...

# Storage under concurrency
go test -race -count=100 ./internal/storage/lsm/...
```

## Project layout

```
internal/
  raft/        Raft: leader election, log replication, durable state
  storage/     LSM engine: write ahead log, in memory table, flush
    lsm/       on disk files, compaction, iterators, bloom filter
  shard/       ties one Raft node to one storage engine, applies committed writes
  server/      gRPC server, request routing, API handlers
  sharding/    the key to shard router (single shard for now)
proto/
  raftpb/      messages nodes use to talk to each other
  kvpb/         the client API
cmd/
  node/        the node program
  client/      command line client (next to be built)
```

## Limitations

Each of these is a deliberate choice, not an accident.

- **Reads can be stale.** Reads come from one node's local storage, and there is no guarantee you can read your own write right after a leader change. Stronger consistency is planned.
- **No range scans.** You can read a single key, not a range of keys. The storage layer already merges sorted files internally, so a scan would build on that, but it is not exposed yet.
- **One shard.** Only a single replicated group is built. The multi group design above is the way to scale further when a single group is not enough.
- **Not deployed yet.** Running it in containers and on Kubernetes is the next step.

## What's next

- Run it on a local Kubernetes cluster: a command line client, a container image, and the Kubernetes configuration to run a stable three node set.
- Run it on a cloud Kubernetes cluster, with the infrastructure defined as code.
- Build the multi group sharding described above, but only once the cluster is large enough for it to make a real difference.
