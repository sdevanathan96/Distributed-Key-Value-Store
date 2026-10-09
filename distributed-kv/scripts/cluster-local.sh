#!/usr/bin/env bash
#
# cluster-local.sh: run a 3 node distributed-kv cluster on localhost.
#
#   ./scripts/cluster-local.sh start          # build, launch 3 nodes, wait for every shard's leader
#   ./scripts/cluster-local.sh status         # kvctl status against each node
#   ./scripts/cluster-local.sh kill node-0    # SIGTERM one node (graceful), keep its data dir
#   ./scripts/cluster-local.sh crash node-0   # SIGKILL one node (no shutdown path), keep its data dir
#   ./scripts/cluster-local.sh revive node-0  # restart it on the SAME data dir
#   ./scripts/cluster-local.sh stop           # stop all, keep data
#   ./scripts/cluster-local.sh wipe           # stop all, delete data
#   ./scripts/cluster-local.sh ctl get k      # kvctl seeded with all three nodes
#
# revive must reuse the same --data-dir: that is what exercises loadPersisted()
# and WAL replay in a real process.
#
# SHARDS (default 3) is fixed per data dir; a node refuses to start on data
# written with a different count, so wipe before changing it.
# RUN_DIR (default /tmp/distributed-kv-local) holds bin/, data/, logs/, pids/.

set -euo pipefail

# Raft ports 9000/9010/9020; main.go derives each client port as raft + 1, so
# clients are 9001/9011/9021. Gaps of 10 keep raft + 1 clear of the next node.
# 127.0.0.1 rather than localhost so leader hints (built from these addresses)
# match the seed addresses exactly.
NODES=(node-0 node-1 node-2)
RAFT_PORTS=(9000 9010 9020)
HOST=127.0.0.1
SHARDS="${SHARDS:-3}"
RUN_DIR="${RUN_DIR:-/tmp/distributed-kv-local}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NODE_BIN="$RUN_DIR/bin/node"
KVCTL="$RUN_DIR/bin/kvctl"

die() {
	echo "cluster-local: $*" >&2
	exit 1
}

index_of() {
	local i
	for i in "${!NODES[@]}"; do
		if [[ "${NODES[$i]}" == "$1" ]]; then
			echo "$i"
			return 0
		fi
	done
	die "unknown node '$1' (want one of ${NODES[*]})"
}

client_addr() { echo "$HOST:$((RAFT_PORTS[$1] + 1))"; }

all_client_addrs() {
	local i out=""
	for i in "${!NODES[@]}"; do
		out="$out${out:+,}$(client_addr "$i")"
	done
	echo "$out"
}

# main.go wants --peers as id=addr pairs for the OTHER nodes only.
peers_for() {
	local i out=""
	for i in "${!NODES[@]}"; do
		[[ "$i" == "$1" ]] && continue
		out="$out${out:+,}${NODES[$i]}=$HOST:${RAFT_PORTS[$i]}"
	done
	echo "$out"
}

pid_file() { echo "$RUN_DIR/pids/$1.pid"; }

is_running() {
	local pf
	pf=$(pid_file "$1")
	[[ -f "$pf" ]] && kill -0 "$(cat "$pf")" 2>/dev/null
}

build() {
	mkdir -p "$RUN_DIR/bin"
	(cd "$ROOT" && go build -o "$NODE_BIN" ./cmd/node && go build -o "$KVCTL" ./cmd/client)
}

launch() {
	local i=$1 id=${NODES[$1]}
	is_running "$id" && die "$id is already running (pid $(cat "$(pid_file "$id")"))"
	mkdir -p "$RUN_DIR/data/$id" "$RUN_DIR/logs" "$RUN_DIR/pids"
	nohup "$NODE_BIN" \
		--node-id "$id" \
		--raft-addr "$HOST:${RAFT_PORTS[$i]}" \
		--data-dir "$RUN_DIR/data/$id" \
		--peers "$(peers_for "$i")" \
		--shards "$SHARDS" \
		>>"$RUN_DIR/logs/$id.log" 2>&1 &
	echo $! >"$(pid_file "$id")"
	echo "started $id pid $! raft=$HOST:${RAFT_PORTS[$i]} client=$(client_addr "$i")"
}

# fail_if_dead dies with the log tail of any listed node that has exited, so a
# port clash or a refused layout surfaces as itself, not as a timeout.
fail_if_dead() {
	local id
	for id in "$@"; do
		if ! is_running "$id"; then
			echo "$id exited; last log lines:" >&2
			tail -n 20 "$RUN_DIR/logs/$id.log" >&2 || true
			die "$id is not running"
		fi
	done
}

# wait_for_leaders <addrs> <seconds> <node ids...>: poll kvctl status against
# addrs until it lists every shard with a leader. Polls rather than sleeping a
# fixed interval: an election is 150 to 300ms, but a slow start is seconds.
wait_for_leaders() {
	local addrs=$1 seconds=$2 out deadline
	shift 2
	deadline=$(($(date +%s) + seconds))
	while (($(date +%s) < deadline)); do
		fail_if_dead "$@"
		if out=$("$KVCTL" --addr "$addrs" --timeout 2s status 2>/dev/null) &&
			[[ $(grep -c 'shard=' <<<"$out") -eq $SHARDS ]] &&
			! grep -q 'leader=-' <<<"$out"; then
			echo "$out"
			return 0
		fi
		sleep 0.2
	done
	die "no leader for every shard within ${seconds}s; see $RUN_DIR/logs"
}

stop_node() {
	local id=$1 signal=${2:-TERM} pf pid
	pf=$(pid_file "$id")
	[[ -f "$pf" ]] || return 0
	pid=$(cat "$pf")
	if kill -0 "$pid" 2>/dev/null; then
		kill "-$signal" "$pid"
		for _ in $(seq 1 50); do
			kill -0 "$pid" 2>/dev/null || break
			sleep 0.1
		done
		if kill -0 "$pid" 2>/dev/null; then
			echo "$id still running 5s after SIG$signal, sending SIGKILL" >&2
			kill -KILL "$pid"
		fi
		echo "stopped $id (SIG$signal)"
	fi
	rm -f "$pf"
}

cmd_start() {
	local i
	build
	for i in "${!NODES[@]}"; do
		launch "$i"
	done
	wait_for_leaders "$(all_client_addrs)" 15 "${NODES[@]}"
}

cmd_status() {
	local i id
	for i in "${!NODES[@]}"; do
		id=${NODES[$i]}
		echo "== $id ($(client_addr "$i"))"
		if is_running "$id"; then
			"$KVCTL" --addr "$(client_addr "$i")" --timeout 2s status || true
		else
			echo "  not running"
		fi
	done
}

cmd_revive() {
	local i
	i=$(index_of "$1")
	[[ -x "$NODE_BIN" ]] || die "no binary at $NODE_BIN; run start first"
	launch "$i"
	# Ask the revived node itself, so this waits until it has rejoined every
	# shard's group and learned each leader.
	wait_for_leaders "$(client_addr "$i")" 15 "$1"
}

usage() {
	sed -n '3,12p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
	exit 1
}

[[ $# -ge 1 ]] || usage
cmd=$1
shift
case "$cmd" in
start) cmd_start ;;
status) cmd_status ;;
kill)
	[[ $# -eq 1 ]] || usage
	index_of "$1" >/dev/null
	stop_node "$1" TERM
	;;
crash)
	[[ $# -eq 1 ]] || usage
	index_of "$1" >/dev/null
	stop_node "$1" KILL
	;;
revive)
	[[ $# -eq 1 ]] || usage
	cmd_revive "$1"
	;;
stop) for id in "${NODES[@]}"; do stop_node "$id"; done ;;
wipe)
	for id in "${NODES[@]}"; do stop_node "$id"; done
	rm -rf "$RUN_DIR"
	echo "wiped $RUN_DIR"
	;;
ctl)
	[[ -x "$KVCTL" ]] || die "no kvctl at $KVCTL; run start first"
	exec "$KVCTL" --addr "$(all_client_addrs)" "$@"
	;;
*) usage ;;
esac
