// kvctl drives a running distributed-kv cluster from the shell.
//
//	kvctl [flags] put <key> <value>
//	kvctl [flags] get <key>
//	kvctl [flags] delete <key>
//	kvctl [flags] status
//
// Flags must come before the subcommand; stdlib flag stops at the first
// non-flag argument.
//
// --addr takes CLIENT addresses (raft port + 1), repeated or comma separated.
// They become the client's seed list, so kvctl keeps working when the node you
// named is the one that just died. The default is the cluster-local.sh layout.
//
// Values go to stdout; which node served the call goes to stderr, so a
// redirect is visible without breaking `v=$(kvctl get k)`.
//
// Exit codes: 0 ok, 1 error, 2 key not found.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc/status"

	"distributed-kv/internal/client"
	"distributed-kv/proto/kvpb"
)

const (
	exitOK       = 0
	exitError    = 1
	exitNotFound = 2
)

const defaultAddrs = "127.0.0.1:9001,127.0.0.1:9011,127.0.0.1:9021"

// addrList is a flag.Value accepting repeated and comma separated addresses.
type addrList []string

func (a *addrList) String() string { return strings.Join(*a, ",") }

func (a *addrList) Set(v string) error {
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			*a = append(*a, s)
		}
	}
	return nil
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run holds all the logic so deferred cleanup executes before os.Exit.
func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("kvctl", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var addrs addrList
	fs.Var(&addrs, "addr", "client address(es), repeatable or comma separated (default "+defaultAddrs+")")
	// Must stay above Options.RequestTimeout, which must stay above the
	// server's 5s propose timeout.
	timeout := fs.Duration("timeout", 10*time.Second, "overall deadline for the operation")
	noFollow := fs.Bool("no-follow", false, "do not follow leader redirects; report them instead")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: kvctl [flags] put <key> <value> | get <key> | delete <key> | status")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return exitError
	}
	if len(addrs) == 0 {
		_ = addrs.Set(defaultAddrs)
	}

	opts := client.DefaultOptions()
	opts.FollowRedirects = !*noFollow
	c, err := client.New(addrs, opts)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitError
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	cmd, rest := fs.Arg(0), fs.Args()
	if len(rest) > 0 {
		rest = rest[1:]
	}
	switch {
	case cmd == "put" && len(rest) == 2:
		if err := c.Put(ctx, []byte(rest[0]), []byte(rest[1])); err != nil {
			return fail(stderr, err)
		}
		fmt.Fprintf(stderr, "ok (served by %s)\n", c.LastServed())
		return exitOK

	case cmd == "get" && len(rest) == 1:
		val, found, err := c.Get(ctx, []byte(rest[0]))
		if err != nil {
			return fail(stderr, err)
		}
		if !found {
			fmt.Fprintf(stderr, "not found (served by %s)\n", c.LastServed())
			return exitNotFound
		}
		fmt.Fprintf(stdout, "%s\n", val)
		fmt.Fprintf(stderr, "(served by %s)\n", c.LastServed())
		return exitOK

	case cmd == "delete" && len(rest) == 1:
		if err := c.Delete(ctx, []byte(rest[0])); err != nil {
			return fail(stderr, err)
		}
		fmt.Fprintf(stderr, "ok (served by %s)\n", c.LastServed())
		return exitOK

	case cmd == "status" && len(rest) == 0:
		st, err := c.ClusterStatus(ctx)
		if err != nil {
			return fail(stderr, err)
		}
		printStatus(stdout, st, c.LastServed())
		return exitOK

	default:
		fs.Usage()
		return exitError
	}
}

// printStatus writes one line per shard in key=value form so cluster-local.sh
// can grep it. A shard with no known leader prints leader=-.
func printStatus(w io.Writer, st *kvpb.ClusterStatusResponse, addr string) {
	fmt.Fprintf(w, "node=%s addr=%s\n", st.GetNodeId(), addr)
	for _, sh := range st.GetShards() {
		leader := sh.GetLeaderId()
		if leader == "" {
			leader = "-"
		}
		fmt.Fprintf(w, "  shard=%d leader=%s term=%d nodes=%s\n",
			sh.GetShardId(), leader, sh.GetTerm(), strings.Join(sh.GetNodes(), ","))
	}
}

// fail prints err, unpacking a not leader redirect so --no-follow output says
// where the leader is.
func fail(stderr io.Writer, err error) int {
	if st, ok := status.FromError(err); ok {
		for _, d := range st.Details() {
			if h, ok := d.(*kvpb.LeaderHint); ok {
				fmt.Fprintf(stderr, "error: %s: not leader, leader is %s\n", st.Code(), h.GetClientAddr())
				return exitError
			}
		}
		fmt.Fprintf(stderr, "error: %s: %s\n", st.Code(), st.Message())
		return exitError
	}
	if errors.Is(err, context.DeadlineExceeded) {
		fmt.Fprintln(stderr, "error: timed out")
		return exitError
	}
	fmt.Fprintf(stderr, "error: %v\n", err)
	return exitError
}
