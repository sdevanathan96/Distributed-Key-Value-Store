package raft

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {

	opts := []goleak.Option{
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*addrConn).resetTransport"),
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*Server).handleRawConn.func1"),
		goleak.IgnoreTopFunction("google.golang.org/grpc/internal/transport.(*controlBuffer).get"),
	}

	goleak.VerifyTestMain(m, opts...)
}
