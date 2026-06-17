package server

import (
	"fmt"
	"net"

	"google.golang.org/grpc"

	"distributed-kv/proto/kvpb"
)

// KVServer wraps the gRPC server hosting the KV API on the client port.
// Both main.go and the integration tests use this so the wiring lives in one
// place.
type KVServer struct {
	grpc *grpc.Server
	lis  net.Listener
}

// NewKVServer binds clientAddr and registers the KV API. It does NOT start
// serving; call Serve. Separating bind from serve lets the caller know the
// listener succeeded (port free) before launching the serve goroutine.
func NewKVServer(s *Server, clientAddr string) (*KVServer, error) {
	lis, err := net.Listen("tcp", clientAddr)
	if err != nil {
		return nil, fmt.Errorf("kv listen %s: %w", clientAddr, err)
	}
	g := grpc.NewServer()
	kvpb.RegisterKVServiceServer(g, NewKVAPI(s))
	return &KVServer{grpc: g, lis: lis}, nil
}

// Serve blocks serving requests. Run it in a goroutine. It returns when Stop
// is called (grpc.Serve returns nil on GracefulStop/Stop).
func (k *KVServer) Serve() error {
	return k.grpc.Serve(k.lis)
}

// Stop gracefully drains in-flight RPCs then stops. Call before Server.Stop so
// no new writes arrive at shards that are about to shut down.
func (k *KVServer) Stop() {
	k.grpc.GracefulStop()
}

// Addr returns the actual bound address, useful when clientAddr used :0.
func (k *KVServer) Addr() string {
	return k.lis.Addr().String()
}
