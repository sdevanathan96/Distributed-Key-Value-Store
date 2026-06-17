package shard

import (
	"distributed-kv/internal/raft"
	"distributed-kv/internal/storage"
	"distributed-kv/proto/raftpb"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	proto "google.golang.org/protobuf/proto"
)

func TestApplier(t *testing.T) {
	dir := t.TempDir()
	config := storage.DefaultConfig(dir)
	config.MemTableSize = 1024

	engine, err := storage.NewEngine(config)
	require.NoError(t, err)

	sh := &Shard{
		ID:      0,
		raft:    nil, // no real raft in apply path test
		engine:  engine,
		waiters: make(map[string]chan error),
	}

	applyCh := make(chan raft.ApplyMsg, 10) // test owns this channel
	sh.wg.Add(1)
	go sh.consumeApply(applyCh)

	t.Cleanup(func() {
		close(applyCh) // test closes it; consumeApply drains then exits
		sh.wg.Wait()
		if err := engine.Close(); err != nil {
			t.Errorf("engine close: %v", err)
		}
	})

	waiter := sh.RegisterWaiter("req-1")
	defer sh.DeleteWaiter("req-1")
	cmd := &raftpb.Command{
		RequestId: "req-1",
		Op:        raftpb.Op_OP_PUT,
		Key:       []byte("key1"),
		Value:     []byte("v"),
	}
	data, err := proto.Marshal(cmd)
	require.NoError(t, err)
	applyCh <- raft.ApplyMsg{
		CommandValid: true,
		Command:      data,
		CommandIndex: 1,
	}

	select {
	case err := <-waiter:
		require.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("apply did not complete within 1s")
	}

	val, err := sh.engine.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), val)

	waiter2 := sh.RegisterWaiter("req-2")
	defer sh.DeleteWaiter("req-2")
	cmdNew := &raftpb.Command{
		RequestId: "req-2",
		Op:        raftpb.Op_OP_DELETE,
		Key:       []byte("key1"),
	}
	dataNew, err := proto.Marshal(cmdNew)
	require.NoError(t, err)
	applyCh <- raft.ApplyMsg{
		CommandValid: true,
		Command:      dataNew,
		CommandIndex: 2,
	}
	select {
	case err := <-waiter2:
		require.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("apply did not complete within 1s")
	}
	_, err = sh.engine.Get([]byte("key1"))
	require.True(t, errors.Is(err, storage.ErrKeyNotFound))
}

func TestApplyWithoutWaiter(t *testing.T) {
	dir := t.TempDir()
	config := storage.DefaultConfig(dir)
	engine, err := storage.NewEngine(config)
	require.NoError(t, err)

	sh := &Shard{
		ID:      0,
		raft:    nil, // no real raft in apply path test
		engine:  engine,
		waiters: make(map[string]chan error),
	}

	applyCh := make(chan raft.ApplyMsg, 10) // test owns this channel
	sh.wg.Add(1)
	go sh.consumeApply(applyCh)

	t.Cleanup(func() {
		close(applyCh) // test closes it; consumeApply drains then exits
		sh.wg.Wait()
		if err := engine.Close(); err != nil {
			t.Errorf("engine close: %v", err)
		}
	})

	cmd := &raftpb.Command{
		RequestId: "no-waiter",
		Op:        raftpb.Op_OP_PUT,
		Key:       []byte("key2"),
		Value:     []byte("v2"),
	}

	sentinel := &raftpb.Command{
		RequestId: "sentinel",
		Op:        raftpb.Op_OP_PUT,
		Key:       []byte("sentinel-key"),
		Value:     []byte("ignored"),
	}
	data, err := proto.Marshal(cmd)
	require.NoError(t, err)

	sentinelData, err := proto.Marshal(sentinel)
	require.NoError(t, err)
	applyCh <- raft.ApplyMsg{
		CommandValid: true,
		Command:      data,
		CommandIndex: 1,
	}
	sentinelWaiter := sh.RegisterWaiter("sentinel")
	defer sh.DeleteWaiter("sentinel")
	applyCh <- raft.ApplyMsg{Command: sentinelData, CommandValid: true, CommandIndex: 2}

	select {
	case err := <-sentinelWaiter:
		require.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("sentinel apply did not complete within 1s")
	}

	val, err := sh.engine.Get([]byte("key2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)
}
