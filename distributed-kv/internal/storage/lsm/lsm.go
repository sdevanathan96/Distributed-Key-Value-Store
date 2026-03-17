package lsm

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
)

const MaxLevels = 7

var LevelMaxBytes = [MaxLevels]int64{
	0,
	10 * 1024 * 1024,
	100 * 1024 * 1024,
	1024 * 1024 * 1024,
	10 * 1024 * 1024 * 1024,
	100 * 1024 * 1024 * 1024,
	1024 * 1024 * 1024 * 1024,
}

const L0CompactionThreshold = 4

type LSMTree struct {
	mu         sync.RWMutex
	wg         sync.WaitGroup
	compacting atomic.Bool
	levels     [MaxLevels]*Level
	manifest   *Manifest
	dir        string
}

func NewLSMTree(ssTableDir string, manifestDir string) (*LSMTree, error) {
	manifest := NewManifest(manifestDir)
	var levels [MaxLevels]*Level
	for i := 0; i < MaxLevels; i++ {
		levels[i] = NewLevel(i)
	}
	for _, sstInfo := range manifest.data.SSTables {
		table, err := OpenSSTable(sstInfo.Path, sstInfo.Level)
		if err != nil {
			log.Printf("open sstable %v error: %v", sstInfo.Path, err)
			continue
		}
		levels[sstInfo.Level].AddSSTable(table)
	}
	return &LSMTree{
		levels:   levels,
		manifest: manifest,
		dir:      ssTableDir,
	}, nil
}
func (l *LSMTree) Get(key []byte) (Entry, bool, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, level := range l.levels {
		entry, isThere, err := level.Get(key)
		if err != nil {
			return Entry{}, false, err
		}
		if isThere {
			return entry, isThere, err
		}
	}
	return Entry{}, false, nil
}
func (l *LSMTree) AddSSTable(sst *SSTable, level int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.levels[level].AddSSTable(sst)
	err := l.manifest.AddSSTable(sst.path, level)
	if err != nil {
		return fmt.Errorf("add sstable %v error: %v", sst.path, err)
	}
	if level == 0 && l.levels[0].SSTableCount() >= L0CompactionThreshold {
		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			l.compact(0)
		}()
	}
	return nil
}
func (l *LSMTree) RemoveSSTable(path string, level int) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, table := range l.levels[level].sstables {
		if table.path == path {
			err := l.manifest.RemoveSSTable(path)
			if err != nil {
				return fmt.Errorf("remove sstable %v error: %v", path, err)
			}
			err = os.Remove(path)
			if err != nil {
				return fmt.Errorf("remove sstable %v error: %v", path, err)
			}
			l.levels[level].RemoveSSTable(path)
			return nil
		}
	}
	return nil
}

func (l *LSMTree) AllocateSSTableID() uint64 {
	return l.manifest.GetNextSSTableID()
}
func (l *LSMTree) GetLevel(num int) *Level {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.levels[num]
}

func (l *LSMTree) Close() error {
	l.wg.Wait()
	for _, level := range l.levels {
		for _, sst := range level.SSTables() {
			err := sst.Close()
			if err != nil {
				return fmt.Errorf("close sstable %v error: %v", sst.path, err)
			}
		}
	}
	return nil
}

func (l *LSMTree) WaitForBackground() {
	l.wg.Wait()
}
