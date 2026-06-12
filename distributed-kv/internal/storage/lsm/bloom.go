package lsm

import (
	"bytes"
	"fmt"

	"github.com/bits-and-blooms/bloom/v3"
)

type BloomFilter struct {
	filter *bloom.BloomFilter
	count  uint
}

// NewBloomFilter returns a bloom filter sized for estimatedKeys at the target
// falsePositiveRate.
func NewBloomFilter(estimatedKeys uint, falsePositiveRate float64) *BloomFilter {
	bloomFilter := bloom.NewWithEstimates(estimatedKeys, falsePositiveRate)
	return &BloomFilter{
		filter: bloomFilter,
		count:  0,
	}
}

// Add records key in the filter.
func (b *BloomFilter) Add(key []byte) {
	b.filter.Add(key)
	b.count++
}

// MayContain reports whether key might be present. False means definitely
// absent; true means possibly present.
func (b *BloomFilter) MayContain(key []byte) bool {
	return b.filter.Test(key)
}

// Serialize encodes the filter to bytes for storage in an SSTable.
func (b *BloomFilter) Serialize() ([]byte, error) {
	buf := new(bytes.Buffer)
	_, err := b.filter.WriteTo(buf)
	if err != nil {
		return nil, fmt.Errorf("error serializing bloom filter: %v", err)
	}
	return buf.Bytes(), nil
}

// DeserializeBloom reconstructs a bloom filter from the bytes produced by
// Serialize.
func DeserializeBloom(data []byte) (*BloomFilter, error) {
	filter := bloom.BloomFilter{}
	from, err := filter.ReadFrom(bytes.NewReader(data))
	if err != nil || from != int64(len(data)) {
		return nil, fmt.Errorf("error deserializing bloom filter: %v", err)
	}
	return &BloomFilter{
		filter: &filter,
		count:  0,
	}, nil
}
