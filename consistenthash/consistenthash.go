/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package consistenthash provides an implementation of a ring hash.
package consistenthash

import (
	"math"
	"slices"
	"sort"

	"github.com/segmentio/fasthash/fnv1"
)

type Hash func(data []byte) uint64

type Map struct {
	hash Hash
	// Number of keyring slices.
	partitions int

	hosts                 []string // Sorted
	hostsForKeyspaceSlice map[int]Hosts

	// Returns the number of hosts that should be considered as "master" nodes for each keyring slice when assigning hosts to keyring slices.
	replicaFn func(hostCount int) int
}

func New(partitions int, replicaFn func(hostCount int) int, fn Hash) *Map {
	m := &Map{
		partitions:            partitions,
		replicaFn:             replicaFn,
		hash:                  fn,
		hostsForKeyspaceSlice: make(map[int]Hosts),
	}
	if m.hash == nil {
		m.hash = fnv1.HashBytes64
	}

	return m
}

type Hosts []string

// Returns true if there are no hosts available.
func (m *Map) IsEmpty() bool {
	return len(m.hosts) == 0
}

// Adds some keys to the hash.
func (m *Map) Add(hosts ...string) {
	for _, k := range hosts {
		if slices.Contains(m.hosts, k) {
			continue
		}
		m.hosts = append(m.hosts, k)
	}
	sort.Strings(m.hosts)

	hostIdx := -1
	nextHost := func() string {
		hostIdx++
		if hostIdx > len(m.hosts)-1 {
			hostIdx = 0
		}
		return m.hosts[hostIdx]
	}

	// Reset the map
	m.hostsForKeyspaceSlice = make(map[int]Hosts)
	for i := 0; i < m.partitions; i++ {
		for j := 0; j < m.replicaFn(len(m.hosts)); j++ {
			nextHost := nextHost()
			if !slices.Contains(m.hostsForKeyspaceSlice[i], nextHost) {
				m.hostsForKeyspaceSlice[i] = append(m.hostsForKeyspaceSlice[i], nextHost)
			}
		}
	}

}

// Generate a slice of integers evenly distributed across the entire int64 space
func (m *Map) spread(n int) []uint64 {
	switch {
	case n <= 0:
		return nil
	case n == 1:
		return []uint64{0}
	}

	step := uint64(math.MaxUint64) / uint64(n-1)

	out := make([]uint64, n)
	for i := range out {
		out[i] = uint64(i) * step
	}
	// Integer division can miss the last element; pin the end.
	out[n-1] = math.MaxUint64
	return out
}

// Gets the set of hosts owning the keyring space closest to the provided key.
func (m *Map) Get(key string) Hosts {
	if m.IsEmpty() {
		return nil
	}

	hash := int(m.hash([]byte(key)))

	// Match the hash to its keyring slice
	if hash < 0 {
		hash = hash * -1
	}
	keyspaceSliceId := hash % m.partitions

	return m.hostsForKeyspaceSlice[keyspaceSliceId]
}

type KeyOwner struct {
	Key   int
	Peers []string
}

func (m *Map) KeyOwners() []KeyOwner {
	owners := make([]KeyOwner, m.partitions)
	for i := 0; i < m.partitions; i++ {
		owners[i] = KeyOwner{
			Key:   i,
			Peers: m.hostsForKeyspaceSlice[i],
		}
	}
	return owners
}
