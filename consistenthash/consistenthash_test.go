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

package consistenthash

import (
	"fmt"
	"math/rand"
	"net"
	"slices"
	"strconv"
	"testing"

	"github.com/segmentio/fasthash/fnv1"
)

func TestHashing(t *testing.T) {

	// Override the hash function to return easier to reason about values. Assumes
	// the keys can be converted to an integer.
	hash := New(5, func(hostCount int) int { return 1 }, func(data []byte) uint64 {
		i, _ := strconv.Atoi(string(data))
		return uint64(i)
	})

	hash.Add("6", "4", "2")

	testCases := map[string]Hosts{
		"0": {"2"},
		"1": {"4"},
		"2": {"6"},
		"3": {"2"},
		"4": {"4"},
		"5": {"2"}, // Wrap around to partition 0 => uses hosts for partition 0
	}

	for k, vs := range testCases {
		if got := hash.Get(k); !slices.Equal(vs, got) {
			t.Errorf("Asking for %s, should have yielded one of %s; got %s instead", k, vs, got)
		}
	}

	hash.Add("8")

	testCases = map[string]Hosts{
		"0": {"2"},
		"1": {"4"},
		"2": {"6"},
		"3": {"8"},
		"4": {"2"},
		"5": {"2"}, // Wrap around to partition 0 => uses hosts for partition 0
	}

	for k, vs := range testCases {
		if got := hash.Get(k); !slices.Equal(vs, got) {
			t.Errorf("Asking for %s, should have yielded one of %s; got %s instead", k, vs, got)
		}
	}
}

func TestConsistency(t *testing.T) {
	hash1 := New(512, func(hostCount int) int { return 10 }, nil)
	hash2 := New(512, func(hostCount int) int { return 10 }, nil)

	hash1.Add("Bill", "Bob", "Bonny")
	hash2.Add("Bob", "Bonny", "Bill")

	if !slices.Equal(hash1.Get("Ben"), hash2.Get("Ben")) {
		t.Errorf("Fetching 'Ben' from both hashes should be the same")
	}

	hash2.Add("Becky", "Ben", "Bobby")
	hash1.Add("Becky", "Ben", "Bobby")

	if !slices.Equal(hash1.Get("Ben"), hash2.Get("Ben")) ||
		!slices.Equal(hash1.Get("Bob"), hash2.Get("Bob")) ||
		!slices.Equal(hash1.Get("Bonny"), hash2.Get("Bonny")) {
		t.Errorf("Direct matches should always return the same entry")
	}
}

func TestDistribution(t *testing.T) {
	hosts := []string{"a.svc.local", "b.svc.local", "c.svc.local"}
	const (
		keyCount = 10000
		replicas = 2
	)

	keys := make([]string, keyCount)

	for i := 0; i < keyCount; i++ {
		r := rand.Int31()
		ip := net.IPv4(192, byte(r>>16), byte(r>>8), byte(r))
		keys[i] = ip.String()
	}

	hashFuncs := map[string]Hash{
		"fasthash/fnv1": fnv1.HashBytes64,
	}

	for name, hashFunc := range hashFuncs {
		t.Run(name, func(t *testing.T) {
			hash := New(512, func(hostCount int) int { return replicas }, hashFunc)
			hostMap := map[string]int{}

			for _, host := range hosts {
				hash.Add(host)
				hostMap[host] = 0
			}

			for i := range keys {
				hostsForKeyspaceSlice := hash.Get(keys[i])
				for _, h := range hostsForKeyspaceSlice {
					hostMap[h]++
				}
			}

			for host, a := range hostMap {
				t.Logf("host: %s, total: %d, percent: %f", host, a, float64(a)/keyCount)
			}
		})
	}
}

func BenchmarkGet8(b *testing.B)   { benchmarkGet(b, 8) }
func BenchmarkGet32(b *testing.B)  { benchmarkGet(b, 32) }
func BenchmarkGet128(b *testing.B) { benchmarkGet(b, 128) }
func BenchmarkGet512(b *testing.B) { benchmarkGet(b, 512) }

func benchmarkGet(b *testing.B, shards int) {

	hash := New(50, func(hostCount int) int { return 1 }, nil)

	var buckets []string
	for i := 0; i < shards; i++ {
		buckets = append(buckets, fmt.Sprintf("shard-%d", i))
	}

	hash.Add(buckets...)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hash.Get(buckets[i&(shards-1)])
	}
}
