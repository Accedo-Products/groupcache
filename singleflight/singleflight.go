/*
Copyright 2012 Google Inc.

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

// Package singleflight provides a duplicate function call suppression
// mechanism.
package singleflight

import (
	"github.com/pkg/errors"
	"sync"
	"time"
)

// call is an in-flight or completed Do call
type call struct {
	wg      sync.WaitGroup
	created time.Time
	val     interface{}
	err     error
}

// Group represents a class of work and forms a namespace in which
// units of work can be executed with duplicate suppression.
type Group struct {
	mu sync.Mutex       // protects m
	m  map[string]*call // lazily initialized
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := &call{
		created: time.Now().UTC(),
		err:     errors.Errorf("singleflight leader panicked"),
	}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	defer func() {
		c.wg.Done()
		g.mu.Lock()
		delete(g.m, key)
		g.mu.Unlock()
	}()

	c.val, c.err = fn()
	return c.val, c.err
}

// Count returns the number of currently active single flight entries.
func (g *Group) Count() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return int64(len(g.m))
}

// LongestRunningStartTime returns the timestamp at which the oldest single flight entry in the group was created. May be 0 if there are no running entries.
func (g *Group) LongestRunningStartTime() time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	var oldest time.Time
	for _, e := range g.m {
		if oldest.IsZero() || e.created.Before(oldest) {
			oldest = e.created
		}
	}
	return oldest
}

// Lock prevents single flights from occurring for the duration
// of the provided function. This allows users to clear caches
// or perform some operation in between running flights.
func (g *Group) Lock(fn func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	fn()
}
