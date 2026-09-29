// Package partition keeps events for the same sink key in order.
// The runner is single-threaded today; Gate is the lock a parallel writer would take.
package partition

import "sync"

// Gate runs fn while holding one mutex per key, acquired in sorted order.
type Gate struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// Do runs fn without overlapping another Do on any of the same keys.
func (g *Gate) Do(keys []string, fn func()) {
	ids := uniqueSorted(keys)
	g.mu.Lock()
	if g.locks == nil {
		g.locks = map[string]*sync.Mutex{}
	}
	held := make([]*sync.Mutex, len(ids))
	for i, id := range ids {
		lock := g.locks[id]
		if lock == nil {
			lock = &sync.Mutex{}
			g.locks[id] = lock
		}
		held[i] = lock
	}
	g.mu.Unlock()
	for _, lock := range held {
		lock.Lock()
	}
	defer func() {
		for i := len(held) - 1; i >= 0; i-- {
			held[i].Unlock()
		}
	}()
	fn()
}

func uniqueSorted(keys []string) []string {
	if len(keys) == 0 {
		return []string{""}
	}
	seen := map[string]struct{}{}
	var out []string
	for _, key := range keys {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
