package partition

import (
	"sync"
	"testing"
	"time"
)

func TestSameKeyDoesNotOverlap(t *testing.T) {
	var gate Gate
	var mu sync.Mutex
	inUse := 0
	maxInUse := 0
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gate.Do([]string{"users:1"}, func() {
				mu.Lock()
				inUse++
				if inUse > maxInUse {
					maxInUse = inUse
				}
				mu.Unlock()
				time.Sleep(time.Millisecond)
				mu.Lock()
				inUse--
				mu.Unlock()
			})
		}()
	}
	wg.Wait()
	if maxInUse != 1 {
		t.Fatalf("overlap %d", maxInUse)
	}
}
