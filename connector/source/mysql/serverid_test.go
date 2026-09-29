package mysql

import "testing"

func TestPickServerIDSkipsUsedPair(t *testing.T) {
	used := map[uint32]struct{}{5400: {}, 5402: {}}
	id, err := pickServerID(used)
	if err != nil {
		t.Fatal(err)
	}
	if id != 5403 {
		t.Fatalf("id %d", id)
	}
	if _, ok := used[id]; ok {
		t.Fatal("picked a used id")
	}
	if _, ok := used[id+1]; ok {
		t.Fatal("scan connection id is used")
	}
}
