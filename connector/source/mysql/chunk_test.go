package mysql

import "testing"

func TestPlanSplitsCoversEveryKeyOnce(t *testing.T) {
	keys := []string{"1", "2", "3", "4", "5"}
	splits, err := PlanSplits(slicePage(keys), 2)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, key := range keys {
		for _, split := range splits {
			if inSplit(key, split.Low, split.High) {
				seen[key]++
			}
		}
	}
	for _, key := range keys {
		if seen[key] != 1 {
			t.Fatalf("key %s seen %d times, splits %#v", key, seen[key], splits)
		}
	}
	if splits[len(splits)-1].High != nil {
		t.Fatal("last split must be open-ended")
	}
}

func TestPlanSplitsEmpty(t *testing.T) {
	splits, err := PlanSplits(slicePage(nil), 3)
	if err != nil || len(splits) != 0 {
		t.Fatalf("%v %#v", err, splits)
	}
}

func TestPlanSplitsStuckOnDuplicateKey(t *testing.T) {
	_, err := PlanSplits(slicePage([]string{"1", "1", "1"}), 2)
	if err == nil {
		t.Fatal("expected stuck chunk key")
	}
}

func slicePage(keys []string) PageFunc {
	return func(low *string, limit int) ([]string, error) {
		start := 0
		if low != nil {
			for start < len(keys) && keys[start] < *low {
				start++
			}
		}
		end := start + limit
		if end > len(keys) {
			end = len(keys)
		}
		out := append([]string(nil), keys[start:end]...)
		return out, nil
	}
}

func inSplit(key, low string, high *string) bool {
	if key < low {
		return false
	}
	if high != nil && key >= *high {
		return false
	}
	return true
}
