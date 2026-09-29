package mysql

import (
	"fmt"

	"github.com/PyRSA/go-cdc/runtime/checkpoint"
)

// PageFunc returns up to limit chunk-key values at or after low, in order.
// A nil low starts at the beginning of the table.
type PageFunc func(low *string, limit int) ([]string, error)

// PlanSplits walks chunk keys the way Flink's JdbcChunkSplitter does.
// Each full page's last key is the next lower bound and is excluded from the current range.
// The final page has a null high. An empty table produces no splits.
func PlanSplits(page PageFunc, chunkSize int) ([]checkpoint.Split, error) {
	if chunkSize < 1 {
		return nil, fmt.Errorf("chunk size must be >= 1")
	}
	var low *string
	var splits []checkpoint.Split
	for {
		keys, err := page(low, chunkSize)
		if err != nil {
			return nil, err
		}
		if len(keys) == 0 {
			if low == nil {
				return nil, nil
			}
			splits = append(splits, checkpoint.Split{Low: *low, High: nil})
			return splits, nil
		}
		if len(keys) < chunkSize {
			start := keys[0]
			if low != nil {
				start = *low
			}
			splits = append(splits, checkpoint.Split{Low: start, High: nil})
			return splits, nil
		}
		high := keys[len(keys)-1]
		start := keys[0]
		if low != nil {
			start = *low
		}
		if high == start {
			return nil, fmt.Errorf("chunk key %q did not advance; choose another chunk key column", high)
		}
		highCopy := high
		splits = append(splits, checkpoint.Split{Low: start, High: &highCopy})
		next := high
		low = &next
	}
}
