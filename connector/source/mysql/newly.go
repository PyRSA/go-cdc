package mysql

import "github.com/PyRSA/go-cdc/runtime/checkpoint"

// NewlyAdded returns matching table ids that are not yet captured, excluded, or mid-snapshot.
func NewlyAdded(matching []string, state checkpoint.State) []string {
	var out []string
	for _, id := range matching {
		if state.HasCaptured(id) || state.HasExcluded(id) || state.HasSnapshot(id) {
			continue
		}
		out = append(out, id)
	}
	return out
}
