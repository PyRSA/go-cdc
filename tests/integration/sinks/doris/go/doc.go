// Package doris_it contains real-Doris integration tests.
//
// The tests are excluded from normal `go test ./...` runs. Enable them with
// the `doris_integration` build tag and a reachable Doris 2.1.x FE/BE pair:
//
//	DORIS_HOST=127.0.0.1 go test -tags doris_integration ./tests/integration/sinks/doris/go/ -count=1 -v
package doris_it
