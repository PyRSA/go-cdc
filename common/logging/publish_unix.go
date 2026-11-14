//go:build !windows

package logging

import (
	"fmt"
	"os"
)

// publishNoReplace makes final refer to the complete temp file without replacing
// an existing final path.
//
// On Unix this uses os.Link only: the call is atomic, fails if final already
// exists, and never stages an empty claim at the final path. If hard links are
// unavailable (for example EXDEV), the error is returned to the caller so the
// temporary file can be cleaned up without leaving a partial archive.
func publishNoReplace(temp, final string) error {
	if err := os.Link(temp, final); err != nil {
		if isTargetConflict(err) {
			return err
		}
		return fmt.Errorf("logging: hard-link publish unavailable: %w", err)
	}
	return nil
}
