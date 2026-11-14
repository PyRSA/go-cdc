//go:build !windows

package logging

import (
	"fmt"
	"os"
)

// publishNoReplace makes final refer to the complete temp file without replacing
// an existing final path.
//
// On Unix we prefer os.Link (atomic, fails if final exists). If Link is
// unavailable (for example EXDEV), we exclusively create an empty claim at
// final and Rename(temp→final) over that claim. Rename(2) replaces the claim
// we own; it never deletes a foreign file first, so there is no
// remove-then-rename race against another publisher's complete archive.
func publishNoReplace(temp, final string) error {
	if err := os.Link(temp, final); err == nil {
		return nil
	} else if isTargetConflict(err) {
		return err
	}

	f, err := os.OpenFile(final, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		if rmErr := os.Remove(final); rmErr != nil && !os.IsNotExist(rmErr) {
			return fmt.Errorf("%w (cleanup claim: %v)", err, rmErr)
		}
		return err
	}
	if err := os.Rename(temp, final); err != nil {
		if rmErr := os.Remove(final); rmErr != nil && !os.IsNotExist(rmErr) {
			return fmt.Errorf("%w (cleanup claim: %v)", err, rmErr)
		}
		return err
	}
	return nil
}
