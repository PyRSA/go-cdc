package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
)

// Store is an atomic checkpoint file. A half-written temporary file is ignored.
type Store struct {
	Path string
}

// Load reads the last complete checkpoint. A missing file is an empty state.
func (s Store) Load() (State, error) {
	data, err := os.ReadFile(s.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			state := State{}
			state.Normalize()
			return state, nil
		}
		return State{}, fmt.Errorf("checkpoint: read %s: %w", s.Path, err)
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return State{}, fmt.Errorf("checkpoint: decode %s: %w", s.Path, err)
	}
	state.Normalize()
	return state, nil
}

// Save replaces the checkpoint by writing a temporary file, syncing it, and renaming.
func (s Store) Save(state State) error {
	state.Normalize()
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o755); err != nil {
		return fmt.Errorf("checkpoint: mkdir: %w", err)
	}
	body, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("checkpoint: encode: %w", err)
	}
	body = append(body, '\n')
	tmp := s.Path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("checkpoint: create temp: %w", err)
	}
	_, writeErr := file.Write(body)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("checkpoint: write temp: %v %v %v", writeErr, syncErr, closeErr)
	}
	if err := os.Rename(tmp, s.Path); err != nil {
		return fmt.Errorf("checkpoint: rename: %w", err)
	}
	// The temp file was synced. The directory entry that rename replaced is a
	// separate write; without this, a power loss can still show the previous
	// checkpoint and the job replays one more batch.
	if err := flushCheckpointDir(filepath.Dir(s.Path)); err != nil {
		return fmt.Errorf("checkpoint: sync dir: %w", err)
	}
	return nil
}

// flushCheckpointDir is syncDir in production. Tests replace it to prove Save
// waits for the directory flush after rename.
var flushCheckpointDir = syncDir

// syncDir flushes the directory so the renamed checkpoint is the entry a
// restart observes. Windows directory handles reject FlushFileBuffers; that
// platform has no equivalent, and the error is not a failed save.
func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = file.Sync()
	closeErr := file.Close()
	if err != nil {
		if dirSyncUnsupported(err) {
			return nil
		}
		return err
	}
	return closeErr
}

// dirSyncUnsupported reports whether errno says the platform cannot flush a
// directory. ENOTSUP and EOPNOTSUPP are the same constant on Linux, so they are
// compared rather than listed as switch cases: two case labels that are equal
// do not compile.
func dirSyncUnsupported(err error) bool {
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return false
	}
	if errno == syscall.ENOTSUP || errno == syscall.EOPNOTSUPP || errno == syscall.EINVAL {
		return true
	}
	// FlushFileBuffers on a Windows directory returns ERROR_INVALID_FUNCTION (1)
	// or ERROR_ACCESS_DENIED (5). Unix uses 1 for EPERM and 5 for EIO, so those
	// stay failures there.
	return runtime.GOOS == "windows" && (errno == 1 || errno == 5)
}
