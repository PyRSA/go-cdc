package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	return nil
}
