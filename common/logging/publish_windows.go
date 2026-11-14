//go:build windows

package logging

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

var (
	modkernel32     = syscall.NewLazyDLL("kernel32.dll")
	procMoveFileExW = modkernel32.NewProc("MoveFileExW")
)

// publishNoReplace makes final refer to the complete temp file without replacing
// an existing final path.
//
// Prefer os.Link (same atomic no-replace semantics as Unix). When Link is
// unavailable, fall back to MoveFileExW with flags=0 — deliberately without
// MOVEFILE_REPLACE_EXISTING, which Go's os.Rename always sets on Windows.
// flags=0 fails atomically when final already exists and never stages an empty
// claim that must be removed before rename (the previous race window).
func publishNoReplace(temp, final string) error {
	if err := os.Link(temp, final); err == nil {
		return nil
	} else if isTargetConflict(err) {
		return err
	}
	return moveFileNoReplace(temp, final)
}

func moveFileNoReplace(temp, final string) error {
	from, err := syscall.UTF16PtrFromString(temp)
	if err != nil {
		return err
	}
	to, err := syscall.UTF16PtrFromString(final)
	if err != nil {
		return err
	}
	r1, _, e1 := procMoveFileExW.Call(
		uintptr(unsafe.Pointer(from)),
		uintptr(unsafe.Pointer(to)),
		0, // no MOVEFILE_REPLACE_EXISTING
	)
	if r1 != 0 {
		return nil
	}
	if e1 == syscall.Errno(0) {
		return syscall.EINVAL
	}
	errno, ok := e1.(syscall.Errno)
	if ok && (errno == syscall.ERROR_ALREADY_EXISTS || errno == syscall.ERROR_FILE_EXISTS) {
		return fmt.Errorf("%w: %v", os.ErrExist, e1)
	}
	return e1
}
