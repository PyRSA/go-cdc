package logging

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const archivedTimeLayout = "2006-01-02T15-04-05.000"

const maxArchiveRenameAttempts = 10000

// RollingFile writes bytes to a single active log file and rotates it by size
// and/or time. It does not format records or filter levels.
//
// Write strategy for one call:
//  1. If the active file must not receive this record (time due, or existing
//     bytes plus this record would exceed the size limit), archive first.
//  2. Always write the record, even when it alone exceeds the size limit.
//  3. If the active file is then at or over the size limit, archive it so the
//     next Write starts fresh.
//
// Archives are built in temporary files (names that do not match the archive
// prefix) and only published to the final name with a no-replace claim, so
// incomplete files are never visible as archives. Existing final names are
// never overwritten; a new sequence is chosen instead.
type RollingFile struct {
	mu sync.Mutex

	path         string
	sizeLimit    int64
	every        time.Duration
	dailyAt      *clock
	retainAge    time.Duration
	retainCount  int
	compressGzip bool

	file         *os.File
	size         int64
	nextRotateAt time.Time
	closed       bool
	archiveSeq   uint64

	// Hooks replaced in tests. nil means the real os/time behaviour.
	now             func() time.Time
	closeFile       func(*os.File) error
	gzipFile        func(string) error // full override; prefer finer hooks below
	renameFile      func(src, dst string) error
	openActive      func() error
	removeFile      func(string) error
	copyFn          func(dst io.Writer, src io.Reader) (int64, error)
	syncFn          func(*os.File) error
	publishFn       func(temp, final string) error
	gzipWriterClose func(*gzip.Writer) error
}

type clock struct {
	hour   int
	minute int
}

// errArchivePublished means the final archive path was published successfully
// but a follow-up cleanup (usually removing the old active file) failed.
type errArchivePublished struct {
	path string
	err  error
}

func (e *errArchivePublished) Error() string {
	return fmt.Sprintf("published %s but cleanup failed: %v", e.path, e.err)
}

func (e *errArchivePublished) Unwrap() error { return e.err }

// RollingOptions configures a RollingFile.
type RollingOptions struct {
	Path         string
	SizeLimit    int64
	Every        time.Duration
	DailyHour    int // used when Daily is true
	DailyMinute  int
	Daily        bool
	RetainAge    time.Duration
	RetainCount  int
	CompressGzip bool
}

// OpenRollingFile creates the parent directory when needed and opens the active file.
func OpenRollingFile(opts RollingOptions) (*RollingFile, error) {
	if strings.TrimSpace(opts.Path) == "" {
		return nil, fmt.Errorf("logging: file path is required")
	}
	if opts.SizeLimit <= 0 && opts.Every <= 0 && !opts.Daily {
		return nil, fmt.Errorf("logging: rotation size or interval is required")
	}
	if err := os.MkdirAll(filepath.Dir(opts.Path), 0o755); err != nil {
		return nil, fmt.Errorf("logging: mkdir %s: %w", filepath.Dir(opts.Path), err)
	}
	r := &RollingFile{
		path:         opts.Path,
		sizeLimit:    opts.SizeLimit,
		every:        opts.Every,
		retainAge:    opts.RetainAge,
		retainCount:  opts.RetainCount,
		compressGzip: opts.CompressGzip,
		now:          time.Now,
		closeFile:    closeFileDefault,
		removeFile:   os.Remove,
	}
	r.openActive = r.openActiveDefault
	if opts.Daily {
		r.dailyAt = &clock{hour: opts.DailyHour, minute: opts.DailyMinute}
		if r.every <= 0 {
			r.every = 24 * time.Hour
		}
	}
	if err := r.openFile(); err != nil {
		return nil, err
	}
	now := r.now()
	r.nextRotateAt = r.computeNextRotate(now)
	if err := r.pruneLocked(now); err != nil {
		_ = r.closeActive()
		return nil, err
	}
	return r, nil
}

func closeFileDefault(f *os.File) error {
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func (r *RollingFile) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	now := r.now()
	if err := r.ensureOpenLocked(now); err != nil {
		return 0, err
	}
	if r.shouldRotateBeforeWrite(now, int64(len(p))) {
		if err := r.rotateLocked(now); err != nil {
			return 0, err
		}
	}
	n, err := r.file.Write(p)
	r.size += int64(n)
	if err != nil {
		return n, err
	}
	// A record that alone fills or exceeds the limit is kept in this file; the
	// file is then archived so the next Write does not append to it.
	if r.sizeLimit > 0 && r.size >= r.sizeLimit {
		if rotErr := r.rotateLocked(now); rotErr != nil {
			return n, rotErr
		}
	}
	return n, nil
}

// Sync flushes the active file to stable storage.
// After Close it is a no-op. When the writer is open but has no active handle
// (for example after a failed reopen), Sync reports an error instead of success.
func (r *RollingFile) Sync() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	if r.file == nil {
		return fmt.Errorf("logging: no active file to sync")
	}
	return r.file.Sync()
}

// Close flushes and closes the active file.
func (r *RollingFile) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	return r.closeActive()
}

func (r *RollingFile) ensureOpenLocked(now time.Time) error {
	if r.file != nil {
		return nil
	}
	if err := r.openFile(); err != nil {
		return err
	}
	r.nextRotateAt = r.computeNextRotate(now)
	return nil
}

func (r *RollingFile) shouldRotateBeforeWrite(now time.Time, incoming int64) bool {
	if r.file == nil {
		return false
	}
	// Existing bytes plus this record would exceed the limit: archive first so
	// the oversized record starts a new file (and may fill it alone).
	if r.sizeLimit > 0 && r.size > 0 && r.size+incoming > r.sizeLimit {
		return true
	}
	if !r.nextRotateAt.IsZero() && !now.Before(r.nextRotateAt) {
		return true
	}
	return false
}

// closeActive releases the active handle. The pointer is cleared even when
// Close returns an error so callers never reuse a closed *os.File.
func (r *RollingFile) closeActive() error {
	if r.file == nil {
		return nil
	}
	f := r.file
	r.file = nil
	r.size = 0
	closeFn := r.closeFile
	if closeFn == nil {
		closeFn = closeFileDefault
	}
	return closeFn(f)
}

func (r *RollingFile) rotateLocked(now time.Time) error {
	// Time-based rotate of an empty active file would create a useless archive
	// and consume retention. Advance the schedule, keep the open handle, and
	// still prune so retainAge/count run on the due tick.
	if r.file != nil && r.size == 0 {
		timeDue := !r.nextRotateAt.IsZero() && !now.Before(r.nextRotateAt)
		if timeDue {
			r.nextRotateAt = r.computeNextRotate(now)
			return r.pruneLocked(now)
		}
	}
	var archived string
	if r.file != nil {
		name := r.file.Name()
		if err := r.closeActive(); err != nil {
			openErr := r.openFile()
			if openErr != nil {
				return fmt.Errorf("logging: close active file: %v (reopen: %w)", err, openErr)
			}
			return fmt.Errorf("logging: close active file: %w", err)
		}
		var err error
		archived, err = r.renameActiveToArchive(name, now)
		if err != nil {
			if openErr := r.openFile(); openErr != nil {
				return fmt.Errorf("logging: archive %s: %v (reopen: %v)", name, err, openErr)
			}
			return fmt.Errorf("logging: archive %s: %w", name, err)
		}
	}
	if err := r.openFile(); err != nil {
		return err
	}
	r.nextRotateAt = r.computeNextRotate(now)
	var compressErr error
	if archived != "" && r.compressGzip {
		if err := r.compressArchived(archived); err != nil {
			compressErr = fmt.Errorf("logging: compress %s: %w", archived, err)
		}
	}
	// Prune even when compression fails so retention still sees leftover plains.
	if err := r.pruneLocked(now); err != nil {
		if compressErr != nil {
			return fmt.Errorf("%v (prune: %v)", compressErr, err)
		}
		return err
	}
	return compressErr
}

func (r *RollingFile) renameActiveToArchive(activeName string, now time.Time) (string, error) {
	var lastErr error
	for i := 0; i < maxArchiveRenameAttempts; i++ {
		candidate := r.nextArchiveName(now)
		err := r.doArchive(activeName, candidate)
		if err == nil {
			return candidate, nil
		}
		var published *errArchivePublished
		if errors.As(err, &published) {
			// Final archive is durable. Clear the old active path so the next
			// open starts empty and this content is not archived again.
			if clearErr := clearActivePath(activeName); clearErr != nil {
				return "", fmt.Errorf("logging: published %s but clear active %s: %v (remove: %v)",
					published.path, activeName, clearErr, published.err)
			}
			return published.path, nil
		}
		lastErr = err
		if isTargetConflict(err) {
			continue
		}
		return "", err
	}
	return "", fmt.Errorf("logging: exhausted unique archive names: %w", lastErr)
}

func (r *RollingFile) doArchive(src, dst string) error {
	if r.renameFile != nil {
		return r.renameFile(src, dst)
	}
	return r.archiveNoReplace(src, dst)
}

func clearActivePath(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	return f.Close()
}

func (r *RollingFile) remove(path string) error {
	removeFn := r.removeFile
	if removeFn == nil {
		removeFn = os.Remove
	}
	return removeFn(path)
}

func (r *RollingFile) copy(dst io.Writer, src io.Reader) (int64, error) {
	if r.copyFn != nil {
		return r.copyFn(dst, src)
	}
	return io.Copy(dst, src)
}

func (r *RollingFile) syncFile(f *os.File) error {
	if r.syncFn != nil {
		return r.syncFn(f)
	}
	return f.Sync()
}

// archiveNoReplace copies src into a temp file, publishes it to dst without
// replacing an existing file, then removes src. Temp names do not match the
// archive prefix used by listArchived.
func (r *RollingFile) archiveNoReplace(src, dst string) error {
	dir := filepath.Dir(dst)
	tmp, err := os.CreateTemp(dir, ".archive-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	cleanupTemp := func(primary error) error {
		if rmErr := r.remove(tmpName); rmErr != nil && !os.IsNotExist(rmErr) {
			if primary != nil {
				return fmt.Errorf("%w (cleanup temp %s: %v)", primary, tmpName, rmErr)
			}
			return fmt.Errorf("logging: cleanup temp %s: %w", tmpName, rmErr)
		}
		return primary
	}

	in, err := os.Open(src)
	if err != nil {
		_ = tmp.Close()
		return cleanupTemp(err)
	}
	_, copyErr := r.copy(tmp, in)
	closeInErr := in.Close()
	modeErr := copyPermFrom(src, tmp)
	syncErr := r.syncFile(tmp)
	closeTmpErr := tmp.Close()
	if copyErr != nil || closeInErr != nil || modeErr != nil || syncErr != nil || closeTmpErr != nil {
		primary := firstError(copyErr, closeInErr, modeErr, syncErr, closeTmpErr)
		return cleanupTemp(primary)
	}

	if err := r.publish(tmpName, dst); err != nil {
		// On failure the final name was not claimed; remove the temp.
		return cleanupTemp(err)
	}
	// Final name is durable. Temp may remain after hard-link publish.
	var cleanupErr error
	if rmErr := r.remove(tmpName); rmErr != nil && !os.IsNotExist(rmErr) {
		cleanupErr = fmt.Errorf("temp cleanup: %w", rmErr)
	}
	if err := r.remove(src); err != nil {
		if cleanupErr != nil {
			return &errArchivePublished{path: dst, err: fmt.Errorf("%v; remove src: %w", cleanupErr, err)}
		}
		return &errArchivePublished{path: dst, err: err}
	}
	if cleanupErr != nil {
		return &errArchivePublished{path: dst, err: cleanupErr}
	}
	return nil
}

func (r *RollingFile) publish(temp, final string) error {
	if r.publishFn != nil {
		return r.publishFn(temp, final)
	}
	return publishNoReplace(temp, final)
}

func firstError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// copyPermFrom sets dst's permission bits to match srcPath so published
// archives stay readable by the same collectors that can read the active log.
func copyPermFrom(srcPath string, dst *os.File) error {
	info, err := os.Stat(srcPath)
	if err != nil {
		return err
	}
	return dst.Chmod(info.Mode().Perm())
}

func isTargetConflict(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrExist) || os.IsExist(err) {
		return true
	}
	// Windows rename-to-existing is often reported without os.ErrExist.
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already exists") ||
		strings.Contains(msg, "file exists") ||
		strings.Contains(msg, "cannot create a file when that file already exists")
}

func (r *RollingFile) openFile() error {
	open := r.openActive
	if open == nil {
		open = r.openActiveDefault
	}
	return open()
}

func (r *RollingFile) openActiveDefault() error {
	file, err := os.OpenFile(r.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("logging: open %s: %w", r.path, err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("logging: stat %s: %w", r.path, err)
	}
	r.file = file
	r.size = info.Size()
	return nil
}

func (r *RollingFile) nextArchiveName(now time.Time) string {
	r.archiveSeq++
	dir := filepath.Dir(r.path)
	name, ext := archiveBaseParts(r.path)
	return filepath.Join(dir, fmt.Sprintf("%s-%s-%d%s", name, now.Format(archivedTimeLayout), r.archiveSeq, ext))
}

// archiveBaseParts returns the archive name stem and extension used by both
// generation and recognition. Paths without an extension use ".log", matching
// historical RollingFile naming (never emit extension-less archives).
func archiveBaseParts(path string) (name, ext string) {
	base := filepath.Base(path)
	ext = filepath.Ext(base)
	name = strings.TrimSuffix(base, ext)
	if ext == "" {
		ext = ".log"
	}
	return name, ext
}

func (r *RollingFile) computeNextRotate(now time.Time) time.Time {
	if r.dailyAt != nil {
		next := time.Date(now.Year(), now.Month(), now.Day(), r.dailyAt.hour, r.dailyAt.minute, 0, 0, now.Location())
		if !next.After(now) {
			// Calendar day forward avoids DST 23h/25h skew from Add(24h).
			next = next.AddDate(0, 0, 1)
		}
		return next
	}
	if r.every > 0 {
		return now.Add(r.every)
	}
	return time.Time{}
}

// archivedFile is one logical archive. Plain and .gz forms of the same name
// share a key so retention counts them once.
type archivedFile struct {
	key     string
	paths   []string
	modTime time.Time
}

func (r *RollingFile) listArchived() ([]archivedFile, error) {
	dir := filepath.Dir(r.path)
	base := filepath.Base(r.path)
	name, ext := archiveBaseParts(r.path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	grouped := map[string]*archivedFile{}
	var keys []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		fname := entry.Name()
		if fname == base {
			continue
		}
		plainName, ok := archivePlainName(fname, name, ext)
		if !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		full := filepath.Join(dir, fname)
		key := filepath.Join(dir, plainName)
		if existing, ok := grouped[key]; ok {
			existing.paths = append(existing.paths, full)
			if info.ModTime().After(existing.modTime) {
				existing.modTime = info.ModTime()
			}
			continue
		}
		grouped[key] = &archivedFile{
			key:     key,
			paths:   []string{full},
			modTime: info.ModTime(),
		}
		keys = append(keys, key)
	}
	out := make([]archivedFile, 0, len(keys))
	for _, key := range keys {
		out = append(out, *grouped[key])
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].modTime.After(out[j].modTime)
	})
	return out, nil
}

// archivePlainName reports whether fname is an archive for name+ext and returns
// the plain (uncompressed) archive filename.
//
// Forms: {name}-{timestamp}[-{seq}]{ext} and the same plus a trailing
// compression ".gz". When ext is already ".gz", plain archives end with ".gz"
// and compressed archives end with ".gz.gz"; a bare TrimSuffix(..., ".gz")
// would wrongly treat the original extension as compression.
func archivePlainName(fname, name, ext string) (string, bool) {
	if strings.HasSuffix(fname, ext+".gz") {
		plain := strings.TrimSuffix(fname, ".gz")
		if matchArchivePlain(plain, name, ext) {
			return plain, true
		}
		return "", false
	}
	if matchArchivePlain(fname, name, ext) {
		return fname, true
	}
	return "", false
}

func isArchivedFileName(fname, name, ext string) bool {
	_, ok := archivePlainName(fname, name, ext)
	return ok
}

func matchArchivePlain(plain, name, ext string) bool {
	if !strings.HasSuffix(plain, ext) {
		return false
	}
	return matchArchiveStem(strings.TrimSuffix(plain, ext), name)
}

func matchArchiveStem(stem, name string) bool {
	prefix := name + "-"
	if !strings.HasPrefix(stem, prefix) {
		return false
	}
	rest := stem[len(prefix):]
	const tsLen = len("2006-01-02T15-04-05.000")
	if len(rest) < tsLen {
		return false
	}
	if _, err := time.Parse(archivedTimeLayout, rest[:tsLen]); err != nil {
		return false
	}
	suffix := rest[tsLen:]
	if suffix == "" {
		return true // historical name without sequence
	}
	if !strings.HasPrefix(suffix, "-") {
		return false
	}
	seq := suffix[1:]
	if seq == "" {
		return false
	}
	for _, r := range seq {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func (r *RollingFile) pruneLocked(now time.Time) error {
	if r.retainAge <= 0 && r.retainCount <= 0 {
		return nil
	}
	files, err := r.listArchived()
	if err != nil {
		return err
	}
	keep := make([]archivedFile, 0, len(files))
	for _, file := range files {
		if r.retainAge > 0 && now.Sub(file.modTime) > r.retainAge {
			if err := removeArchiveFiles(r.remove, file.paths); err != nil {
				return err
			}
			continue
		}
		keep = append(keep, file)
	}
	if r.retainCount > 0 && len(keep) > r.retainCount {
		for _, file := range keep[r.retainCount:] {
			if err := removeArchiveFiles(r.remove, file.paths); err != nil {
				return err
			}
		}
	}
	return nil
}

func removeArchiveFiles(removeFn func(string) error, paths []string) error {
	for _, path := range paths {
		if err := removeFn(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func (r *RollingFile) compressArchived(path string) error {
	if r.gzipFile != nil {
		return r.gzipFile(path)
	}
	return r.gzipToArchive(path)
}

// gzipToArchive compresses path into a temp file and publishes path+".gz"
// without replacing an existing compressed archive.
func (r *RollingFile) gzipToArchive(path string) error {
	in, err := os.Open(path)
	if err != nil {
		return err
	}

	outPath := path + ".gz"
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".gz-*.tmp")
	if err != nil {
		_ = in.Close()
		return err
	}
	tmpName := tmp.Name()

	cleanupTemp := func(primary error) error {
		if rmErr := r.remove(tmpName); rmErr != nil && !os.IsNotExist(rmErr) {
			if primary != nil {
				return fmt.Errorf("%w (cleanup temp %s: %v)", primary, tmpName, rmErr)
			}
			return fmt.Errorf("logging: cleanup temp %s: %w", tmpName, rmErr)
		}
		return primary
	}

	gz := gzip.NewWriter(tmp)
	_, copyErr := r.copy(gz, in)
	closeInErr := in.Close()
	var closeGzErr error
	if r.gzipWriterClose != nil {
		closeGzErr = r.gzipWriterClose(gz)
	} else {
		closeGzErr = gz.Close()
	}
	modeErr := copyPermFrom(path, tmp)
	syncErr := r.syncFile(tmp)
	closeTmpErr := tmp.Close()
	if copyErr != nil || closeInErr != nil || closeGzErr != nil || modeErr != nil || syncErr != nil || closeTmpErr != nil {
		primary := firstError(copyErr, closeInErr, closeGzErr, modeErr, syncErr, closeTmpErr)
		return cleanupTemp(primary)
	}

	if err := r.publish(tmpName, outPath); err != nil {
		return cleanupTemp(err)
	}
	var cleanupErr error
	if rmErr := r.remove(tmpName); rmErr != nil && !os.IsNotExist(rmErr) {
		cleanupErr = fmt.Errorf("temp cleanup: %w", rmErr)
	}
	if err := r.remove(path); err != nil {
		// .gz published; leave plain so one complete copy remains.
		if cleanupErr != nil {
			return fmt.Errorf("%v; remove plain: %w", cleanupErr, err)
		}
		return err
	}
	if cleanupErr != nil {
		return fmt.Errorf("logging: published %s but %v", outPath, cleanupErr)
	}
	return nil
}
