package definition

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	defaultLogLevel  = "INFO"
	defaultLogFormat = "text"
	defaultLogStream = "stderr"
	defaultRotation  = "100 MB"
)

func parseLogging(raw map[string]any) (Logging, error) {
	out := Logging{
		Configured: true,
		Level:      defaultLogLevel,
		Format:     defaultLogFormat,
	}
	if raw == nil {
		return out, nil
	}
	for key, value := range raw {
		switch key {
		case "level":
			level, err := parseLogLevel(fmt.Sprint(value))
			if err != nil {
				return Logging{}, configf("logging.level", "%v", err)
			}
			out.Level = level
		case "format":
			format, err := parseLogFormat(fmt.Sprint(value))
			if err != nil {
				return Logging{}, configf("logging.format", "%v", err)
			}
			out.Format = format
		case "console":
			m := asMap(value)
			if value != nil && m == nil {
				return Logging{}, configErr("logging.console", "must be a mapping")
			}
			console, err := parseLoggingConsole(m)
			if err != nil {
				return Logging{}, err
			}
			out.Console = &console
		case "file":
			m := asMap(value)
			if m == nil {
				return Logging{}, configErr("logging.file", "must be a mapping")
			}
			file, err := parseLoggingFile(m)
			if err != nil {
				return Logging{}, err
			}
			out.File = &file
		default:
			return Logging{}, configErr("logging."+key, "unknown field")
		}
	}
	return out, nil
}

func parseLoggingConsole(raw map[string]any) (LoggingConsole, error) {
	out := LoggingConsole{Stream: defaultLogStream}
	if raw == nil {
		return out, nil
	}
	for key, value := range raw {
		switch key {
		case "stream":
			stream, err := parseLogStream(fmt.Sprint(value))
			if err != nil {
				return LoggingConsole{}, configf("logging.console.stream", "%v", err)
			}
			out.Stream = stream
		default:
			return LoggingConsole{}, configErr("logging.console."+key, "unknown field")
		}
	}
	return out, nil
}

func parseLoggingFile(raw map[string]any) (LoggingFile, error) {
	if raw == nil {
		return LoggingFile{}, configErr("logging.file", "must be a mapping")
	}
	out := LoggingFile{}
	var (
		hasPath, hasRotation, hasRetention, hasCompression bool
		rotationRaw, retentionRaw, compressionRaw          string
	)
	for key, value := range raw {
		switch key {
		case "path":
			path, pathErr := requireNonEmptyString("logging.file.path", value)
			if pathErr != nil {
				return LoggingFile{}, pathErr
			}
			out.Path = path
			hasPath = true
		case "rotation":
			rotationRaw = strings.TrimSpace(fmt.Sprint(value))
			hasRotation = true
		case "retention":
			retentionRaw = strings.TrimSpace(fmt.Sprint(value))
			hasRetention = true
		case "compression":
			compressionRaw = strings.TrimSpace(fmt.Sprint(value))
			hasCompression = true
		default:
			return LoggingFile{}, configErr("logging.file."+key, "unknown field")
		}
	}
	if !hasPath || out.Path == "" {
		return LoggingFile{}, configErr("logging.file.path", "required")
	}
	if !hasRotation || rotationRaw == "" {
		rotationRaw = defaultRotation
	}
	size, every, daily, err := parseRotation(rotationRaw)
	if err != nil {
		return LoggingFile{}, configf("logging.file.rotation", "%v", err)
	}
	out.SizeBytes = size
	out.Every = every
	out.DailyAt = daily
	if hasRetention {
		age, count, err := parseRetention(retentionRaw)
		if err != nil {
			return LoggingFile{}, configf("logging.file.retention", "%v", err)
		}
		out.RetainAge = age
		out.RetainCount = count
	}
	if hasCompression {
		gzip, err := parseCompression(compressionRaw)
		if err != nil {
			return LoggingFile{}, configf("logging.file.compression", "%v", err)
		}
		out.CompressGzip = gzip
	}
	return out, nil
}

func parseLogLevel(raw string) (string, error) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "DEBUG":
		return "DEBUG", nil
	case "INFO":
		return "INFO", nil
	case "WARN", "WARNING":
		return "WARN", nil
	case "ERROR":
		return "ERROR", nil
	default:
		return "", fmt.Errorf("must be DEBUG, INFO, WARN or ERROR")
	}
}

func parseLogFormat(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "text":
		return "text", nil
	case "json":
		return "json", nil
	default:
		return "", fmt.Errorf("must be text or json")
	}
}

func parseLogStream(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "stdout":
		return "stdout", nil
	case "stderr":
		return "stderr", nil
	default:
		return "", fmt.Errorf("must be stdout or stderr")
	}
}

// parseRotation accepts a size ("100 MB") or a time interval ("1 day", "24h",
// "00:00"). Size and time cannot be combined in one value.
func parseRotation(raw string) (sizeBytes int64, every time.Duration, daily *Clock, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, 0, nil, fmt.Errorf("required")
	}
	if clock, ok, clockErr := parseClock(raw); clockErr != nil {
		return 0, 0, nil, clockErr
	} else if ok {
		return 0, 24 * time.Hour, &clock, nil
	}
	if size, ok, sizeErr := parseSizeBytes(raw); sizeErr != nil {
		return 0, 0, nil, sizeErr
	} else if ok {
		if size <= 0 {
			return 0, 0, nil, fmt.Errorf("size must be positive")
		}
		return size, 0, nil, nil
	}
	every, err = parseHumanDuration(raw)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("must be a size (e.g. \"100 MB\"), a duration (e.g. \"1 day\"), or HH:MM: %v", err)
	}
	if every <= 0 {
		return 0, 0, nil, fmt.Errorf("duration must be positive")
	}
	return 0, every, nil, nil
}

func parseRetention(raw string) (age time.Duration, count int, err error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, 0, fmt.Errorf("required when set")
	}
	// A bare integer is a file count (Loguru-style retention: 10).
	if isAllDigits(raw) {
		n, convErr := strconv.Atoi(raw)
		if convErr != nil || n <= 0 {
			return 0, 0, fmt.Errorf("count must be a positive integer")
		}
		return 0, n, nil
	}
	lower := strings.ToLower(raw)
	if strings.HasSuffix(lower, " files") || strings.HasSuffix(lower, " file") {
		num := strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(lower, " files"), " file"))
		n, convErr := strconv.Atoi(num)
		if convErr != nil || n <= 0 {
			return 0, 0, fmt.Errorf("count must be a positive integer")
		}
		return 0, n, nil
	}
	age, err = parseHumanDuration(raw)
	if err != nil {
		return 0, 0, fmt.Errorf("must be a duration (e.g. \"30 days\") or a positive file count: %v", err)
	}
	if age <= 0 {
		return 0, 0, fmt.Errorf("duration must be positive")
	}
	return age, 0, nil
}

func parseCompression(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "none", "off", "false":
		return false, nil
	case "gz", "gzip":
		return true, nil
	default:
		return false, fmt.Errorf("must be gz (or gzip), or empty for none")
	}
}

func parseClock(raw string) (Clock, bool, error) {
	parts := strings.Split(raw, ":")
	if len(parts) != 2 {
		return Clock{}, false, nil
	}
	for _, p := range parts {
		if p == "" || !isAllDigits(p) {
			return Clock{}, false, nil
		}
	}
	hour, err := strconv.Atoi(parts[0])
	if err != nil || hour < 0 || hour > 23 {
		return Clock{}, false, fmt.Errorf("HH:MM hour must be 0..23")
	}
	minute, err := strconv.Atoi(parts[1])
	if err != nil || minute < 0 || minute > 59 {
		return Clock{}, false, fmt.Errorf("HH:MM minute must be 0..59")
	}
	return Clock{Hour: hour, Minute: minute}, true, nil
}

func parseSizeBytes(raw string) (int64, bool, error) {
	raw = strings.TrimSpace(raw)
	fields := strings.Fields(raw)
	var numStr, unit string
	switch len(fields) {
	case 1:
		split := splitLeadingNumber(fields[0])
		if split.num == "" || split.rest == "" {
			return 0, false, nil
		}
		numStr, unit = split.num, split.rest
	case 2:
		numStr, unit = fields[0], fields[1]
	default:
		return 0, false, nil
	}
	n, err := strconv.ParseFloat(numStr, 64)
	if err != nil || n <= 0 {
		return 0, false, fmt.Errorf("size amount must be a positive number")
	}
	mult, ok := sizeUnitBytes(unit)
	if !ok {
		return 0, false, nil
	}
	bytes := int64(n * float64(mult))
	if bytes <= 0 {
		return 0, false, fmt.Errorf("size must be positive")
	}
	return bytes, true, nil
}

func sizeUnitBytes(unit string) (int64, bool) {
	// Bare "m"/"k"/"g" are rejected so values like "30m" stay Go durations
	// (30 minutes) instead of being read as megabytes.
	switch strings.ToLower(strings.TrimSpace(unit)) {
	case "b", "byte", "bytes":
		return 1, true
	case "kb", "kib":
		return 1024, true
	case "mb", "mib":
		return 1024 * 1024, true
	case "gb", "gib":
		return 1024 * 1024 * 1024, true
	default:
		return 0, false
	}
}

type numRest struct {
	num  string
	rest string
}

func splitLeadingNumber(s string) numRest {
	i := 0
	for i < len(s) {
		r := rune(s[i])
		if unicode.IsDigit(r) || r == '.' {
			i++
			continue
		}
		break
	}
	return numRest{num: s[:i], rest: s[i:]}
}

func parseHumanDuration(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty duration")
	}
	// Go duration tokens (1h, 30m, 24h) first.
	if d, err := time.ParseDuration(raw); err == nil {
		return d, nil
	}
	fields := strings.Fields(strings.ToLower(raw))
	var numStr, unit string
	switch len(fields) {
	case 1:
		split := splitLeadingNumber(fields[0])
		if split.num == "" || split.rest == "" {
			return 0, fmt.Errorf("not a duration")
		}
		numStr, unit = split.num, split.rest
	case 2:
		numStr, unit = fields[0], fields[1]
	default:
		return 0, fmt.Errorf("not a duration")
	}
	n, err := strconv.ParseFloat(numStr, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("duration amount must be a positive number")
	}
	switch unit {
	case "ms", "millisecond", "milliseconds":
		return time.Duration(n * float64(time.Millisecond)), nil
	case "s", "sec", "secs", "second", "seconds":
		return time.Duration(n * float64(time.Second)), nil
	case "m", "min", "mins", "minute", "minutes":
		return time.Duration(n * float64(time.Minute)), nil
	case "h", "hr", "hrs", "hour", "hours":
		return time.Duration(n * float64(time.Hour)), nil
	case "d", "day", "days":
		return time.Duration(n * float64(24*time.Hour)), nil
	case "w", "week", "weeks":
		return time.Duration(n * float64(7*24*time.Hour)), nil
	default:
		return 0, fmt.Errorf("unknown duration unit %q", unit)
	}
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// requireNonEmptyString rejects nil and non-string YAML values instead of
// fmt.Sprint coercion (which turns null into the literal "<nil>").
func requireNonEmptyString(field string, value any) (string, error) {
	s, ok := value.(string)
	if !ok {
		return "", configErr(field, "must be a non-empty string")
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return "", configErr(field, "must be a non-empty string")
	}
	return s, nil
}
