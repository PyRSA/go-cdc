package definition

import (
	"strings"
	"testing"
	"time"
)

func TestParseLoggingAbsentKeepsUnconfigured(t *testing.T) {
	pipe, err := Parse([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	if pipe.Logging.Configured {
		t.Fatal("logging must stay unconfigured when the key is absent")
	}
}

func TestParseLoggingDefaults(t *testing.T) {
	body := sample + `
logging: {}
`
	pipe, err := Parse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if !pipe.Logging.Configured || pipe.Logging.Level != "INFO" || pipe.Logging.Format != "text" {
		t.Fatalf("%#v", pipe.Logging)
	}
	if pipe.Logging.Console != nil || pipe.Logging.File != nil {
		t.Fatalf("empty logging must not invent targets: %#v", pipe.Logging)
	}
}

func TestParseLoggingConsoleAndFile(t *testing.T) {
	body := sample + `
logging:
  level: DEBUG
  format: json
  console:
    stream: stdout
  file:
    path: ./logs/go-cdc.log
    rotation: 100 MB
    retention: 30 days
    compression: gz
`
	pipe, err := Parse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	log := pipe.Logging
	if log.Level != "DEBUG" || log.Format != "json" {
		t.Fatalf("%#v", log)
	}
	if log.Console == nil || log.Console.Stream != "stdout" {
		t.Fatalf("console %#v", log.Console)
	}
	if log.File == nil || log.File.Path != "./logs/go-cdc.log" {
		t.Fatalf("file %#v", log.File)
	}
	if log.File.SizeBytes != 100*1024*1024 {
		t.Fatalf("size %d", log.File.SizeBytes)
	}
	if log.File.RetainAge != 30*24*time.Hour || log.File.RetainCount != 0 {
		t.Fatalf("retention age=%s count=%d", log.File.RetainAge, log.File.RetainCount)
	}
	if !log.File.CompressGzip {
		t.Fatal("expected gzip")
	}
}

func TestParseLoggingRotationVariants(t *testing.T) {
	cases := []struct {
		raw   string
		size  int64
		every time.Duration
		daily bool
	}{
		{"50MB", 50 * 1024 * 1024, 0, false},
		{"1 GiB", 1024 * 1024 * 1024, 0, false},
		{"1 day", 0, 24 * time.Hour, false},
		{"12 hours", 0, 12 * time.Hour, false},
		{"00:00", 0, 24 * time.Hour, true},
		{"9:30", 0, 24 * time.Hour, true},
	}
	for _, tc := range cases {
		body := sample + `
logging:
  file:
    path: ./logs/a.log
    rotation: ` + tc.raw + `
`
		pipe, err := Parse([]byte(body))
		if err != nil {
			t.Fatalf("%s: %v", tc.raw, err)
		}
		f := pipe.Logging.File
		if f.SizeBytes != tc.size || f.Every != tc.every || (f.DailyAt != nil) != tc.daily {
			t.Fatalf("%s: size=%d every=%s daily=%v", tc.raw, f.SizeBytes, f.Every, f.DailyAt)
		}
	}
}

func TestParseLoggingRetentionCount(t *testing.T) {
	body := sample + `
logging:
  file:
    path: ./logs/a.log
    retention: 10
`
	pipe, err := Parse([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if pipe.Logging.File.RetainCount != 10 || pipe.Logging.File.RetainAge != 0 {
		t.Fatalf("%#v", pipe.Logging.File)
	}
}

func TestParseLoggingRejectsIllegal(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{"level", "logging:\n  level: TRACE\n", "logging.level"},
		{"format", "logging:\n  format: xml\n", "logging.format"},
		{"stream", "logging:\n  console:\n    stream: file\n", "logging.console.stream"},
		{"path", "logging:\n  file:\n    rotation: 1 MB\n", "logging.file.path"},
		{"rotation", "logging:\n  file:\n    path: a.log\n    rotation: weeklyly\n", "logging.file.rotation"},
		{"compression", "logging:\n  file:\n    path: a.log\n    compression: zip\n", "logging.file.compression"},
		{"unknown", "logging:\n  sink: x\n", "logging.sink"},
		{"logging-bool", "logging: true\n", "logging"},
		{"logging-string", "logging: nope\n", "logging"},
		{"logging-number", "logging: 1\n", "logging"},
		{"logging-list", "logging:\n  - a\n", "logging"},
		{"logging-null", "logging: null\n", "logging"},
		{"path-null", "logging:\n  file:\n    path: null\n", "logging.file.path"},
		{"path-number", "logging:\n  file:\n    path: 1\n", "logging.file.path"},
		{"path-bool", "logging:\n  file:\n    path: true\n", "logging.file.path"},
		{"path-empty", "logging:\n  file:\n    path: \"\"\n", "logging.file.path"},
		{"path-blank", "logging:\n  file:\n    path: \"   \"\n", "logging.file.path"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(sample + "\n" + tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v want field %s", err, tc.want)
			}
		})
	}
}
