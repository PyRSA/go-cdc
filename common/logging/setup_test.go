package logging

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/PyRSA/go-cdc/common/logfmt"
	"github.com/PyRSA/go-cdc/composer/definition"
)

func TestSetupUnconfiguredLeavesDefault(t *testing.T) {
	orig := slog.Default()
	t.Cleanup(func() { slog.SetDefault(orig) })

	dir := t.TempDir()
	path := filepath.Join(dir, "probe.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	slog.SetDefault(slog.New(logfmt.NewHandler(f, slog.LevelInfo)))
	closer, err := Setup(definition.Logging{})
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	slog.Info("kept")
	_ = f.Close()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "kept") {
		t.Fatalf("default handler replaced unexpectedly: %q", body)
	}
}

func TestSetupLevelFiltering(t *testing.T) {
	orig := slog.Default()
	t.Cleanup(func() { slog.SetDefault(orig) })

	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	closer, err := Setup(definition.Logging{
		Configured: true,
		Level:      "WARN",
		Format:     "text",
		File: &definition.LoggingFile{
			Path:      path,
			SizeBytes: 1 << 20,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	slog.Info("hidden")
	slog.Warn("visible")
	_ = closer.Close()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if strings.Contains(text, "hidden") {
		t.Fatalf("INFO must be filtered: %q", text)
	}
	if !strings.Contains(text, "visible") {
		t.Fatalf("WARN missing: %q", text)
	}
}

func TestSetupJSONFormat(t *testing.T) {
	orig := slog.Default()
	t.Cleanup(func() { slog.SetDefault(orig) })
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	closer, err := Setup(definition.Logging{
		Configured: true,
		Level:      "INFO",
		Format:     "json",
		File:       &definition.LoggingFile{Path: path, SizeBytes: 1 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	slog.Info("hello", "k", 1)
	_ = closer.Close()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("json %q: %v", body, err)
	}
	if payload["msg"] != "hello" {
		t.Fatalf("%#v", payload)
	}
}

func TestSetupConsoleAndFile(t *testing.T) {
	orig := slog.Default()
	t.Cleanup(func() { slog.SetDefault(orig) })

	// Capture stdout for the console target.
	rOut, wOut, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = wOut
	t.Cleanup(func() { os.Stdout = old })

	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	closer, err := Setup(definition.Logging{
		Configured: true,
		Level:      "INFO",
		Format:     "text",
		Console:    &definition.LoggingConsole{Stream: "stdout"},
		File:       &definition.LoggingFile{Path: path, SizeBytes: 1 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	slog.Info("fanout")
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}
	_ = wOut.Close()
	consoleBody, err := io.ReadAll(rOut)
	if err != nil {
		t.Fatal(err)
	}
	fileBody, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(consoleBody), "fanout") {
		t.Fatalf("console: %q", consoleBody)
	}
	if !strings.Contains(string(fileBody), "fanout") {
		t.Fatalf("file: %q", fileBody)
	}
}

func TestSetupTextKeepsModuleField(t *testing.T) {
	orig := slog.Default()
	t.Cleanup(func() { slog.SetDefault(orig) })
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	closer, err := Setup(definition.Logging{
		Configured: true,
		Level:      "INFO",
		Format:     "text",
		File:       &definition.LoggingFile{Path: path, SizeBytes: 1 << 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closer.Close()
	logfmt.Module("pipeline").Info("status")
	_ = closer.Close()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "[pipeline]") {
		t.Fatalf("module missing: %q", body)
	}
}
