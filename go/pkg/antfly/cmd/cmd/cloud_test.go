package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRunAntflyCloudMissingBinary(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	var stdout, stderr bytes.Buffer
	code := runAntflyCloud([]string{"status"}, strings.NewReader(""), &stdout, &stderr)
	if code != 127 {
		t.Fatalf("exit code = %d, want 127", code)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	msg := stderr.String()
	for _, want := range []string{"antfly-cloud is not installed", "antfly cloud", "brew install antflydb/taps/antfly-cloud"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("missing %q in stderr: %s", want, msg)
		}
	}
}

func TestRunAntflyCloudDelegatesArgsStdioAndExitCode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script test")
	}
	dir := t.TempDir()
	logPath := filepath.Join(dir, "args.txt")
	bin := filepath.Join(dir, antflyCloudBinary)
	script := `#!/bin/sh
input=$(/bin/cat)
printf 'stdin=%s' "$input"
printf 'stderr:%s\n' "$ANTFLY_SHIM_TEST" >&2
printf '%s\n' "$@" > "` + logPath + `"
exit 23
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	t.Setenv("ANTFLY_SHIM_TEST", "present")
	var stdout, stderr bytes.Buffer
	code := runAntflyCloud([]string{"status", "--json"}, strings.NewReader("hello"), &stdout, &stderr)
	if code != 23 {
		t.Fatalf("exit code = %d, want 23", code)
	}
	if stdout.String() != "stdin=hello" {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "stderr:present") {
		t.Fatalf("stderr = %q", stderr.String())
	}
	args, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(args) != "status\n--json\n" {
		t.Fatalf("args = %q", string(args))
	}
}
