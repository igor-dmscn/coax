package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

const autobahnImage = "crossbario/autobahn-testsuite"

// excludedCases skips the permessage-deflate groups, which are deliberately not
// implemented (see docs/go-port-plan.md §9.1). Excluding them keeps the report
// unambiguous instead of mixing real failures with a known gap.
var excludedCases = []string{"12.*", "13.*"}

// TestAutobahn runs the Autobahn WebSocket test suite against our server.
//
// The suite is the reference conformance check for RFC 6455 — roughly 500 cases
// covering framing, fragmentation, UTF-8 handling, close codes and limits. It
// runs in Docker, so it is opt-in:
//
//	WS_AUTOBAHN=1 go test ./ws/ -run TestAutobahn -timeout 15m
//
// The generated report is copied into testdata/autobahn/ so the result is
// reviewable without rerunning the suite.
func TestAutobahn(t *testing.T) {
	if os.Getenv("WS_AUTOBAHN") == "" {
		t.Skip("set WS_AUTOBAHN=1 to run the Autobahn conformance suite (requires Docker)")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	if out, err := exec.CommandContext(ctx, "docker", "info").CombinedOutput(); err != nil {
		t.Skipf("docker is not usable: %v: %s", err, out)
	}

	addr := startAutobahnEcho(t)
	reportDir := worldWritableTempDir(t)
	configDir := writeAutobahnConfig(t, addr)

	// --network=host lets the container reach the echo server on the loopback
	// address; the suite has no other network needs.
	cmd := exec.CommandContext(ctx, "docker", "run", "--rm",
		"--network=host",
		"-v", configDir+":/config",
		"-v", reportDir+":/reports",
		autobahnImage,
		"wstest", "--mode", "fuzzingclient", "--spec", "/config/fuzzingclient.json",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("running the suite: %v\n%s", err, tail(string(out), 40))
	}

	checkAutobahnReport(t, reportDir)
}

// startAutobahnEcho serves an echo endpoint for the suite to drive and returns
// its address. Limits are raised well past the defaults because the suite's 9.x
// cases send messages up to 16 MiB.
func startAutobahnEcho(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}

	opts := &AcceptOptions{
		// The suite connects without an Origin header, but be explicit.
		InsecureSkipVerify: true,
		MaxFrameSize:       64 << 20,
		MaxMessageSize:     64 << 20,
	}

	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := Accept(w, r, opts)
			if err != nil {
				return
			}
			defer conn.CloseNow()

			for {
				typ, reader, err := conn.Reader(r.Context())
				if err != nil {
					return
				}
				writer, err := conn.Writer(r.Context(), typ)
				if err != nil {
					return
				}
				if _, err := io.Copy(writer, reader); err != nil {
					writer.Close()
					return
				}
				if err := writer.Close(); err != nil {
					return
				}
			}
		}),
	}

	go srv.Serve(listener)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	})

	return listener.Addr().String()
}

func writeAutobahnConfig(t *testing.T, addr string) string {
	t.Helper()

	dir := worldWritableTempDir(t)
	config := map[string]any{
		"outdir": "/reports",
		"servers": []map[string]string{
			{"agent": "go-cable/ws", "url": "ws://" + addr},
		},
		"cases":         []string{"*"},
		"exclude-cases": excludedCases,
	}

	body, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		t.Fatalf("marshalling config: %v", err)
	}

	path := filepath.Join(dir, "fuzzingclient.json")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("writing config: %v", err)
	}
	return dir
}

// worldWritableTempDir returns a temporary directory the container can write
// to: a rootful Docker daemon runs the suite as root, and the suite emits two
// files per case, so the raw output stays out of the repository.
func worldWritableTempDir(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatalf("chmod %s: %v", dir, err)
	}
	return dir
}

// autobahnSummary is the committed record of a suite run: one small file we
// write ourselves, rather than the 600 root-owned files the suite produces.
type autobahnSummary struct {
	Image        string            `json:"image"`
	ExcludedCase []string          `json:"excluded_cases"`
	Total        int               `json:"total"`
	Tally        map[string]int    `json:"tally"`
	Cases        map[string]string `json:"cases"`
}

func writeAutobahnSummary(t *testing.T, index map[string]map[string]autobahnCase) {
	t.Helper()

	summary := autobahnSummary{
		Image:        autobahnImage,
		ExcludedCase: excludedCases,
		Tally:        map[string]int{},
		Cases:        map[string]string{},
	}
	for _, cases := range index {
		for name, result := range cases {
			summary.Total++
			summary.Tally[result.Behavior]++
			summary.Cases[name] = result.Behavior + "/" + result.BehaviorClose
		}
	}

	body, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		t.Fatalf("marshalling summary: %v", err)
	}
	dir := filepath.Join("testdata", "autobahn")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "summary.json"), append(body, '\n'), 0o644); err != nil {
		t.Fatalf("writing summary: %v", err)
	}
}

// autobahnCase is one entry of the suite's index.json.
type autobahnCase struct {
	Behavior      string `json:"behavior"`
	BehaviorClose string `json:"behaviorClose"`
}

// acceptable reports whether a behaviour string counts as a pass. INFORMATIONAL
// and UNIMPLEMENTED are outcomes the suite records for cases that do not assert
// conformance; NON-STRICT means a permitted alternative interpretation.
func acceptable(behavior string) bool {
	switch strings.ToUpper(behavior) {
	case "OK", "NON-STRICT", "INFORMATIONAL", "UNIMPLEMENTED":
		return true
	default:
		return false
	}
}

func checkAutobahnReport(t *testing.T, reportDir string) {
	t.Helper()

	body, err := os.ReadFile(filepath.Join(reportDir, "index.json"))
	if err != nil {
		t.Fatalf("reading the report index: %v", err)
	}

	var index map[string]map[string]autobahnCase
	if err := json.Unmarshal(body, &index); err != nil {
		t.Fatalf("parsing the report index: %v", err)
	}

	var failures []string
	total := 0
	for agent, cases := range index {
		for name, result := range cases {
			total++
			if !acceptable(result.Behavior) || !acceptable(result.BehaviorClose) {
				failures = append(failures, fmt.Sprintf("%s case %s: behavior=%s behaviorClose=%s",
					agent, name, result.Behavior, result.BehaviorClose))
			}
		}
	}

	if total == 0 {
		t.Fatal("the report contains no cases; the suite likely could not reach the server")
	}

	sort.Strings(failures)
	if len(failures) > 0 {
		t.Errorf("%d of %d Autobahn cases failed:\n%s", len(failures), total, strings.Join(failures, "\n"))
		return
	}

	writeAutobahnSummary(t, index)
	t.Logf("all %d Autobahn cases passed", total)
}

func tail(s string, lines int) string {
	parts := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(parts) > lines {
		parts = parts[len(parts)-lines:]
	}
	return strings.Join(parts, "\n")
}
