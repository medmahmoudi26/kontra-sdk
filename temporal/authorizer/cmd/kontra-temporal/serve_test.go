package main

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.temporal.io/server/common/primitives"
)

// serveChild makes the test binary run main() instead of the tests: the server blocks
// until a signal and exits the process on error, so a test runs it as a child process.
const serveChild = "KONTRA_TEST_SERVE_CHILD"

func TestMain(m *testing.M) {
	if os.Getenv(serveChild) == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// TestServerStartsWithTheInternalFrontendOffLoopback runs the real server with its internal
// frontend bound to 127.0.0.2: a loopback address on Linux that a connection to 127.0.0.1
// does not reach. The system worker starts only by dialling the internal frontend, so the
// server comes up only if it dials the address the frontend binds. With the address fixed
// at 127.0.0.1 it gave up after its start timeout ("failed to start service worker").
func TestServerStartsWithTheInternalFrontendOffLoopback(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a Temporal server")
	}
	const internalIP = "127.0.0.2"
	probe, err := net.Listen("tcp", net.JoinHostPort(internalIP, "0"))
	if err != nil {
		t.Skipf("cannot bind %s on this machine: %v", internalIP, err)
	}
	_ = probe.Close()

	var server *child
	for attempt := 1; ; attempt++ {
		env := map[string]string{
			"KONTRA_INTERNAL_FRONTEND_BIND_IP":   internalIP,
			"KONTRA_FRONTEND_PORT":               freePort(t, "127.0.0.1"),
			"KONTRA_HISTORY_PORT":                freePort(t, "127.0.0.1"),
			"KONTRA_MATCHING_PORT":               freePort(t, "127.0.0.1"),
			"KONTRA_WORKER_PORT":                 freePort(t, "127.0.0.1"),
			"KONTRA_INTERNAL_FRONTEND_PORT":      freePort(t, internalIP),
			"KONTRA_INTERNAL_FRONTEND_HTTP_PORT": freePort(t, internalIP),
			"KONTRA_LOG_FORMAT":                  "json",
			"KONTRA_LOG_LEVEL":                   "info",
		}
		// The system worker starts last, and only once its client reached the internal frontend.
		server = startServer(t, env, `"msg":"worker service started"`)
		err := server.wait(2 * time.Minute)
		if err == nil {
			for key, value := range env {
				t.Setenv(key, value) // register-namespace below reads the same settings
			}
			break
		}
		server.stop()
		if attempt < 3 && strings.Contains(server.tail(), "address already in use") {
			continue // a port picked as free was taken before the server bound it
		}
		t.Fatalf("the server did not start: %v\n%s", err, server.tail())
	}
	t.Cleanup(server.stop)

	s, err := loadSettings()
	if err != nil {
		t.Fatal(err)
	}
	internal := s.address(primitives.InternalFrontendService, false)
	if conn, err := net.DialTimeout("tcp", internal, 5*time.Second); err != nil {
		t.Errorf("the internal frontend is not on %s: %v", internal, err)
	} else {
		_ = conn.Close()
	}
	// Control: nothing answers on 127.0.0.1 at that port, so the server cannot have
	// reached its internal frontend there.
	port := strconv.Itoa(s.rpc[primitives.InternalFrontendService].GRPCPort)
	if conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", port), 5*time.Second); err == nil {
		_ = conn.Close()
		t.Errorf("something answers on 127.0.0.1:%s: the test proves nothing", port)
	}
	// register-namespace dials the internal frontend at its default address, the same one.
	if err := registerNamespaces([]string{"kontra-dial-test"}); err != nil {
		t.Errorf("register-namespace through %s: %v", internal, err)
	}
}

// freePort is a port free on ip right now (the server binds it a moment later).
func freePort(t *testing.T, ip string) string {
	t.Helper()
	l, err := net.Listen("tcp", net.JoinHostPort(ip, "0"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
}

// child is the server process and what it printed (its tail), watched for a ready line.
type child struct {
	cmd    *exec.Cmd
	ready  string
	exited chan struct{} // closed once the process has exited and its output is read

	mu      sync.Mutex
	partial []byte
	lines   []string
	found   bool
	change  chan struct{} // closed and replaced whenever a line arrives
}

// startServer runs this test binary as the server, with every KONTRA_ variable of the
// host's environment replaced by env.
func startServer(t *testing.T, env map[string]string, ready string) *child {
	t.Helper()
	c := &child{ready: ready, exited: make(chan struct{}), change: make(chan struct{})}
	c.cmd = exec.Command(os.Args[0], "serve")
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "KONTRA_") {
			c.cmd.Env = append(c.cmd.Env, kv)
		}
	}
	c.cmd.Env = append(c.cmd.Env, serveChild+"=1")
	for key, value := range env {
		c.cmd.Env = append(c.cmd.Env, key+"="+value)
	}
	c.cmd.Stdout, c.cmd.Stderr = c, c
	if err := c.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.cmd.Wait(); close(c.exited) }()
	return c
}

// Write collects the child's output line by line (os/exec copies it here).
func (c *child) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.partial = append(c.partial, p...)
	for {
		i := bytes.IndexByte(c.partial, '\n')
		if i < 0 {
			break
		}
		line := string(c.partial[:i])
		c.partial = c.partial[i+1:]
		c.found = c.found || strings.Contains(line, c.ready)
		if c.lines = append(c.lines, line); len(c.lines) > 400 {
			c.lines = c.lines[len(c.lines)-200:]
		}
		close(c.change)
		c.change = make(chan struct{})
	}
	return len(p), nil
}

// wait returns once the ready line was printed, or an error when the server exited or the
// time is up.
func (c *child) wait(within time.Duration) error {
	deadline := time.After(within)
	for {
		c.mu.Lock()
		found, change := c.found, c.change
		c.mu.Unlock()
		if found {
			return nil
		}
		select {
		case <-change:
		case <-c.exited:
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.found {
				return nil
			}
			return errors.New("the server exited")
		case <-deadline:
			return fmt.Errorf("no %s within %s", c.ready, within)
		}
	}
}

// stop stops the child by its PID: SIGTERM, then SIGKILL if it does not exit.
func (c *child) stop() {
	select {
	case <-c.exited:
		return
	default:
	}
	_ = c.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-c.exited:
	case <-time.After(30 * time.Second):
		_ = c.cmd.Process.Kill()
		<-c.exited
	}
}

func (c *child) tail() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := strings.Join(c.lines, "\n") + "\n" + string(c.partial)
	if len(out) > 6000 {
		out = out[len(out)-6000:]
	}
	return out
}
