package cable

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"go-cable/ws"
)

// TestManyIdleConnections is the measurement this port exists for: how much a
// process pays to hold connections that are doing nothing, and how long the one
// piece of work that scales with them takes.
//
// Opt-in, because it holds tens of thousands of file descriptors:
//
//	CABLE_LOAD=1 go test ./cable/ -run TestManyIdleConnections -v -timeout 10m
//	CABLE_LOAD=1 CABLE_CONNS=50000 go test ./cable/ -run TestManyIdleConnections -v -timeout 20m
//
// Both ends of every connection live in this process, so the memory figures
// include the client side too — roughly twice what a server alone would use. The
// sweep timing does not: it is server-side work only.
func TestManyIdleConnections(t *testing.T) {
	if os.Getenv("CABLE_LOAD") == "" {
		t.Skip("set CABLE_LOAD=1 to run the load test (it opens tens of thousands of sockets)")
	}

	connections := 10_000
	if raw := os.Getenv("CABLE_CONNS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("CABLE_CONNS=%q is not a number: %v", raw, err)
		}
		connections = parsed
	}

	// Two descriptors per connection, plus room for everything else.
	if limit := fileLimit(t); limit < uint64(2*connections)+256 {
		t.Skipf("the file descriptor limit is %d, too low for %d connections", limit, connections)
	}

	// A heartbeat slow enough not to interfere: the sweep is timed directly below.
	srv := New(&Options{
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		HeartbeatInterval: time.Hour,
	})
	defer srv.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	hs := &http.Server{Handler: srv}
	go hs.Serve(listener)
	defer hs.Close()

	url := "ws://" + listener.Addr().String()

	runtime.GC()
	baseRSS, baseHeap := memory()
	baseGoroutines := runtime.NumGoroutine()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	start := time.Now()
	clients, opened, err := dialMany(t, ctx, url, connections)
	if errors.Is(err, syscall.EADDRNOTAVAIL) {
		// Both ends are on loopback, so every connection consumes a local port.
		// The range (net.ipv4.ip_local_port_range, about 28k by default) bounds
		// this test, not the server: a server's connections come from many hosts.
		t.Skipf("ran out of ephemeral ports after %d of %d connections; lower CABLE_CONNS or widen net.ipv4.ip_local_port_range", opened, connections)
	}
	if err != nil {
		t.Fatalf("dialling %d connections: %v", connections, err)
	}
	t.Logf("connected %d clients in %v", len(clients), time.Since(start).Round(time.Millisecond))

	waitFor(t, "every connection to register", func() bool { return srv.ConnectionCount() == connections })

	runtime.GC()
	rss, heap := memory()
	t.Logf("memory: RSS %s (+%s), heap %s (+%s) — both ends of every connection",
		bytesHuman(rss), bytesHuman(rss-baseRSS), bytesHuman(heap), bytesHuman(heap-baseHeap))
	t.Logf("per connection: %d bytes RSS, %d bytes heap (halve for a server-only figure)",
		(rss-baseRSS)/uint64(connections), (heap-baseHeap)/uint64(connections))
	t.Logf("goroutines: %d (+%d, %.1f per connection)",
		runtime.NumGoroutine(), runtime.NumGoroutine()-baseGoroutines,
		float64(runtime.NumGoroutine()-baseGoroutines)/float64(connections))

	// The heartbeat sweep, measured on the same code path the ticker uses. Five
	// runs, because the first also warms the snapshot slice.
	frame, err := newPing(time.Now()).encode()
	if err != nil {
		t.Fatalf("encoding a ping: %v", err)
	}

	var sweeps, snapshots []time.Duration
	var conns []*Connection
	for range 5 {
		began := time.Now()
		conns = srv.snapshot(conns)
		snapshots = append(snapshots, time.Since(began))

		began = time.Now()
		conns = srv.sweep(frame, conns)
		sweeps = append(sweeps, time.Since(began))
	}
	slices.Sort(sweeps)
	slices.Sort(snapshots)
	median, snapshot := sweeps[len(sweeps)/2], snapshots[len(snapshots)/2]

	t.Logf("heartbeat sweep over %d connections: median %v, worst %v (%.1f ns per connection)",
		connections, median.Round(time.Microsecond), sweeps[len(sweeps)-1].Round(time.Microsecond),
		float64(median.Nanoseconds())/float64(connections))

	// Decomposed, because the two halves scale differently: taking the snapshot is
	// a locked map walk, while queueing wakes one writer goroutine per connection
	// and is therefore at the scheduler's mercy.
	t.Logf("  of which the snapshot is %v (%.0f%%); the rest is queueing and %d goroutine wake-ups",
		snapshot.Round(time.Microsecond), 100*float64(snapshot)/float64(median), connections)

	// The budget, per connection so that it still means something at other sizes:
	// 10µs each is 100ms at ten thousand connections, and a sweep happens every
	// three seconds. Anything near that would mean the server spends real time on
	// bookkeeping instead of work.
	const budget = 10 * time.Microsecond
	if perConnection := median / time.Duration(connections); perConnection > budget {
		t.Errorf("sweep took %v per connection, want under %v (%v total over %d connections)",
			perConnection, budget, median, connections)
	}

	// Every client must have received the ping, which is what proves the sweep did
	// the work rather than dropping it.
	for i, c := range clients[:min(len(clients), 100)] {
		if _, _, err := c.Read(ctx); err != nil {
			t.Fatalf("client %d did not receive the heartbeat: %v", i, err)
		}
	}

	// Closing is part of the measurement: this is what a deploy does.
	began := time.Now()
	for _, c := range clients {
		c.CloseNow()
	}
	waitFor(t, "connections to drain", func() bool { return srv.ConnectionCount() == 0 })
	t.Logf("dropped %d connections in %v", connections, time.Since(began).Round(time.Millisecond))

	waitForStableGoroutines(t)
	if got := runtime.NumGoroutine(); got > baseGoroutines+10 {
		t.Errorf("goroutines = %d after every connection closed, baseline %d", got, baseGoroutines)
	}
}

// dialMany opens connections in parallel and reads each one's welcome, so that
// every connection returned is registered and idle. On failure it reports how many
// did open, which is what tells a caller whether it hit a system limit.
func dialMany(t *testing.T, ctx context.Context, url string, count int) ([]*ws.Conn, int, error) {
	t.Helper()

	conns := make([]*ws.Conn, count)
	errs := make([]error, count)

	// Bounded parallelism: the point is to open sockets, not to schedule
	// goroutines.
	const workers = 64
	var wg sync.WaitGroup
	work := make(chan int, workers)

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				conn, err := ws.Dial(ctx, url, &ws.DialOptions{Subprotocols: []string{Subprotocol}})
				if err != nil {
					errs[i] = err
					continue
				}
				if _, _, err := conn.Read(ctx); err != nil {
					errs[i] = fmt.Errorf("reading the welcome: %w", err)
					conn.CloseNow()
					continue
				}
				conns[i] = conn
			}
		}()
	}

	for i := range count {
		work <- i
	}
	close(work)
	wg.Wait()

	// Registered before reporting failure, so whatever did open is released
	// either way.
	t.Cleanup(func() {
		for _, c := range conns {
			if c != nil {
				c.CloseNow()
			}
		}
	})

	opened := 0
	for _, c := range conns {
		if c != nil {
			opened++
		}
	}
	for _, err := range errs {
		if err != nil {
			return nil, opened, err
		}
	}
	return conns, opened, nil
}

// memory reports resident set size and Go heap in bytes. RSS is what an operator
// sees; the heap is what is portable.
func memory() (rss, heap uint64) {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return residentSetSize(), stats.HeapAlloc
}

// residentSetSize reads RSS from /proc, returning 0 where that does not exist.
func residentSetSize() uint64 {
	file, err := os.Open("/proc/self/statm")
	if err != nil {
		return 0
	}
	defer file.Close()

	var pages uint64
	if _, err := fmt.Fscan(file, new(uint64), &pages); err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}

// fileLimit reports the soft limit on open files.
func fileLimit(t *testing.T) uint64 {
	t.Helper()

	file, err := os.Open("/proc/self/limits")
	if err != nil {
		return ^uint64(0) // unknown: let the test try
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "Max open files") {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(line, "Max open files"))
		if len(fields) == 0 {
			break
		}
		limit, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			break
		}
		return limit
	}
	return ^uint64(0)
}

func bytesHuman(n uint64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// BenchmarkHeartbeatSweep isolates the sweep from the delivery it sets off. No
// writer goroutines run here, so nothing is woken and nothing is written: what is
// left is the locked map walk and one buffered channel send per connection.
//
// The difference between this and the sweep timing in TestManyIdleConnections is
// the cost of actually delivering — goroutine wake-ups and socket writes — which
// the sweeping goroutine pays for because the runtime hands the CPU to the writers
// it readies. Measured on this machine: ~240ns per connection here against ~3µs
// there, so the sweep itself is a twelfth of the cost of the delivery it triggers.
//
// The reported allocations are one-time rather than per-sweep: the first sweep
// calls Done() on each connection's context, which is where a context lazily
// creates its channel. Four times the iterations reports a quarter the
// allocations, which is what a fixed total looks like when it is amortised.
func BenchmarkHeartbeatSweep(b *testing.B) {
	frame, err := newPing(time.Now()).encode()
	if err != nil {
		b.Fatalf("encoding a ping: %v", err)
	}

	for _, count := range []int{100, 1_000, 10_000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			// Not closed on purpose: these connections have no socket, and Close
			// would try to close one. Nothing was started that needs stopping —
			// no heartbeat, no connection goroutines, and the default pub/sub
			// runs none either.
			srv := New(&Options{
				Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
				HeartbeatInterval: time.Hour,
			})

			request := httptest.NewRequest(http.MethodGet, "/cable", nil)
			for range count {
				// No socket and no goroutines: transmit only touches the queue,
				// which the loop below drains, so nothing else is ever reached.
				srv.add(newConnection(srv, nil, request, nil, srv.opts.Logger))
			}

			var conns []*Connection
			b.ReportAllocs()
			for b.Loop() {
				conns = srv.sweep(frame, conns)

				b.StopTimer()
				for _, c := range conns {
					<-c.send
				}
				b.StartTimer()
			}
			b.ReportMetric(float64(count), "conns/op")
		})
	}
}
