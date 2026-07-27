package ws

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// The allocation tests run against an in-memory net.Conn rather than a real
// socket and a peer goroutine, because testing.AllocsPerRun measures allocations
// process-wide: a concurrent peer would pollute the count. Real TCP deadline
// calls do not allocate either, so the numbers stay representative of the
// framing path, which is what the zero-allocation claim covers.

// skipUnderRace keeps allocation assertions honest: the race runtime allocates
// per operation, so the counts only mean something in a normal build.
func skipUnderRace(t *testing.T) {
	t.Helper()
	if raceEnabled {
		t.Skip("allocation counts are not measurable under -race")
	}
}

type fakeAddr struct{}

func (fakeAddr) Network() string { return "fake" }
func (fakeAddr) String() string  { return "fake" }

type fakeConn struct {
	r io.Reader
	w io.Writer
}

func (f *fakeConn) Read(p []byte) (int, error)  { return f.r.Read(p) }
func (f *fakeConn) Write(p []byte) (int, error) { return f.w.Write(p) }
func (f *fakeConn) Close() error                { return nil }
func (f *fakeConn) LocalAddr() net.Addr         { return fakeAddr{} }
func (f *fakeConn) RemoteAddr() net.Addr        { return fakeAddr{} }
func (f *fakeConn) SetDeadline(time.Time) error { return nil }

func (f *fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (f *fakeConn) SetWriteDeadline(time.Time) error { return nil }

// loopReader replays data endlessly so a read benchmark never runs dry.
type loopReader struct {
	data []byte
	off  int
}

func (l *loopReader) Read(p []byte) (int, error) {
	if l.off >= len(l.data) {
		l.off = 0
	}
	n := copy(p, l.data[l.off:])
	l.off += n
	return n, nil
}

// bakeFrame encodes one masked client frame, which is what a server reads.
func bakeFrame(op opcode, payload []byte) []byte {
	h := frameHeader{
		fin:        true,
		opcode:     op,
		masked:     true,
		payloadLen: int64(len(payload)),
		maskKey:    [4]byte{0x37, 0xfa, 0x21, 0x3d},
	}
	frame := appendFrameHeader(make([]byte, 0, maxHeaderSize), h)
	body := append([]byte(nil), payload...)
	mask(body, h.maskKey, 0)
	return append(frame, body...)
}

// serverConn builds a server-side Conn reading a repeating frame stream and
// discarding everything it writes.
func serverConn(stream []byte) *Conn {
	fc := &fakeConn{r: &loopReader{data: stream}, w: io.Discard}
	return newConn(fc, bufio.NewReader(fc), bufio.NewWriter(fc), false, "", 0, 0)
}

func TestReaderIsAllocationFree(t *testing.T) {
	skipUnderRace(t)
	conn := serverConn(bakeFrame(opText, []byte(strings.Repeat("x", 512))))
	buf := make([]byte, 4096)
	ctx := context.Background()

	read := func() {
		_, r, err := conn.Reader(ctx)
		if err != nil {
			t.Fatalf("Reader() error = %v", err)
		}
		for {
			_, err := r.Read(buf)
			if err == io.EOF {
				return
			}
			if err != nil {
				t.Fatalf("Read() error = %v", err)
			}
		}
	}

	if allocs := testing.AllocsPerRun(200, read); allocs != 0 {
		t.Errorf("Reader round trip = %v allocs/op, want 0", allocs)
	}
}

func TestWriteIsAllocationFree(t *testing.T) {
	skipUnderRace(t)
	conn := serverConn(nil)
	payload := []byte(strings.Repeat("y", 512))
	ctx := context.Background()

	write := func() {
		if err := conn.Write(ctx, MessageText, payload); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}

	if allocs := testing.AllocsPerRun(200, write); allocs != 0 {
		t.Errorf("Write = %v allocs/op, want 0", allocs)
	}
}

// TestWriterIsAllocationFree covers the streaming writer once its buffer has
// been created by a first use, since that first allocation is deliberate.
func TestWriterIsAllocationFree(t *testing.T) {
	skipUnderRace(t)
	conn := serverConn(nil)
	payload := []byte(strings.Repeat("z", 512))
	ctx := context.Background()

	stream := func() {
		w, err := conn.Writer(ctx, MessageBinary)
		if err != nil {
			t.Fatalf("Writer() error = %v", err)
		}
		if _, err := w.Write(payload); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}

	stream() // allocate the writer's buffer once
	if allocs := testing.AllocsPerRun(200, stream); allocs != 0 {
		t.Errorf("Writer round trip = %v allocs/op, want 0", allocs)
	}
}

// TestReadAllocatesForConvenience documents the deliberate difference: the
// []byte-returning API cannot avoid allocating, which is why Reader exists.
func TestReadAllocatesForConvenience(t *testing.T) {
	skipUnderRace(t)
	conn := serverConn(bakeFrame(opText, []byte(strings.Repeat("x", 512))))
	ctx := context.Background()

	allocs := testing.AllocsPerRun(50, func() {
		if _, _, err := conn.Read(ctx); err != nil {
			t.Fatalf("Read() error = %v", err)
		}
	})
	if allocs == 0 {
		t.Error("Read = 0 allocs/op; expected it to allocate, so the doc comment is wrong")
	}
}

func BenchmarkReader(b *testing.B) {
	for _, size := range []int{64, 512, 8192} {
		b.Run(byteSizeName(size), func(b *testing.B) {
			conn := serverConn(bakeFrame(opBinary, bytes.Repeat([]byte("x"), size)))
			buf := make([]byte, 16384)
			ctx := context.Background()

			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				_, r, err := conn.Reader(ctx)
				if err != nil {
					b.Fatal(err)
				}
				for {
					if _, err := r.Read(buf); err == io.EOF {
						break
					} else if err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func BenchmarkWrite(b *testing.B) {
	for _, size := range []int{64, 512, 8192} {
		b.Run(byteSizeName(size), func(b *testing.B) {
			conn := serverConn(nil)
			payload := bytes.Repeat([]byte("y"), size)
			ctx := context.Background()

			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				if err := conn.Write(ctx, MessageBinary, payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMask(b *testing.B) {
	key := [4]byte{0x37, 0xfa, 0x21, 0x3d}
	for _, size := range []int{64, 512, 8192} {
		b.Run(byteSizeName(size), func(b *testing.B) {
			buf := make([]byte, size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				mask(buf, key, 0)
			}
		})
	}
}

func byteSizeName(n int) string { return strconv.Itoa(n) + "B" }
