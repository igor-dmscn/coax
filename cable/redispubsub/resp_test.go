package redispubsub

import (
	"bufio"
	"errors"
	"strings"
	"testing"
)

// parse reads one value from a string, which is how every reply arrives.
func parse(t *testing.T, in string) (value, error) {
	t.Helper()
	return readValue(bufio.NewReader(strings.NewReader(in)), defaultMaxReplySize)
}

func TestReadValue(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		check func(*testing.T, value)
	}{
		{
			name: "simple string",
			in:   "+OK\r\n",
			check: func(t *testing.T, v value) {
				if v.typ != typeSimpleString || v.str() != "OK" {
					t.Errorf("got %q of type %q", v.str(), string(v.typ))
				}
			},
		},
		{
			name: "error",
			in:   "-WRONGPASS invalid username-password pair\r\n",
			check: func(t *testing.T, v value) {
				if v.typ != typeError || v.str() != "WRONGPASS invalid username-password pair" {
					t.Errorf("got %q of type %q", v.str(), string(v.typ))
				}
			},
		},
		{
			name: "integer",
			in:   ":42\r\n",
			check: func(t *testing.T, v value) {
				if v.typ != typeInteger || v.num != 42 {
					t.Errorf("got %d of type %q", v.num, string(v.typ))
				}
			},
		},
		{
			name: "negative integer",
			in:   ":-1\r\n",
			check: func(t *testing.T, v value) {
				if v.num != -1 {
					t.Errorf("got %d, want -1", v.num)
				}
			},
		},
		{
			name: "bulk string",
			in:   "$5\r\nhello\r\n",
			check: func(t *testing.T, v value) {
				if v.str() != "hello" {
					t.Errorf("got %q, want %q", v.str(), "hello")
				}
			},
		},
		{
			name: "empty bulk string",
			in:   "$0\r\n\r\n",
			check: func(t *testing.T, v value) {
				if v.null || v.str() != "" {
					t.Errorf("got %q, null=%v, want an empty string", v.str(), v.null)
				}
			},
		},
		{
			// Payloads are binary: a broadcast may contain anything, and CRLF
			// inside one must not be mistaken for the terminator.
			name: "bulk string containing CRLF and NUL",
			in:   "$7\r\na\r\nb\x00c\r\r\n",
			check: func(t *testing.T, v value) {
				if v.str() != "a\r\nb\x00c\r" {
					t.Errorf("got %q", v.str())
				}
			},
		},
		{
			name: "null bulk string",
			in:   "$-1\r\n",
			check: func(t *testing.T, v value) {
				if !v.null {
					t.Error("null = false, want true")
				}
			},
		},
		{
			name: "empty array",
			in:   "*0\r\n",
			check: func(t *testing.T, v value) {
				if v.typ != typeArray || len(v.arr) != 0 {
					t.Errorf("got %d elements of type %q", len(v.arr), string(v.typ))
				}
			},
		},
		{
			name: "null array",
			in:   "*-1\r\n",
			check: func(t *testing.T, v value) {
				if !v.null {
					t.Error("null = false, want true")
				}
			},
		},
		{
			// The shape that matters: this is a published message.
			name: "message push",
			in:   "*3\r\n$7\r\nmessage\r\n$6\r\nroom_1\r\n$15\r\n{\"body\":\"hi\"}\r\n\r\n",
			check: func(t *testing.T, v value) {
				if len(v.arr) != 3 {
					t.Fatalf("got %d elements, want 3", len(v.arr))
				}
				if v.arr[0].str() != "message" || v.arr[1].str() != "room_1" {
					t.Errorf("got %q on %q", v.arr[0].str(), v.arr[1].str())
				}
				if want := "{\"body\":\"hi\"}\r\n"; v.arr[2].str() != want {
					t.Errorf("payload = %q, want %q", v.arr[2].str(), want)
				}
			},
		},
		{
			name: "subscribe confirmation",
			in:   "*3\r\n$9\r\nsubscribe\r\n$6\r\nroom_1\r\n:1\r\n",
			check: func(t *testing.T, v value) {
				if len(v.arr) != 3 || v.arr[0].str() != "subscribe" || v.arr[2].num != 1 {
					t.Errorf("got %+v", v)
				}
			},
		},
		{
			// PING in subscribe mode answers with an array, not +PONG.
			name: "pong push",
			in:   "*2\r\n$4\r\npong\r\n$0\r\n\r\n",
			check: func(t *testing.T, v value) {
				if len(v.arr) != 2 || v.arr[0].str() != "pong" {
					t.Errorf("got %+v", v)
				}
			},
		},
		{
			name: "nested array",
			in:   "*1\r\n*1\r\n:7\r\n",
			check: func(t *testing.T, v value) {
				if len(v.arr) != 1 || len(v.arr[0].arr) != 1 || v.arr[0].arr[0].num != 7 {
					t.Errorf("got %+v", v)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parse(t, tt.in)
			if err != nil {
				t.Fatalf("readValue(%q) error = %v", tt.in, err)
			}
			tt.check(t, got)
		})
	}
}

func TestReadValueRejects(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{name: "empty input", in: ""},
		{name: "unknown type byte", in: "!oops\r\n"},
		{name: "bare LF", in: "+OK\n"},
		{name: "no terminator", in: "+OK"},
		{name: "empty line", in: "\r\n"},
		{name: "integer with letters", in: ":12a\r\n"},
		{name: "integer that is only a sign", in: ":-\r\n"},
		{name: "integer with a leading plus", in: ":+1\r\n"},
		{name: "integer out of range", in: ":99999999999999999999\r\n"},
		{name: "bulk string length that is not a number", in: "$abc\r\n"},
		{name: "bulk string shorter than its header", in: "$10\r\nshort\r\n"},
		{name: "bulk string not followed by CRLF", in: "$5\r\nhelloXX"},
		{name: "negative bulk string length other than -1", in: "$-2\r\n"},
		{name: "negative array length other than -1", in: "*-2\r\n"},
		{name: "array shorter than its header", in: "*2\r\n:1\r\n"},
		{name: "array too long", in: "*100000\r\n"},
		{name: "nested too deep", in: strings.Repeat("*1\r\n", maxDepth+2) + ":1\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parse(t, tt.in); err == nil {
				t.Errorf("readValue(%q) error = nil, want a failure", tt.in)
			}
		})
	}
}

// TestReadValueBoundsAllocation checks the limit is applied to the header, before
// the payload is allocated: a bogus length must not be able to ask for gigabytes.
func TestReadValueBoundsAllocation(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("$9999999999\r\n"))

	_, err := readValue(reader, 1024)
	if err == nil {
		t.Fatal("readValue() error = nil, want a size limit failure")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("error = %v, want it to name the limit", err)
	}
}

func TestReadValueSurvivesALongLine(t *testing.T) {
	// A line longer than the reader's buffer cannot be scanned for its
	// terminator, so it must be an error rather than a partial parse.
	reader := bufio.NewReader(strings.NewReader("+" + strings.Repeat("x", 1<<20) + "\r\n"))

	if _, err := readValue(reader, defaultMaxReplySize); !errors.Is(err, errProtocol) {
		t.Errorf("readValue() error = %v, want %v", err, errProtocol)
	}
}

func TestReadValueReadsASequence(t *testing.T) {
	// Replies arrive back to back on one connection, so the parser must leave the
	// reader positioned exactly at the next one.
	reader := bufio.NewReader(strings.NewReader("+OK\r\n:1\r\n$2\r\nhi\r\n"))

	for _, want := range []string{"OK", "1", "hi"} {
		got, err := readValue(reader, defaultMaxReplySize)
		if err != nil {
			t.Fatalf("readValue() error = %v", err)
		}
		if text := got.str(); text != want && got.num != 1 {
			t.Errorf("got %q, want %q", text, want)
		}
	}
}

func TestParseInt(t *testing.T) {
	tests := map[string]struct {
		want    int64
		wantErr bool
	}{
		"0":                   {want: 0},
		"7":                   {want: 7},
		"-7":                  {want: -7},
		"9223372036854775807": {want: 1<<63 - 1},
		"9223372036854775808": {wantErr: true},
		"":                    {wantErr: true},
		"-":                   {wantErr: true},
		" 1":                  {wantErr: true},
		"1 ":                  {wantErr: true},
		"1-2":                 {wantErr: true},
	}

	for in, tt := range tests {
		t.Run(in, func(t *testing.T) {
			got, err := parseInt([]byte(in))
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Fatalf("parseInt(%q) error = %v, want an error: %v", in, err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Errorf("parseInt(%q) = %d, want %d", in, got, tt.want)
			}
		})
	}
}

func TestAppendCommand(t *testing.T) {
	got := string(appendCommand(nil, "SUBSCRIBE", "room_1"))
	if want := "*2\r\n$9\r\nSUBSCRIBE\r\n$6\r\nroom_1\r\n"; got != want {
		t.Errorf("appendCommand() = %q, want %q", got, want)
	}
}

func TestAppendPublish(t *testing.T) {
	// The payload goes out as bytes, so anything JSON can contain survives.
	got := string(appendPublish(nil, "room_1", []byte("{\"body\":\"a\r\nb\"}")))
	want := "*3\r\n$7\r\nPUBLISH\r\n$6\r\nroom_1\r\n$15\r\n{\"body\":\"a\r\nb\"}\r\n"
	if got != want {
		t.Errorf("appendPublish() = %q, want %q", got, want)
	}
}

// TestAppendCommandRoundTrip: what we write is what our own parser reads, which
// is what makes the fake Redis server in these tests trustworthy.
func TestAppendCommandRoundTrip(t *testing.T) {
	args := []string{"AUTH", "alice", "p@ss w\r\nord"}

	reader := bufio.NewReader(strings.NewReader(string(appendCommand(nil, args...))))
	got, err := readValue(reader, defaultMaxReplySize)
	if err != nil {
		t.Fatalf("readValue() error = %v", err)
	}
	if len(got.arr) != len(args) {
		t.Fatalf("got %d arguments, want %d", len(got.arr), len(args))
	}
	for i, want := range args {
		if got.arr[i].str() != want {
			t.Errorf("argument %d = %q, want %q", i, got.arr[i].str(), want)
		}
	}
}

func TestAppendReusesItsBuffer(t *testing.T) {
	// Commands are written from one reused buffer per connection, so appending
	// must start from the slice it is given.
	buf := make([]byte, 0, 128)
	for range 3 {
		buf = appendCommand(buf[:0], "PING")
	}
	if got, want := string(buf), "*1\r\n$4\r\nPING\r\n"; got != want {
		t.Errorf("buffer = %q, want %q", got, want)
	}
}

// FuzzReadValue checks that no reply can panic the parser or make it allocate
// without bound. A Redis connection is a trust boundary: the bytes may come from
// a compromised or simply different server.
func FuzzReadValue(f *testing.F) {
	seeds := []string{
		"+OK\r\n",
		"-ERR nope\r\n",
		":1\r\n",
		":-1\r\n",
		"$5\r\nhello\r\n",
		"$-1\r\n",
		"$0\r\n\r\n",
		"*0\r\n",
		"*-1\r\n",
		"*3\r\n$7\r\nmessage\r\n$6\r\nroom_1\r\n$2\r\nhi\r\n",
		"*3\r\n$9\r\nsubscribe\r\n$6\r\nroom_1\r\n:1\r\n",
		"*2\r\n$4\r\npong\r\n$0\r\n\r\n",
		"*1\r\n*1\r\n:7\r\n",
		"$99999999999999999999\r\n",
		"*99999999\r\n",
		"*1\r\n*1\r\n*1\r\n*1\r\n*1\r\n*1\r\n*1\r\n*1\r\n*1\r\n*1\r\n:1\r\n",
	}
	for _, seed := range seeds {
		f.Add([]byte(seed))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		reader := bufio.NewReader(strings.NewReader(string(data)))

		// A small limit so a fuzz case cannot legitimately allocate much: the
		// interesting failure is a parser that allocates before checking.
		v, err := readValue(reader, 4096)
		if err != nil {
			return
		}

		// A successful parse must be self-consistent, since the adapter reads
		// these fields without re-checking them.
		switch v.typ {
		case typeSimpleString, typeError, typeInteger, typeBulkString, typeArray:
		default:
			t.Fatalf("parsed a value of unknown type %q", string(v.typ))
		}
		if len(v.arr) > maxArrayLen {
			t.Fatalf("parsed an array of %d elements, over the %d limit", len(v.arr), maxArrayLen)
		}
		if len(v.text) > 4096 {
			t.Fatalf("parsed %d bytes of text, over the 4096 limit", len(v.text))
		}
	})
}
