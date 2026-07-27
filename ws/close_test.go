package ws

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestStatusCodeValidForSend(t *testing.T) {
	tests := []struct {
		code StatusCode
		want bool
	}{
		{0, false},
		{999, false},
		{StatusNormalClosure, true},
		{StatusGoingAway, true},
		{StatusProtocolError, true},
		{StatusUnsupportedData, true},
		{1004, false}, // reserved, never assigned
		{StatusNoStatusRcvd, false},
		{StatusAbnormalClosure, false},
		{StatusInvalidFramePayloadData, true},
		{StatusPolicyViolation, true},
		{StatusMessageTooBig, true},
		{StatusMandatoryExtension, true},
		{StatusInternalError, true},
		{StatusServiceRestart, true},
		{StatusTryAgainLater, true},
		{StatusBadGateway, true},
		{1015, false}, // TLS handshake failure, local reporting only
		{1016, false},
		{1999, false},
		{2000, false},
		{2999, false},
		{3000, true}, // registered private use
		{3999, true},
		{4000, true}, // application private use
		{4999, true},
		{5000, false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprint(int(tt.code)), func(t *testing.T) {
			if got := tt.code.validForSend(); got != tt.want {
				t.Errorf("StatusCode(%d).validForSend() = %v, want %v", tt.code, got, tt.want)
			}
		})
	}
}

func TestParseClosePayload(t *testing.T) {
	closeFrame := func(code StatusCode, reason string) []byte {
		b := binary.BigEndian.AppendUint16(nil, uint16(code))
		return append(b, reason...)
	}

	t.Run("empty payload means no status", func(t *testing.T) {
		got, err := parseClosePayload(nil)
		if err != nil {
			t.Fatalf("parseClosePayload() error = %v", err)
		}
		if got.Code != StatusNoStatusRcvd {
			t.Errorf("Code = %v, want %v", got.Code, StatusNoStatusRcvd)
		}
	})

	t.Run("code and reason", func(t *testing.T) {
		got, err := parseClosePayload(closeFrame(StatusGoingAway, "bye"))
		if err != nil {
			t.Fatalf("parseClosePayload() error = %v", err)
		}
		if got.Code != StatusGoingAway || got.Reason != "bye" {
			t.Errorf("got %+v, want {1001 bye}", got)
		}
	})

	t.Run("one byte payload", func(t *testing.T) {
		_, err := parseClosePayload([]byte{0x03})
		if err == nil {
			t.Fatal("parseClosePayload() = nil error, want a failure")
		}
		if got := closeStatusFor(err); got != StatusProtocolError {
			t.Errorf("closeStatusFor() = %v, want %v", got, StatusProtocolError)
		}
	})

	invalidCodes := []StatusCode{0, 999, 1004, StatusNoStatusRcvd, StatusAbnormalClosure, 1015, 1016, 2000, 2999, 5000}
	for _, code := range invalidCodes {
		t.Run(fmt.Sprintf("rejects code %d", int(code)), func(t *testing.T) {
			_, err := parseClosePayload(closeFrame(code, ""))
			if err == nil {
				t.Fatalf("parseClosePayload(%d) = nil error, want a failure", code)
			}
			if got := closeStatusFor(err); got != StatusProtocolError {
				t.Errorf("closeStatusFor() = %v, want %v", got, StatusProtocolError)
			}
		})
	}

	t.Run("reason must be valid UTF-8", func(t *testing.T) {
		payload := append(closeFrame(StatusNormalClosure, ""), 0xFF, 0xFE)
		_, err := parseClosePayload(payload)
		if err == nil {
			t.Fatal("parseClosePayload() = nil error, want a failure")
		}
		if got := closeStatusFor(err); got != StatusInvalidFramePayloadData {
			t.Errorf("closeStatusFor() = %v, want %v", got, StatusInvalidFramePayloadData)
		}
	})
}

func TestAppendClosePayload(t *testing.T) {
	buf := make([]byte, 0, maxControlPayload)

	t.Run("encodes code and reason", func(t *testing.T) {
		got, err := appendClosePayload(buf, StatusPolicyViolation, "nope")
		if err != nil {
			t.Fatalf("appendClosePayload() error = %v", err)
		}
		if want := 2 + len("nope"); len(got) != want {
			t.Fatalf("len = %d, want %d", len(got), want)
		}
		if code := StatusCode(binary.BigEndian.Uint16(got)); code != StatusPolicyViolation {
			t.Errorf("code = %v, want %v", code, StatusPolicyViolation)
		}
		if string(got[2:]) != "nope" {
			t.Errorf("reason = %q, want %q", got[2:], "nope")
		}
	})

	t.Run("rejects an unsendable code", func(t *testing.T) {
		if _, err := appendClosePayload(buf, StatusAbnormalClosure, ""); err == nil {
			t.Error("appendClosePayload(1006) = nil error, want a failure")
		}
	})

	t.Run("rejects an oversized reason", func(t *testing.T) {
		_, err := appendClosePayload(buf, StatusNormalClosure, strings.Repeat("x", maxCloseReason+1))
		if err == nil {
			t.Fatal("appendClosePayload() = nil error, want a failure")
		}
	})

	t.Run("accepts a reason at the limit", func(t *testing.T) {
		got, err := appendClosePayload(buf, StatusNormalClosure, strings.Repeat("x", maxCloseReason))
		if err != nil {
			t.Fatalf("appendClosePayload() error = %v", err)
		}
		if len(got) != maxControlPayload {
			t.Errorf("len = %d, want %d", len(got), maxControlPayload)
		}
	})
}

func TestCloseStatus(t *testing.T) {
	t.Run("finds a wrapped CloseError", func(t *testing.T) {
		err := fmt.Errorf("reading: %w", CloseError{Code: StatusGoingAway})
		if got := CloseStatus(err); got != StatusGoingAway {
			t.Errorf("CloseStatus() = %v, want %v", got, StatusGoingAway)
		}
	})

	t.Run("reports -1 for other errors", func(t *testing.T) {
		if got := CloseStatus(errors.New("boom")); got != -1 {
			t.Errorf("CloseStatus() = %v, want -1", got)
		}
	})

	t.Run("reports -1 for nil", func(t *testing.T) {
		if got := CloseStatus(nil); got != -1 {
			t.Errorf("CloseStatus() = %v, want -1", got)
		}
	})
}

func TestCloseStatusForDefaultsToInternalError(t *testing.T) {
	if got := closeStatusFor(errors.New("unexpected")); got != StatusInternalError {
		t.Errorf("closeStatusFor() = %v, want %v", got, StatusInternalError)
	}
}
