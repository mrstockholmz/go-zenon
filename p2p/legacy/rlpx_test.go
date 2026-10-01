package legacy

import (
	"crypto/rand"
	"crypto/sha256"
	"io"
	"net"
	"testing"

	"github.com/zenon-network/go-zenon/p2p"
)

// testSecrets returns secrets for testing.  The egressMAC and ingressMAC
// are fresh hash instances; the two sides of a pipe use swapped MACs.
func testSecrets(t *testing.T) (aesKey, macKey []byte) {
	t.Helper()
	aesKey = make([]byte, 32)
	macKey = make([]byte, 32)
	if _, err := rand.Read(aesKey); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(macKey); err != nil {
		t.Fatal(err)
	}
	return aesKey, macKey
}

// TestFrameRWHandshakeBound verifies that during the handshake phase
// (before raiseFrameLimit), WriteMsg rejects frames larger than
// baseProtocolMaxMsgSize, and ReadMsg rejects oversized frame headers.
func TestFrameRWHandshakeBound(t *testing.T) {
	aesKey, macKey := testSecrets(t)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	reader := newRLPXFrameRW(c1, secrets{
		AES:        aesKey,
		MAC:        macKey,
		EgressMAC:  sha256.New(),
		IngressMAC: sha256.New(),
	})

	// Write path: a frame larger than baseProtocolMaxMsgSize must be rejected.
	largePayload := make([]byte, baseProtocolMaxMsgSize+100)
	msg := p2p.Msg{
		Code:    0x10,
		Size:    uint32(len(largePayload)),
		Payload: bytesReader(largePayload),
	}
	err := reader.WriteMsg(msg)
	if err == nil {
		t.Error("expected WriteMsg to reject oversized frame during handshake phase")
	} else {
		t.Logf("WriteMsg correctly rejected: %v", err)
	}

	// Read path: create a writer-side frame RW with independent MAC instances
	// (same initial state, but not shared — each side evolves its own).
	writer := newRLPXFrameRW(c2, secrets{
		AES:        aesKey,
		MAC:        macKey,
		EgressMAC:  sha256.New(),
		IngressMAC: sha256.New(),
	})

	go func() {
		writeOversizedFrame(t, writer, baseProtocolMaxMsgSize+100)
	}()

	_, err = reader.ReadMsg()
	if err == nil {
		t.Error("expected ReadMsg to reject oversized frame during handshake phase")
	} else {
		t.Logf("ReadMsg correctly rejected: %v", err)
	}
}

// TestFrameRWPostHandshakeBound verifies that after raiseFrameLimit(),
// the frame reader accepts frames larger than baseProtocolMaxMsgSize.
func TestFrameRWPostHandshakeBound(t *testing.T) {
	aesKey, macKey := testSecrets(t)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	writer := newRLPXFrameRW(c1, secrets{
		AES:        aesKey,
		MAC:        macKey,
		EgressMAC:  sha256.New(),
		IngressMAC: sha256.New(),
	})
	reader := newRLPXFrameRW(c2, secrets{
		AES:        aesKey,
		MAC:        macKey,
		EgressMAC:  sha256.New(),
		IngressMAC: sha256.New(),
	})
	writer.raiseFrameLimit()
	reader.raiseFrameLimit()

	// Write a frame larger than 2 KiB but smaller than 10 MiB.
	payload := make([]byte, 4096) // 4 KiB > 2 KiB handshake bound
	msg := p2p.Msg{
		Code:    0x10,
		Size:    uint32(len(payload)),
		Payload: bytesReader(payload),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- writer.WriteMsg(msg)
	}()

	got, err := reader.ReadMsg()
	if err != nil {
		t.Fatalf("ReadMsg failed after raiseFrameLimit: %v", err)
	}
	if got.Code != 0x10 {
		t.Errorf("expected code 0x10, got 0x%x", got.Code)
	}
	if err := <-errCh; err != nil {
		t.Errorf("WriteMsg failed after raiseFrameLimit: %v", err)
	}
}

// TestFrameRWWriteBoundAfterRaise verifies that even after raiseFrameLimit,
// WriteMsg still rejects frames above maxFrameSizeLimit.
func TestFrameRWWriteBoundAfterRaise(t *testing.T) {
	aesKey, macKey := testSecrets(t)
	c1, _ := net.Pipe()
	defer c1.Close()

	rw := newRLPXFrameRW(c1, secrets{
		AES:        aesKey,
		MAC:        macKey,
		EgressMAC:  sha256.New(),
		IngressMAC: sha256.New(),
	})
	rw.raiseFrameLimit()

	// A frame larger than maxFrameSizeLimit (10 MiB) must be rejected.
	// We don't actually allocate 10 MiB — just set the Size field.
	msg := p2p.Msg{
		Code:    0x10,
		Size:    maxFrameSizeLimit + 1,
		Payload: bytesReader(nil),
	}
	err := rw.WriteMsg(msg)
	if err == nil {
		t.Error("expected WriteMsg to reject frame above maxFrameSizeLimit")
	} else {
		t.Logf("WriteMsg correctly rejected: %v", err)
	}
}

// writeOversizedFrame crafts and writes a complete encrypted frame with the
// given fsize (but a minimal body), using the writer's cipher and MAC.
// The reader should reject the frame after parsing the header, without
// reading the body.
func writeOversizedFrame(t *testing.T, writer *rlpxFrameRW, fsize uint32) {
	t.Helper()

	// Build the plaintext header.
	headbuf := make([]byte, 32)
	putInt24(fsize, headbuf)
	copy(headbuf[3:], zeroHeader)

	// Encrypt the first 16 bytes.
	writer.enc.XORKeyStream(headbuf[:16], headbuf[:16])

	// Compute the header MAC.
	mac := updateMAC(writer.egressMAC, writer.macCipher, headbuf[:16])
	copy(headbuf[16:], mac)

	if _, err := writer.conn.Write(headbuf); err != nil {
		t.Error(err)
	}
	// Do NOT write the body — ReadMsg should reject based on the header alone.
}

// bytesReader returns an io.Reader over a byte slice.
func bytesReader(b []byte) io.Reader {
	return &sliceReader{b: b, i: 0}
}

type sliceReader struct {
	b []byte
	i int
}

func (r *sliceReader) Read(p []byte) (n int, err error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n = copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}
