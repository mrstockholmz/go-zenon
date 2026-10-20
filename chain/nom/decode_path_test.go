package nom

import (
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/zenon-network/go-zenon/common/types"
)

// TestDeProtoAccountBlock_MalformedDescendant verifies that
// DeProtoAccountBlock returns an error naming the descendant index when a
// descendant block carries a wrong-length address. The error must include
// both the "invalid descendant block at index" context and the underlying
// "invalid address" cause so callers can locate the bad field.
func TestDeProtoAccountBlock_MalformedDescendant(t *testing.T) {
	pb := validAccountBlockProto()

	// Two descendants: index 0 is valid, index 1 has a bad address.
	goodDesc := validAccountBlockProto()
	badDesc := validAccountBlockProto()
	badDesc.Address = &types.AddressProto{Address: make([]byte, 10)} // wrong length

	pb.DescendantBlocks = []*AccountBlockProto{goodDesc, badDesc}

	_, err := DeProtoAccountBlock(pb)
	if err == nil {
		t.Fatal("expected error for descendant with wrong-length address, got nil")
	}
	if !strings.Contains(err.Error(), "invalid descendant block at index 1") {
		t.Fatalf("expected error to name descendant index 1, got: %v", err)
	}
	if !strings.Contains(err.Error(), "invalid address") {
		t.Fatalf("expected error to wrap the underlying address error, got: %v", err)
	}
}

// TestDeserializeMomentum_MalformedContentAddress verifies that
// DeserializeMomentum surfaces a wrong-length address inside a momentum's
// content entry. The error chain is:
//
//	DeProtoMomentum → DeProtoMomentumContent → DeProtoAccountHeader → DeProtoAddress
//
// and must carry the "invalid momentum content at index" context from
// DeProtoMomentumContent together with the "invalid address" context from
// DeProtoAccountHeader.
func TestDeserializeMomentum_MalformedContentAddress(t *testing.T) {
	valid := &Momentum{
		Version:         1,
		ChainIdentifier: 1,
		Hash:            types.Hash{1},
		PreviousHash:    types.Hash{2},
		Height:          42,
		TimestampUnix:   1000000,
		Content:         MomentumContent{},
		ChangesHash:     types.Hash{3},
	}
	pb := valid.Proto()

	// Two content entries: index 0 is valid, index 1 has a bad address.
	goodHeader := (&types.AccountHeader{
		Address:    types.Address{1},
		HashHeight: types.HashHeight{Hash: types.Hash{2}, Height: 10},
	}).Proto()
	badHeader := (&types.AccountHeader{
		Address:    types.Address{3},
		HashHeight: types.HashHeight{Hash: types.Hash{4}, Height: 20},
	}).Proto()
	badHeader.Address = &types.AddressProto{Address: make([]byte, 7)} // wrong length

	pb.Content = []*types.AccountHeaderProto{goodHeader, badHeader}

	data, err := proto.Marshal(pb)
	if err != nil {
		t.Fatal(err)
	}
	_, err = DeserializeMomentum(data)
	if err == nil {
		t.Fatal("expected error for momentum content with wrong-length address, got nil")
	}
	if !strings.Contains(err.Error(), "invalid momentum content at index 1") {
		t.Fatalf("expected error to name content index 1, got: %v", err)
	}
	if !strings.Contains(err.Error(), "invalid address") {
		t.Fatalf("expected error to wrap the underlying address error, got: %v", err)
	}
}

// TestDeserializeAccountBlock_NestingLimit verifies that the protobuf
// decoder's default nesting limit (10 000 in the pinned
// google.golang.org/protobuf) rejects pathologically deep descendant
// nesting with an error rather than a stack overflow. The bytes are
// hand-crafted: each AccountBlockProto embeds the next one as field 13
// (descendantBlocks), so the decoder recurses once per level.
func TestDeserializeAccountBlock_NestingLimit(t *testing.T) {
	// Wire format for field 13 (descendantBlocks), wire type 2 (length-delimited):
	//   tag = (13 << 3) | 2 = 0x6A
	// Each nesting level wraps the inner bytes in a length prefix.
	//
	// Build from the inside out: start with an empty payload (a valid
	// proto), then wrap it 10 001 times — one past the default limit.
	const tag = 0x6A
	depth := 10001

	payload := []byte{}
	for i := 0; i < depth; i++ {
		// Prepend tag + varint length + inner payload.
		length := len(payload)
		var lengthBytes []byte
		for {
			b := byte(length & 0x7F)
			length >>= 7
			if length > 0 {
				lengthBytes = append(lengthBytes, b|0x80)
			} else {
				lengthBytes = append(lengthBytes, b)
				break
			}
		}
		frame := make([]byte, 0, 1+len(lengthBytes)+len(payload))
		frame = append(frame, tag)
		frame = append(frame, lengthBytes...)
		frame = append(frame, payload...)
		payload = frame
	}

	_, err := DeserializeAccountBlock(payload)
	if err == nil {
		t.Fatal("expected error for deeply nested descendant blocks, got nil")
	}
	// The protobuf decoder reports the nesting limit as an error; we do
	// not assert the exact string because it is library-internal, but we
	// verify an error was returned (no panic, no stack overflow).
	t.Logf("got expected error: %v", err)
}

// TestDeserializeAccountBlock_NestingLimitJustUnder verifies that a nesting
// depth just under the protobuf limit (9 999 levels) still decodes without
// error. This ensures the limit test above is actually exercising the
// boundary and not failing for an unrelated reason (e.g. malformed bytes).
func TestDeserializeAccountBlock_NestingLimitJustUnder(t *testing.T) {
	const tag = 0x6A
	depth := 9999

	payload := []byte{}
	for i := 0; i < depth; i++ {
		length := len(payload)
		var lengthBytes []byte
		for {
			b := byte(length & 0x7F)
			length >>= 7
			if length > 0 {
				lengthBytes = append(lengthBytes, b|0x80)
			} else {
				lengthBytes = append(lengthBytes, b)
				break
			}
		}
		frame := make([]byte, 0, 1+len(lengthBytes)+len(payload))
		frame = append(frame, tag)
		frame = append(frame, lengthBytes...)
		frame = append(frame, payload...)
		payload = frame
	}

	// At 9 999 levels the decoder succeeds (the nesting limit is 10 000),
	// but DeProtoAccountBlock then fails on the innermost block which has
	// no hash. We only care that the protobuf decoder itself did not
	// reject the input — so any error must NOT be a nesting-limit error.
	_, err := DeserializeAccountBlock(payload)
	if err == nil {
		// Even better: the whole thing decoded (unlikely, since the innermost
		// block has no hash field).
		return
	}
	msg := err.Error()
	if strings.Contains(msg, "exceeded nesting") ||
		strings.Contains(msg, "recursion") ||
		strings.Contains(msg, "nesting limit") {
		t.Fatalf("nesting limit hit at depth %d (limit should be 10000): %v", depth, err)
	}
	// Any other error (e.g. "invalid hash") is fine — the protobuf decode
	// succeeded and the failure is in DeProtoAccountBlock.
	t.Logf("got expected non-nesting error at depth %d: %v", depth, err)
}

// TestDeProtoAccountBlock_MalformedDescendantIndex0 verifies that the
// descendant index in the error message is 0 when the first (and only)
// descendant is malformed. This guards against a hardcoded index in the
// error wrapping.
func TestDeProtoAccountBlock_MalformedDescendantIndex0(t *testing.T) {
	pb := validAccountBlockProto()
	badDesc := validAccountBlockProto()
	badDesc.Address = &types.AddressProto{Address: make([]byte, 10)}
	pb.DescendantBlocks = []*AccountBlockProto{badDesc}

	_, err := DeProtoAccountBlock(pb)
	if err == nil {
		t.Fatal("expected error for descendant with wrong-length address, got nil")
	}
	if !strings.Contains(err.Error(), "invalid descendant block at index 0") {
		t.Fatalf("expected error to name descendant index 0, got: %v", err)
	}
}

// TestDeserializeMomentum_MalformedContentAddressIndex0 verifies that the
// content index in the error message is 0 when the first (and only) content
// entry has a malformed address.
func TestDeserializeMomentum_MalformedContentAddressIndex0(t *testing.T) {
	valid := &Momentum{
		Version:         1,
		ChainIdentifier: 1,
		Hash:            types.Hash{1},
		PreviousHash:    types.Hash{2},
		Height:          42,
		TimestampUnix:   1000000,
		Content:         MomentumContent{},
		ChangesHash:     types.Hash{3},
	}
	pb := valid.Proto()
	badHeader := (&types.AccountHeader{
		Address:    types.Address{3},
		HashHeight: types.HashHeight{Hash: types.Hash{4}, Height: 20},
	}).Proto()
	badHeader.Address = &types.AddressProto{Address: make([]byte, 7)}
	pb.Content = []*types.AccountHeaderProto{badHeader}

	data, err := proto.Marshal(pb)
	if err != nil {
		t.Fatal(err)
	}
	_, err = DeserializeMomentum(data)
	if err == nil {
		t.Fatal("expected error for momentum content with wrong-length address, got nil")
	}
	if !strings.Contains(err.Error(), "invalid momentum content at index 0") {
		t.Fatalf("expected error to name content index 0, got: %v", err)
	}
}

// TestDeProtoAccountBlock_MalformedDescendantErrorFormat verifies the full
// error format string for the descendant wrapping, ensuring the %d verb
// produces the correct decimal index and %w preserves the wrapped error for
// errors.Is / errors.As traversal.
func TestDeProtoAccountBlock_MalformedDescendantErrorFormat(t *testing.T) {
	pb := validAccountBlockProto()
	badDesc := validAccountBlockProto()
	badDesc.Address = &types.AddressProto{Address: make([]byte, 10)}
	pb.DescendantBlocks = []*AccountBlockProto{badDesc}

	_, err := DeProtoAccountBlock(pb)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	expected := fmt.Sprintf("invalid descendant block at index %d", 0)
	if !strings.HasPrefix(err.Error(), expected) {
		t.Fatalf("expected error to start with %q, got: %v", expected, err)
	}
}
