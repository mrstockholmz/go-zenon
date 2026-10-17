package nom

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/zenon-network/go-zenon/common/types"
)

// TestDeserializeAccountBlock_BadChildBlock checks that
// DeserializeAccountBlock returns an error (not a panic) when a child
// block has a malformed field, and that the error identifies the child
// index. Covers the recursion in DeProtoAccountBlock at account_block.go:331.
func TestDeserializeAccountBlock_BadChildBlock(t *testing.T) {
	pb := validAccountBlockProto()

	// Child 0 is valid; child 1 has a wrong-length address.
	goodChild := validAccountBlockProto()
	badChild := validAccountBlockProto()
	badChild.Address = &types.AddressProto{Address: make([]byte, 10)} // wrong length

	pb.DescendantBlocks = []*AccountBlockProto{goodChild, badChild}

	data, err := proto.Marshal(pb)
	if err != nil {
		t.Fatal(err)
	}
	_, err = DeserializeAccountBlock(data)
	if err == nil {
		t.Fatal("expected error for AccountBlockProto with malformed descendant, got nil")
	}
	if !strings.Contains(err.Error(), "invalid descendant block at index 1") {
		t.Fatalf("expected error to mention child index 1, got: %v", err)
	}
	if !strings.Contains(err.Error(), "invalid address") {
		t.Fatalf("expected error to mention 'invalid address', got: %v", err)
	}
}

// TestDeserializeMomentum_MalformedContentAddress verifies that
// DeserializeMomentum returns an error (not a panic) when a momentum-content
// entry has a malformed address, and that the error identifies the content
// index. Covers the chain DeProtoMomentum → DeProtoMomentumContent →
// DeProtoAccountHeader → DeProtoAddress.
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

	// Add a content entry with a wrong-length address (5 bytes, not 20).
	pb.Content = []*types.AccountHeaderProto{
		{
			Address: &types.AddressProto{Address: []byte{1, 2, 3, 4, 5}},
			HashHeight: &types.HashHeightProto{
				Hash:   &types.HashProto{Hash: make([]byte, 32)},
				Height: 1,
			},
		},
	}

	data, err := proto.Marshal(pb)
	if err != nil {
		t.Fatal(err)
	}
	_, err = DeserializeMomentum(data)
	if err == nil {
		t.Fatal("expected error for MomentumProto with malformed content address, got nil")
	}
	if !strings.Contains(err.Error(), "invalid momentum content at index 0") {
		t.Fatalf("expected error to mention content index 0, got: %v", err)
	}
	if !strings.Contains(err.Error(), "invalid address") {
		t.Fatalf("expected error to mention 'invalid address', got: %v", err)
	}
}

// TestDeserializeAccountBlock_NestingLimitExceeded verifies that
// DeserializeAccountBlock returns an error (not a stack overflow) when the
// wire-format data nests AccountBlockProto descendants past the protobuf
// decoder's default recursion limit (10 000). The recursion guard lives in
// the protobuf library, not in DeProtoAccountBlock itself, so this test pins
// the assumption that proto.Unmarshal rejects the input before the recursive
// DeProtoAccountBlock runs.
func TestDeserializeAccountBlock_NestingLimitExceeded(t *testing.T) {
	// Build raw wire-format bytes: field 13 (descendantBlocks, wire type 2)
	// wrapping an empty message, nested past the recursion limit.
	// Each level adds a tag byte + varint length prefix.
	//
	// We build from the innermost outward: start with an empty payload,
	// then wrap it in a field-13 length-delimited record repeatedly.
	payload := []byte{}
	for i := 0; i < 10001; i++ {
		// Prepend field 13 tag (0x6A = 13<<3 | 2) + varint length + payload.
		buf := make([]byte, 0, len(payload)+6)
		buf = protowire.AppendTag(buf, 13, protowire.BytesType)
		buf = protowire.AppendVarint(buf, uint64(len(payload)))
		buf = append(buf, payload...)
		payload = buf
	}

	_, err := DeserializeAccountBlock(payload)
	if err == nil {
		t.Fatal("expected error for deeply nested AccountBlockProto, got nil")
	}
	// The protobuf decoder returns "exceeded maximum recursion depth" (or a
	// wrapping of it). We accept any error — the key assertion is that we get
	// an error rather than a stack overflow or a successful deserialize.
	t.Logf("got expected error: %v", err)
}
