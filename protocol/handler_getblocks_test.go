package protocol

import (
	"bytes"
	"testing"

	"github.com/ethereum/go-ethereum/rlp"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
)

// makeGetBlocksPayload encodes a list of hashes as a GetBlocksMsg payload.
func makeGetBlocksPayload(t *testing.T, hashes []types.Hash) []byte {
	t.Helper()
	encoded, err := rlp.EncodeToBytes(hashes)
	if err != nil {
		t.Fatalf("encode hashes: %v", err)
	}
	return encoded
}

// makeTestMomentum returns a minimal DetailedMomentum with the given hash.
func makeTestMomentum(hash types.Hash) *nom.DetailedMomentum {
	return &nom.DetailedMomentum{
		Momentum: &nom.Momentum{
			Hash:   hash,
			Height: 100,
		},
		AccountBlocks: []*nom.AccountBlock{},
	}
}

// makeLargeMomentum returns a DetailedMomentum with a large Data field.
// The Data field is the simplest way to inflate the RLP-encoded size.
func makeLargeMomentum(hash types.Hash, dataSize int) *nom.DetailedMomentum {
	return &nom.DetailedMomentum{
		Momentum: &nom.Momentum{
			Hash:   hash,
			Height: 100,
			Data:   make([]byte, dataSize),
		},
		AccountBlocks: []*nom.AccountBlock{},
	}
}

// streamFromPayload creates an RLP stream from a GetBlocksMsg payload.
func streamFromPayload(t *testing.T, payload []byte) *rlp.Stream {
	t.Helper()
	stream := rlp.NewStream(bytes.NewReader(payload), uint64(len(payload)))
	if _, err := stream.List(); err != nil {
		t.Fatalf("open list: %v", err)
	}
	return stream
}

func TestGatherBlocks_DeduplicatesHashes(t *testing.T) {
	hash1 := types.HexToHashPanic("0100000000000000000000000000000000000000000000000000000000000000")
	hash2 := types.HexToHashPanic("0200000000000000000000000000000000000000000000000000000000000000")

	// Request: [hash1, hash1, hash1, hash2] — hash1 repeated 3 times.
	hashes := []types.Hash{hash1, hash1, hash1, hash2}
	payload := makeGetBlocksPayload(t, hashes)
	stream := streamFromPayload(t, payload)

	lookupCount := 0
	blocks, hashCount, err := gatherBlocksForReply(stream, func(h types.Hash) *nom.DetailedMomentum {
		lookupCount++
		return makeTestMomentum(h)
	})
	if err != nil {
		t.Fatalf("gatherBlocksForReply: %v", err)
	}

	// All 4 hashes counted, but only 2 unique lookups.
	if hashCount != 4 {
		t.Errorf("hashCount = %d, want 4", hashCount)
	}
	if lookupCount != 2 {
		t.Errorf("lookupCount = %d, want 2 (dedup should skip repeats)", lookupCount)
	}
	if len(blocks) != 2 {
		t.Errorf("len(blocks) = %d, want 2", len(blocks))
	}
}

func TestGatherBlocks_ReplySizeCap(t *testing.T) {
	largeHash := types.HexToHashPanic("ff00000000000000000000000000000000000000000000000000000000000000")
	largeBlock := makeLargeMomentum(largeHash, 3*1024*1024) // 3 MB Data field

	// Verify this block exceeds the soft limit when encoded.
	encoded, err := rlp.EncodeToBytes(largeBlock)
	if err != nil {
		t.Fatalf("encode large block: %v", err)
	}
	t.Logf("large block encodes to %d bytes (soft limit %d)", len(encoded), softResponseLimit)

	if len(encoded) <= softResponseLimit {
		t.Skipf("test block (%d bytes) does not exceed soft limit (%d), adjust test data",
			len(encoded), softResponseLimit)
	}

	hashes := []types.Hash{largeHash}
	payload := makeGetBlocksPayload(t, hashes)
	stream := streamFromPayload(t, payload)

	blocks, hashCount, err := gatherBlocksForReply(stream, func(h types.Hash) *nom.DetailedMomentum {
		return largeBlock
	})
	if err != nil {
		t.Fatalf("gatherBlocksForReply: %v", err)
	}

	// The block is included (always return at least one), but the loop
	// stops after it because the reply exceeds the soft limit.
	if len(blocks) != 1 {
		t.Errorf("len(blocks) = %d, want 1 (block included but stops after)", len(blocks))
	}
	if hashCount != 1 {
		t.Errorf("hashCount = %d, want 1", hashCount)
	}
}

func TestGatherBlocks_CapAccumulates(t *testing.T) {
	smallHash := types.HexToHashPanic("0100000000000000000000000000000000000000000000000000000000000000")
	smallBlock := makeTestMomentum(smallHash)

	smallEncoded, err := rlp.EncodeToBytes(smallBlock)
	if err != nil {
		t.Fatalf("encode small block: %v", err)
	}

	mediumHash := types.HexToHashPanic("0200000000000000000000000000000000000000000000000000000000000000")
	mediumBlock := makeLargeMomentum(mediumHash, 2*1024*1024) // 2 MB Data field
	mediumEncoded, err := rlp.EncodeToBytes(mediumBlock)
	if err != nil {
		t.Fatalf("encode medium block: %v", err)
	}
	t.Logf("small=%d bytes, medium=%d bytes, soft limit=%d", len(smallEncoded), len(mediumEncoded), softResponseLimit)

	if len(smallEncoded)+len(mediumEncoded) <= softResponseLimit {
		t.Skip("combined size does not exceed soft limit, adjust test data")
	}

	// Request: [small, medium] — small fits, medium pushes over.
	hashes := []types.Hash{smallHash, mediumHash}
	payload := makeGetBlocksPayload(t, hashes)
	stream := streamFromPayload(t, payload)

	blocks, hashCount, err := gatherBlocksForReply(stream, func(h types.Hash) *nom.DetailedMomentum {
		if h == smallHash {
			return smallBlock
		}
		return mediumBlock
	})
	if err != nil {
		t.Fatalf("gatherBlocksForReply: %v", err)
	}

	// Both blocks are included: small fits, medium is appended (pushing
	// the total over the limit), then the loop stops. This ensures at
	// least the first found block is always returned.
	if len(blocks) != 2 {
		t.Errorf("len(blocks) = %d, want 2 (small + medium, stops after medium)", len(blocks))
	}
	if hashCount != 2 {
		t.Errorf("hashCount = %d, want 2", hashCount)
	}
}

func TestGatherBlocks_StopsAfterLimitExceeded(t *testing.T) {
	// Three blocks: small, medium (pushes over), large (never reached).
	smallHash := types.HexToHashPanic("0100000000000000000000000000000000000000000000000000000000000000")
	smallBlock := makeTestMomentum(smallHash)

	mediumHash := types.HexToHashPanic("0200000000000000000000000000000000000000000000000000000000000000")
	mediumBlock := makeLargeMomentum(mediumHash, 2*1024*1024)

	largeHash := types.HexToHashPanic("0300000000000000000000000000000000000000000000000000000000000000")
	largeBlock := makeLargeMomentum(largeHash, 3*1024*1024)

	hashes := []types.Hash{smallHash, mediumHash, largeHash}
	payload := makeGetBlocksPayload(t, hashes)
	stream := streamFromPayload(t, payload)

	lookupCount := 0
	blocks, hashCount, err := gatherBlocksForReply(stream, func(h types.Hash) *nom.DetailedMomentum {
		lookupCount++
		switch h {
		case smallHash:
			return smallBlock
		case mediumHash:
			return mediumBlock
		default:
			return largeBlock
		}
	})
	if err != nil {
		t.Fatalf("gatherBlocksForReply: %v", err)
	}

	// small + medium are included (medium pushes over the limit),
	// then the loop stops. large is never looked up.
	if len(blocks) != 2 {
		t.Errorf("len(blocks) = %d, want 2 (small + medium, stops before large)", len(blocks))
	}
	if hashCount != 2 {
		t.Errorf("hashCount = %d, want 2 (stops decoding after limit)", hashCount)
	}
	if lookupCount != 2 {
		t.Errorf("lookupCount = %d, want 2 (large block never looked up)", lookupCount)
	}
}

func TestGatherBlocks_MaxBlockFetchLimit(t *testing.T) {
	// Request more unique hashes than MaxBlockFetch (128).
	var hashes []types.Hash
	for i := 0; i < 200; i++ {
		var h types.Hash
		h[0] = byte(i)
		h[1] = byte(i >> 8)
		hashes = append(hashes, h)
	}
	payload := makeGetBlocksPayload(t, hashes)
	stream := streamFromPayload(t, payload)

	blocks, hashCount, err := gatherBlocksForReply(stream, func(h types.Hash) *nom.DetailedMomentum {
		return makeTestMomentum(h)
	})
	if err != nil {
		t.Fatalf("gatherBlocksForReply: %v", err)
	}

	if len(blocks) != 128 {
		t.Errorf("len(blocks) = %d, want 128 (MaxBlockFetch)", len(blocks))
	}
	if hashCount != 128 {
		t.Errorf("hashCount = %d, want 128", hashCount)
	}
}

func TestGatherBlocks_EmptyRequest(t *testing.T) {
	payload := makeGetBlocksPayload(t, nil)
	stream := streamFromPayload(t, payload)

	blocks, hashCount, err := gatherBlocksForReply(stream, func(h types.Hash) *nom.DetailedMomentum {
		t.Error("GetBlock called for empty request")
		return nil
	})
	if err != nil {
		t.Fatalf("gatherBlocksForReply: %v", err)
	}
	if len(blocks) != 0 {
		t.Errorf("len(blocks) = %d, want 0", len(blocks))
	}
	if hashCount != 0 {
		t.Errorf("hashCount = %d, want 0", hashCount)
	}
}

func TestGatherBlocks_MissingBlocksSkipped(t *testing.T) {
	hash1 := types.HexToHashPanic("0100000000000000000000000000000000000000000000000000000000000000")
	hash2 := types.HexToHashPanic("0200000000000000000000000000000000000000000000000000000000000000")
	hash3 := types.HexToHashPanic("0300000000000000000000000000000000000000000000000000000000000000")

	hashes := []types.Hash{hash1, hash2, hash3}
	payload := makeGetBlocksPayload(t, hashes)
	stream := streamFromPayload(t, payload)

	blocks, hashCount, err := gatherBlocksForReply(stream, func(h types.Hash) *nom.DetailedMomentum {
		if h == hash2 {
			return nil // not found
		}
		return makeTestMomentum(h)
	})
	if err != nil {
		t.Fatalf("gatherBlocksForReply: %v", err)
	}

	if len(blocks) != 2 {
		t.Errorf("len(blocks) = %d, want 2 (hash2 not found)", len(blocks))
	}
	if hashCount != 3 {
		t.Errorf("hashCount = %d, want 3", hashCount)
	}
}
