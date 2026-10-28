package mock

import (
	"testing"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/verifier"
)

// A user block whose address has never had an account block reaches
// getContext's nil-accountStore branch.  Frontier() on the empty
// account store returns (nil, nil), and the old code dereferenced
// that nil frontier.  The verifier must return ErrABPreviousMissing
// instead of panicking.
func TestGetContext_NilFrontier_ReturnsPrevMissing(t *testing.T) {
	z := NewMockZenon(t)
	defer z.StopPanic()

	z.InsertMomentumsTo(5)

	v := verifier.NewAccountBlockVerifier(z.Chain(), z.Consensus())

	block := &nom.AccountBlock{
		Version:   1,
		BlockType: nom.BlockTypeUserSend,
		Address:   types.ParseAddressPanic("z1qzmu3t6m8zmljvj3e9s4vqgn6llggjzyyjsnl8"),
		Height:    2,
		// Nonzero PreviousHash and MomentumAcknowledged are required to
		// pass the early guards in getContext.
		PreviousHash:         types.NewHash([]byte{1}),
		MomentumAcknowledged: types.HashHeight{Hash: types.NewHash([]byte{2}), Height: 1},
	}

	err := v.AccountBlock(block)
	if err != verifier.ErrABPreviousMissing {
		t.Fatalf("expected ErrABPreviousMissing, got %v", err)
	}
}
