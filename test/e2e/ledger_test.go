//go:build e2e

package e2e

import (
	"math/big"
	"testing"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/vm/constants"
)

// TestLedger covers TEST-PLAN.md section 5.2: sends, receives, plasma and
// the account-block rules a node must enforce at publish.
func TestLedger(t *testing.T) {
	group(t)
	a := newAccount(t, 0, 100, 1000, 1000) // funded, fused plasma
	b := net.Key(runBase + 1)              // empty, no plasma: pays with proof of work

	var sendZnn *nom.AccountBlock

	t.Run("B-1_send_and_receive_znn_and_qsr", func(t *testing.T) {
		cover(t, "E1", "rpc-method", "LedgerApi.PublishRawTransaction", "LedgerApi.GetFrontierAccountBlock",
			"LedgerApi.GetAccountBlockByHash", "LedgerApi.GetUnreceivedBlocksByAddress", "LedgerApi.GetAccountInfoByAddress",
			"LedgerApi.GetFrontierMomentum", "PlasmaApi.GetRequiredPoWForAccountBlock")
		aZnn, aQsr := bal(t, a.Address, znn), bal(t, a.Address, qsr)

		// A published block passes the whole account-block pipeline
		// (getContext, every rule in all(), then the transaction-level
		// verifyTransaction checks) on every node that admits it.
		cover(t, "E1", "verifier-rule", "account_block.all", "account_block.getContext", "account_block.verifyTransaction")
		sendZnn = transfer(t, a, b.Address, znn, coins(1), nil)
		sendQsr := transfer(t, a, b.Address, qsr, coins(2), nil)
		receiveAll(t, b, 2)

		wantBig(t, "receiver ZNN", bal(t, b.Address, znn), coins(1))
		wantBig(t, "receiver QSR", bal(t, b.Address, qsr), coins(2))
		wantBig(t, "sender ZNN", bal(t, a.Address, znn), new(big.Int).Sub(aZnn, coins(1)))
		wantBig(t, "sender QSR", bal(t, a.Address, qsr), new(big.Int).Sub(aQsr, coins(2)))

		var frontier *api.AccountBlock
		get(t, &frontier, "ledger.getFrontierAccountBlock", b.Address)
		if frontier == nil || frontier.Height != 2 {
			t.Errorf("receiver account height: got %+v, want 2", frontier)
		}
		for _, h := range []types.Hash{sendZnn.Hash, sendQsr.Hash, frontier.Hash} {
			if err := net.Agreement(h, wait); err != nil {
				t.Error(err)
			}
		}
	})

	t.Run("B-2a_fused_plasma_only", func(t *testing.T) {
		got, err := net.Block(0, sendZnn.Hash)
		if err != nil || got == nil {
			t.Fatalf("read back: %v", err)
		}
		if got.Difficulty != 0 || got.FusedPlasma == 0 || got.TotalPlasma < got.BasePlasma {
			t.Errorf("fused=%d difficulty=%d base=%d used=%d, want fused only and used >= base", got.FusedPlasma, got.Difficulty, got.BasePlasma, got.TotalPlasma)
		}
	})

	t.Run("B-2b_proof_of_work_only", func(t *testing.T) {
		var got *api.AccountBlock
		get(t, &got, "ledger.getFrontierAccountBlock", b.Address)
		if got.Difficulty == 0 || got.FusedPlasma != 0 || got.TotalPlasma < got.BasePlasma {
			t.Errorf("fused=%d difficulty=%d base=%d used=%d, want proof of work only and used >= base", got.FusedPlasma, got.Difficulty, got.BasePlasma, got.TotalPlasma)
		}
	})

	t.Run("B-2c_no_plasma_refused", func(t *testing.T) {
		cover(t, "E2", "error", "vm/constants.ErrNotEnoughTotalPlasma")
		err := tryTransfer(a, b.Address, znn, coins(1), &Mutator{Plasma: func(b *nom.AccountBlock) { b.FusedPlasma, b.Difficulty = 0, 0 }})
		mustReject(t, err, constants.ErrNotEnoughTotalPlasma.Error())
	})

	t.Run("B-2d_fused_plasma_the_account_does_not_have_refused", func(t *testing.T) {
		cover(t, "E5", "error", "vm/constants.ErrNotEnoughPlasma")
		err := tryTransfer(b, a.Address, znn, units(1), &Mutator{Plasma: func(b *nom.AccountBlock) { b.FusedPlasma, b.Difficulty = constants.AccountBlockBasePlasma, 0 }})
		mustReject(t, err, constants.ErrNotEnoughPlasma.Error())
	})

	t.Run("B-2e_difficulty_without_work_refused", func(t *testing.T) {
		cover(t, "E6", "error", "verifier.ErrABPoWInvalid")
		cover(t, "E6", "verifier-rule", "account_block.pow")
		err := tryTransfer(b, a.Address, znn, units(1), &Mutator{SkipPoW: true,
			Plasma: func(b *nom.AccountBlock) {
				b.FusedPlasma, b.Difficulty = 0, constants.AccountBlockBasePlasma*constants.PoWDifficultyPerPlasma
			}})
		mustReject(t, err, "nonce/difficulty is invalid")
	})

	t.Run("B-3a_data_at_the_limit_accepted", func(t *testing.T) {
		blk := transfer(t, a, b.Address, znn, units(1), make([]byte, constants.MaxDataLength))
		got, err := net.WaitConfirmed(blk.Hash, wait)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Data) != constants.MaxDataLength {
			t.Errorf("data length read back: got %d, want %d", len(got.Data), constants.MaxDataLength)
		}
	})

	t.Run("B-3b_data_one_byte_over_the_limit_refused", func(t *testing.T) {
		cover(t, "E2", "error", "verifier.ErrABDataTooBig")
		_, err := net.Send(a, &nom.AccountBlock{BlockType: nom.BlockTypeUserSend, ToAddress: b.Address, TokenStandard: znn, Amount: units(1),
			Data: make([]byte, constants.MaxDataLength+1)},
			&Mutator{Plasma: func(b *nom.AccountBlock) { b.FusedPlasma = 2_000_000 }})
		mustReject(t, err, "data field is too big")
	})

	t.Run("B-4_receive_by_the_wrong_account_refused", func(t *testing.T) {
		cover(t, "E3", "error", "verifier.ErrABFromBlockReceiverMismatch")
		cover(t, "E3", "verifier-rule", "account_block.fromHash")
		blk := transfer(t, a, b.Address, znn, units(5), nil)
		if _, err := net.WaitConfirmed(blk.Hash, wait); err != nil {
			t.Fatal(err)
		}
		_, err := net.Receive(a, blk.Hash, nil)
		mustReject(t, err, "receiver mismatch")
		receiveAll(t, b, 1) // B-3a's send is still pending as well; this clears the mailbox
	})

	t.Run("B-5a_double_receive_refused", func(t *testing.T) {
		cover(t, "E4", "error", "verifier.ErrABFromBlockAlreadyReceived")
		cover(t, "E4", "verifier-rule", "account_block.fromHash")
		_, err := net.Receive(b, sendZnn.Hash, nil)
		mustReject(t, err, "already received")
	})

	t.Run("B-5b_receive_of_an_unknown_send_refused", func(t *testing.T) {
		cover(t, "E4", "error", "verifier.ErrABFromBlockMissing")
		cover(t, "E4", "verifier-rule", "account_block.fromHash")
		_, err := net.Receive(b, types.NewHash([]byte("no such block "+runTag)), nil)
		mustReject(t, err, "from-block doesn't exist")
	})

	t.Run("B-5c_wrong_previous_hash_refused", func(t *testing.T) {
		err := tryTransfer(a, b.Address, znn, units(1), &Mutator{Fields: func(b *nom.AccountBlock) { b.PreviousHash = types.NewHash([]byte("wrong")) }})
		mustReject(t, err, "previous block is missing")
		cover(t, "E6", "error", "verifier.ErrABPreviousMissing")
		cover(t, "E6", "verifier-rule", "account_block.previous")
	})

	t.Run("B-5d_wrong_height_refused", func(t *testing.T) {
		cover(t, "E2", "error", "verifier.ErrABPrevHasCementedOnTop", "verifier.ErrABPreviousMissing")
		for delta, want := range map[int64]string{-1: "cemented block on top of it", 1: "previous block is missing", 1000: "previous block is missing"} {
			err := tryTransfer(a, b.Address, znn, units(1), &Mutator{Fields: func(b *nom.AccountBlock) { b.Height = uint64(int64(b.Height) + delta) }})
			mustReject(t, err, want)
		}
		err := tryTransfer(a, b.Address, znn, units(1), &Mutator{Fields: func(b *nom.AccountBlock) { b.Height = 0 }})
		mustReject(t, err, "height must be higher than 0")
		cover(t, "E2", "error", "verifier.ErrABMHeightMissing")
		cover(t, "E2", "verifier-rule", "account_block.previous")
	})

	t.Run("B-5e_bad_signature_refused", func(t *testing.T) {
		cover(t, "E6", "error", "verifier.ErrABSignatureInvalid", "verifier.ErrABSignatureMissing", "verifier.ErrABPublicKeyWrongAddress", "verifier.ErrABPublicKeyMissing")
		cover(t, "E6", "verifier-rule", "account_block.signature", "account_block.producer")
		mustReject(t, tryTransfer(a, b.Address, znn, units(1), &Mutator{Signed: func(b *nom.AccountBlock) { b.Signature[0] ^= 0xff }}), "signature is invalid")
		mustReject(t, tryTransfer(a, b.Address, znn, units(1), &Mutator{Signed: func(b *nom.AccountBlock) { b.Signature = nil }}), "signature is missing")
		mustReject(t, tryTransfer(a, b.Address, znn, units(1), &Mutator{Signed: func(blk *nom.AccountBlock) {
			blk.PublicKey = b.Public
			blk.Signature = b.Sign(blk.Hash.Bytes())
		}}), "publicKey doesn't correspond to the address")
		mustReject(t, tryTransfer(a, b.Address, znn, units(1), &Mutator{Signed: func(b *nom.AccountBlock) { b.PublicKey = nil }}), "publicKey is missing")
	})

	t.Run("B-5f_wrong_chain_identifier_refused", func(t *testing.T) {
		cover(t, "E6", "error", "verifier.ErrABChainIdentifierMissing")
		cover(t, "E6", "verifier-rule", "account_block.chainIdentifier")
		mustReject(t, tryTransfer(a, b.Address, znn, units(1), &Mutator{Fields: func(b *nom.AccountBlock) { b.ChainIdentifier = net.ChainID + 1 }}), "different network Id")
		mustReject(t, tryTransfer(a, b.Address, znn, units(1), &Mutator{Fields: func(b *nom.AccountBlock) { b.ChainIdentifier = 0 }}), "chain-identifier is missing")
	})

	t.Run("B-5g_amount_above_balance_refused", func(t *testing.T) {
		cover(t, "E5", "error", "vm/constants.ErrInsufficientBalance")
		over := new(big.Int).Add(bal(t, a.Address, znn), units(1))
		mustReject(t, tryTransfer(a, b.Address, znn, over, &Mutator{}), constants.ErrInsufficientBalance.Error())
	})

	t.Run("B-5h_unknown_token_refused", func(t *testing.T) {
		zts := types.NewZenonTokenStandard([]byte("no such token " + runTag))
		mustReject(t, tryTransfer(a, b.Address, zts, units(1), &Mutator{}), constants.ErrDataNonExistent.Error())
	})

	t.Run("B-5i_hash_not_matching_the_fields_refused", func(t *testing.T) {
		cover(t, "E6", "error", "verifier.ErrABHashInvalid", "verifier.ErrABHashMissing")
		cover(t, "E6", "verifier-rule", "account_block.hash")
		mustReject(t, tryTransfer(a, b.Address, znn, units(1), &Mutator{Signed: func(b *nom.AccountBlock) { b.Amount = units(2) }}), "hash is different than the one computed")
		mustReject(t, tryTransfer(a, b.Address, znn, units(1), &Mutator{Signed: func(b *nom.AccountBlock) { b.Hash = types.ZeroHash }}), "hash must not be zero")
	})

	t.Run("B-5j_version_and_type_refused", func(t *testing.T) {
		cover(t, "E6", "error", "verifier.ErrABVersionMissing", "verifier.ErrABVersionInvalid", "verifier.ErrABTypeMissing", "verifier.ErrABTypeUnsupported")
		cover(t, "E6", "verifier-rule", "account_block.version", "account_block.blockType")
		mustReject(t, tryTransfer(a, b.Address, znn, units(1), &Mutator{Fields: func(b *nom.AccountBlock) { b.Version = 0 }}), "version is missing")
		mustReject(t, tryTransfer(a, b.Address, znn, units(1), &Mutator{Fields: func(b *nom.AccountBlock) { b.Version = 2 }}), "version is invalid")
		mustReject(t, tryTransfer(a, b.Address, znn, units(1), &Mutator{Plasma: func(b *nom.AccountBlock) { b.BlockType = 0; b.FusedPlasma = constants.AccountBlockBasePlasma }}), "type is missing")
		mustReject(t, tryTransfer(a, b.Address, znn, units(1), &Mutator{Plasma: func(b *nom.AccountBlock) { b.BlockType = 9; b.FusedPlasma = constants.AccountBlockBasePlasma }}), "type is not supported")
	})

	t.Run("B-5k_contract_block_types_from_a_user_refused", func(t *testing.T) {
		cover(t, "E3", "error", "verifier.ErrABTypeMustBeUser", "verifier.ErrABTypeMustNotBeGenesis")
		cover(t, "E3", "verifier-rule", "account_block.blockType")
		for typ, want := range map[uint64]string{nom.BlockTypeContractSend: "can't apply BlockTypeContractSend", nom.BlockTypeContractReceive: "not suitable for user-blocks"} {
			err := tryTransfer(a, b.Address, znn, units(1), &Mutator{Plasma: func(b *nom.AccountBlock) { b.BlockType = typ; b.FusedPlasma = constants.AccountBlockBasePlasma }})
			mustReject(t, err, want)
		}
		err := tryTransfer(a, b.Address, znn, units(1), &Mutator{Plasma: func(b *nom.AccountBlock) {
			b.BlockType = nom.BlockTypeGenesisReceive
			b.FusedPlasma = constants.AccountBlockBasePlasma
		}})
		mustReject(t, err, "must not be genesis")
	})

	t.Run("B-5l_receive_block_with_send_fields_refused", func(t *testing.T) {
		cover(t, "E6", "error", "verifier.ErrABAmountMustBeZero", "verifier.ErrABToAddressMustBeZero")
		cover(t, "E6", "verifier-rule", "account_block.amounts")
		blk := transfer(t, a, b.Address, znn, units(7), nil)
		if _, err := net.WaitConfirmed(blk.Hash, wait); err != nil {
			t.Fatal(err)
		}
		_, err := net.Receive(b, blk.Hash, &Mutator{Fields: func(b *nom.AccountBlock) { b.Amount = units(1); b.TokenStandard = znn }})
		mustReject(t, err, "amount must be zero")
		_, err = net.Receive(b, blk.Hash, &Mutator{Fields: func(blk *nom.AccountBlock) { blk.ToAddress = a.Address }})
		mustReject(t, err, "to-address must be zero")
		receiveAll(t, b, 1)
	})

	t.Run("B-5m_momentum_acknowledged_refused", func(t *testing.T) {
		cover(t, "E6", "error", "verifier.ErrABMAMustNotBeZero", "verifier.ErrABMAMissing")
		cover(t, "E6", "verifier-rule", "account_block.momentumAcknowledged")
		mustReject(t, tryTransfer(a, b.Address, znn, units(1), &Mutator{Fields: func(b *nom.AccountBlock) { b.MomentumAcknowledged = types.HashHeight{} }}), "momentum-acknowledged missing")
		mustReject(t, tryTransfer(a, b.Address, znn, units(1), &Mutator{Fields: func(b *nom.AccountBlock) { b.MomentumAcknowledged.Hash = types.NewHash([]byte("x")) }}), "momentum-acknowledged")
	})

	t.Run("B-9_refused_blocks_left_no_trace", func(t *testing.T) {
		// Every refusal above must leave both chains usable and unchanged.
		blk := transfer(t, a, b.Address, znn, units(3), nil)
		receiveAll(t, b, 1)
		if err := net.Agreement(blk.Hash, wait); err != nil {
			t.Error(err)
		}
		var list *api.AccountBlockList
		get(t, &list, "ledger.getUnconfirmedBlocksByAddress", a.Address, 0, 10)
		if list == nil || list.Count != 0 {
			t.Errorf("sender still has unconfirmed blocks: %+v", list)
		}
	})
}
