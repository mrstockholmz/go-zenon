package pillar

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/zenon-network/go-zenon/chain"
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/chain/store"
	"github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/wallet"
)

// mockChainForGenerate implements chain.Chain by embedding the interface
// and overriding only the methods generateNext calls.  The
// frontierAccountStore field lets each test inject the account store it
// needs without implementing the full AccountPool interface.
type mockChainForGenerate struct {
	chain.Chain
	acquireInsertCalls    int
	frontierAccountStore  store.Account
}

func (m *mockChainForGenerate) AcquireInsert(reason string) sync.Locker {
	m.acquireInsertCalls++
	mu := &sync.Mutex{}
	mu.Lock()
	return mu
}

func (m *mockChainForGenerate) GetFrontierAccountStore(address types.Address) store.Account {
	return m.frontierAccountStore
}

// mockAccountForGenerate implements store.Account by embedding the
// interface and overriding SequencerFront.
type mockAccountForGenerate struct {
	store.Account
	front *types.AccountHeader
	err   error
}

func (m *mockAccountForGenerate) SequencerFront(mailbox store.AccountMailbox) (*types.AccountHeader, error) {
	return m.front, m.err
}

// mockMomentumForGenerate implements store.Momentum by embedding the
// interface and overriding GetAccountMailbox and GetAccountBlock.
type mockMomentumForGenerate struct {
	store.Momentum
	mailbox  store.AccountMailbox
	block    *nom.AccountBlock
	blockErr error
}

func (m *mockMomentumForGenerate) GetAccountMailbox(address types.Address) store.AccountMailbox {
	return m.mailbox
}

func (m *mockMomentumForGenerate) GetAccountBlock(header types.AccountHeader) (*nom.AccountBlock, error) {
	return m.block, m.blockErr
}

// TestGenerateNextEmptyQueue verifies that generateNext returns
// ErrNothingToGenerate when the contract's sequencer queue is empty
// (SequencerFront returns nil), and that the insert lock is acquired
// and released.
func TestGenerateNextEmptyQueue(t *testing.T) {
	producer := types.ParseAddressPanic("z1qz8v73ea2vy2rrlq7skssngu8cm8mknjjkr2ju")

	// SequencerFront returns nil → empty queue.
	acct := &mockAccountForGenerate{front: nil, err: nil}
	mc := &mockChainForGenerate{frontierAccountStore: acct}
	w := &worker{
		log:      common.PillarLogger.New("submodule", "worker"),
		coinbase: &wallet.KeyPair{Address: producer},
		chain:    mc,
	}

	ms := &mockMomentumForGenerate{}

	tx, err := w.generateNext(ms, types.PillarContract)
	if !errors.Is(err, ErrNothingToGenerate) {
		t.Fatalf("expected ErrNothingToGenerate, got tx=%v err=%v", tx, err)
	}
	if tx != nil {
		t.Fatalf("expected nil transaction, got %v", tx)
	}
	if mc.acquireInsertCalls != 1 {
		t.Fatalf("expected 1 AcquireInsert call, got %d", mc.acquireInsertCalls)
	}
}

// TestGenerateNextSendBlockReadError verifies that when SequencerFront
// returns a valid header but the corresponding send-block read fails,
// the error propagates (wrapped) instead of panicking or returning nil.
func TestGenerateNextSendBlockReadError(t *testing.T) {
	producer := types.ParseAddressPanic("z1qz8v73ea2vy2rrlq7skssngu8cm8mknjjkr2ju")

	header := types.AccountHeader{
		Address: types.PillarContract,
		HashHeight: types.HashHeight{
			Hash:   types.Hash{0xab},
			Height: 42,
		},
	}
	acct := &mockAccountForGenerate{front: &header, err: nil}
	mc := &mockChainForGenerate{frontierAccountStore: acct}
	w := &worker{
		log:      common.PillarLogger.New("submodule", "worker"),
		coinbase: &wallet.KeyPair{Address: producer},
		chain:    mc,
	}

	forcedErr := errors.New("forced read failure")
	ms := &mockMomentumForGenerate{block: nil, blockErr: forcedErr}

	tx, err := w.generateNext(ms, types.PillarContract)
	if err == nil {
		t.Fatalf("expected error, got tx=%v", tx)
	}
	if tx != nil {
		t.Fatalf("expected nil transaction, got %v", tx)
	}
	if !strings.Contains(err.Error(), "failed to read send block") {
		t.Fatalf("expected send-block read diagnostic, got: %v", err)
	}
	if !errors.Is(err, forcedErr) {
		t.Fatalf("expected wrapped forced error, got: %v", err)
	}
}
