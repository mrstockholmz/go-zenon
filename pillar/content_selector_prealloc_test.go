package pillar

import (
	"runtime"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/dp"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
)

// TestContent_PreallocCappedAtInputSize verifies that the contract-batch
// preallocation in filterBlocksToCommit is bounded by the number of input
// blocks, not by MaxContractBlocksInMomentum. Before the fix, a config at
// the accepted upper limit (10^10 base blocks / 52,500 per contract block
// ≈ 4×10^9 slots ≈ 32 GB of pointers) caused an enormous allocation even
// with zero pending blocks (issue #164).
func TestContent_PreallocCappedAtInputSize(t *testing.T) {
	// MaxBasePlasmaInMomentum at the accepted upper limit.
	config := &definition.PlasmaVariables{
		MaxBasePlasmaInMomentum: definition.MaxBasePlasmaInMomentumUpperLimit,
	}
	previousMomentum := &nom.Momentum{NextFusionPrice: 1000, NextWorkPrice: 1000, Version: 2}
	cs := &contentSelector{
		plasma: dp.NewDynamicPlasma(previousMomentum, config),
	}

	// Measure allocations during the call with a small input.
	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	done := make(chan struct{})
	go func() {
		toCommit := cs.filterBlocksToCommit([]*nom.AccountBlock{
			{Height: 1, BlockType: nom.BlockTypeUserSend, BasePlasma: 21000, FusedPlasma: 21000},
			{Height: 2, BlockType: nom.BlockTypeUserSend, BasePlasma: 21000, FusedPlasma: 21000},
		})
		common.Expect(t, len(toCommit), 2)
		close(done)
	}()

	select {
	case <-done:
		// completed
	case <-time.After(5 * time.Second):
		t.Fatal("filterBlocksToCommit timed out — likely allocating the full 32 GB preallocation")
	}

	runtime.ReadMemStats(&memAfter)
	alloc := memAfter.TotalAlloc - memBefore.TotalAlloc
	// Generous bound: the call should allocate well under 1 MB for two blocks.
	// Without the fix it allocates ~32 GB (4 billion × 8-byte pointers).
	if alloc > 1<<20 {
		t.Fatalf("allocated %d bytes (> 1 MiB) for 2 blocks; preallocation not capped at input size", alloc)
	}
}

// TestContent_PreallocCappedAtInputSize_Empty verifies the empty-input case:
// no allocation proportional to MaxContractBlocksInMomentum.
func TestContent_PreallocCappedAtInputSize_Empty(t *testing.T) {
	config := &definition.PlasmaVariables{
		MaxBasePlasmaInMomentum: definition.MaxBasePlasmaInMomentumUpperLimit,
	}
	previousMomentum := &nom.Momentum{NextFusionPrice: 1000, NextWorkPrice: 1000, Version: 2}
	cs := &contentSelector{
		plasma: dp.NewDynamicPlasma(previousMomentum, config),
	}

	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	toCommit := cs.filterBlocksToCommit(nil)
	common.Expect(t, len(toCommit), 0)

	runtime.ReadMemStats(&memAfter)
	alloc := memAfter.TotalAlloc - memBefore.TotalAlloc
	if alloc > 1<<20 {
		t.Fatalf("allocated %d bytes (> 1 MiB) for empty input; preallocation not capped at input size", alloc)
	}
}

// TestContent_PreallocCappedAtInputSize_ContractBlocks verifies that
// contract blocks still work correctly with the capped preallocation.
func TestContent_PreallocCappedAtInputSize_ContractBlocks(t *testing.T) {
	config := &definition.PlasmaVariables{
		MaxBasePlasmaInMomentum: 21000 * 5,
	}
	previousMomentum := &nom.Momentum{NextFusionPrice: 1000, NextWorkPrice: 1000, Version: 2}
	cs := &contentSelector{
		plasma: dp.NewDynamicPlasma(previousMomentum, config),
	}

	toCommit := cs.filterBlocksToCommit([]*nom.AccountBlock{
		{Height: 1, BlockType: nom.BlockTypeContractSend, Address: types.PillarContract},
		{Height: 2, BlockType: nom.BlockTypeContractReceive, Address: types.PillarContract},
	})
	common.Expect(t, len(toCommit), 2)
}
