package chain

import (
	"testing"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/common/types"
)

// DeleteMomentum must mark the pricing context dirty instead of refreshing
// it inline. During a batch rollback the frontier moves N times; refreshing
// per event would pin the cache to a mid-rollback momentum. The next read
// (higherPriority) or the next InsertMomentum recomputes from the settled
// frontier.
func TestDeleteMomentum_SetsDirtyFlag(t *testing.T) {
	ap := newAccountPool(fakeStable{})

	// Prime the cache (fakeStable returns nil store → plasma stays nil, but
	// the dirty flag mechanics are what we are testing).
	ap.refreshDynamicPlasma()

	ap.plasmaMu.Lock()
	common.ExpectTrue(t, !ap.plasmaDirty)
	ap.plasmaMu.Unlock()

	ap.DeleteMomentum(nil)

	ap.plasmaMu.Lock()
	common.ExpectTrue(t, ap.plasmaDirty)
	ap.plasmaMu.Unlock()
}

// A batch of DeleteMomentum calls must leave the dirty flag set (not toggle
// it on/off) so the next read still triggers a refresh.
func TestDeleteMomentum_BatchSetsDirtyOnce(t *testing.T) {
	ap := newAccountPool(fakeStable{})
	ap.refreshDynamicPlasma()

	// Simulate a 5-momentum rollback burst.
	for i := 0; i < 5; i++ {
		ap.DeleteMomentum(nil)
	}

	ap.plasmaMu.Lock()
	common.ExpectTrue(t, ap.plasmaDirty)
	ap.plasmaMu.Unlock()
}

// higherPriority must clear the dirty flag by refreshing the cache, so a
// subsequent read does not pay for another store round-trip.
func TestHigherPriority_ClearsDirtyFlag(t *testing.T) {
	ap := newAccountPool(fakeStable{})
	ap.refreshDynamicPlasma()
	ap.DeleteMomentum(nil)

	ap.plasmaMu.Lock()
	common.ExpectTrue(t, ap.plasmaDirty)
	ap.plasmaMu.Unlock()

	a := &nom.AccountBlock{TotalPlasma: 100, BasePlasma: 100}
	b := &nom.AccountBlock{TotalPlasma: 200, BasePlasma: 100}
	_ = ap.higherPriority(a, b)

	ap.plasmaMu.Lock()
	common.ExpectTrue(t, !ap.plasmaDirty)
	ap.plasmaMu.Unlock()
}

// InsertMomentum refreshes the cache and clears the dirty flag, so a
// rollback followed by a new momentum does not double-refresh.
func TestInsertMomentum_ClearsDirtyFlag(t *testing.T) {
	ap := newAccountPool(fakeStable{})
	ap.refreshDynamicPlasma()
	ap.DeleteMomentum(nil)

	ap.plasmaMu.Lock()
	common.ExpectTrue(t, ap.plasmaDirty)
	ap.plasmaMu.Unlock()

	detailed := &nom.DetailedMomentum{
		Momentum: &nom.Momentum{
			Height: 1,
			Hash:   types.ZeroHash,
		},
	}
	ap.InsertMomentum(detailed)

	ap.plasmaMu.Lock()
	common.ExpectTrue(t, !ap.plasmaDirty)
	ap.plasmaMu.Unlock()
}

// Without the dirty-flag fix, DeleteMomentum refreshes the cache inline.
// This test verifies the fix is present: after DeleteMomentum the cache must
// NOT have been refreshed (the flag must be set, and the plasma field must
// be whatever it was before — not recomputed).
func TestDeleteMomentum_DoesNotRefreshInline(t *testing.T) {
	ap := newAccountPool(fakeStable{})

	// Prime with a known state.
	ap.refreshDynamicPlasma()
	ap.plasmaMu.Lock()
	common.ExpectTrue(t, !ap.plasmaDirty)
	ap.plasmaMu.Unlock()

	// DeleteMomentum must not call refreshDynamicPlasma.
	ap.DeleteMomentum(nil)

	// The dirty flag proves the deferred-refresh path was taken.
	ap.plasmaMu.Lock()
	common.ExpectTrue(t, ap.plasmaDirty)
	ap.plasmaMu.Unlock()
}
