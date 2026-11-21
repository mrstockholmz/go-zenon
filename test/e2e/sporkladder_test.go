//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"github.com/zenon-network/go-zenon/wallet"
)

// TestSporkLadderEnforcement covers TEST-PLAN.md S-1 and S-2 on the ladder
// genesis (edge class E7) for the three legacy rungs that every other
// variant enforces from height 0 and that therefore have no before/at/after
// there: AcceleratorSpork (40), BridgeAndLiquiditySpork (55) and HtlcSpork
// (70).
//
// The gate is the method-table ladder of vm/embedded (sporkVariantKey): the
// origin set knows the Accelerator contract with Donate only and Liquidity
// with Donate and Update; the accelerator rung adds CreateProject, the
// bridge rung adds the Bridge contract and the Liquidity stake methods, the
// htlc rung adds the HTLC contract. Below its height a gated call is
// refused at publish ("method not found in the abi", or "contract doesn't
// exist" for a contract the origin set does not have); a block that
// acknowledges the enforcement height is judged by the new table.
//
//   - S-1: below the first ladder height every gated call is refused.
//   - S-2: at each height all five nodes agree on the momentum one below,
//     at and one above it, no container has ever exited, and a block
//     acknowledging exactly the height is processed under the new table:
//     CreateProject executes, LiquidityStake reaches the contract (it
//     fails there only because this genesis configures no liquidity token
//     tuple), Create locks the sent ZNN.
//
// The "one below acknowledged after the height" half of the boundary is
// not probed with a live block: the acknowledgement-recency rule (#280,
// S-7, in force here because Dynamic Plasma is) forbids acknowledging a
// momentum from before the newest enforced spork, so a post-height block
// pinned to H-1 is refused by that rule, not by the ladder. The S-1 phase
// below the height is the pre-height evidence.
//
// Run it positioned before the first ladder height:
//
//	E2E_MIN_HEIGHT=8 go test -tags e2e ./test/e2e/ -run TestSporkLadderEnforcement
//
// It reads the other nodes' RPC through the docker-published ports
// (E2E_ALL_RPC) and controls the containers through the docker CLI
// (E2E_DOCKER_NET names the container prefix), like
// TestSporkStagedEnforcement.
func TestSporkLadderEnforcement(t *testing.T) {
	if fast {
		t.Skip("the fast variant enforces every spork from height 0")
	}
	// The ladder genesis: the legacy rungs enforced on a running network,
	// the later sporks from genesis, the libp2p/dynamic-plasma switch-over
	// at the stock heights (test/devnet/genesis.py).
	want := map[string]uint64{"accelerator": 40, "bridge": 55, "htlc": 70}
	for name, h := range want {
		if sporkHeights[name] != h {
			t.Skipf("needs the ladder genesis: spork %q enforced at %d, this chain has %d", name, h, sporkHeights[name])
		}
	}
	for _, name := range []string{"ptlc", "embedded-refund", "epoch-reward-terms"} {
		if sporkHeights[name] != 0 {
			t.Skipf("needs the ladder genesis: spork %q enforced at %d, want 0", name, sporkHeights[name])
		}
	}
	if sporkHeights["libp2p"] != 20 || sporkHeights["dynamic-plasma"] != 30 {
		t.Skipf("needs the ladder genesis: libp2p/dynamic-plasma at 20/30, this chain has %d/%d",
			sporkHeights["libp2p"], sporkHeights["dynamic-plasma"])
	}

	allRPC := strings.Split(envOr("E2E_ALL_RPC",
		"http://localhost:35991,http://localhost:36992,http://localhost:36993,http://localhost:36997,http://localhost:36994"), ",")
	prefix := envOr("E2E_DOCKER_NET", "znnd-devnet")
	containers := []string{prefix + "-pillar", prefix + "-pillar2", prefix + "-pillar3", prefix + "-rpc", prefix + "-observer"}

	docker := func(args ...string) (string, error) {
		out, err := exec.Command("docker", args...).CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
	// noExitYet fails when a container has ever restarted: across an
	// enforcement height a node that exited would show a non-zero restart
	// count here.
	noExitYet := func(what string) {
		t.Helper()
		args := append([]string{"inspect", "-f", "{{.Name}} {{.RestartCount}}"}, containers...)
		out, err := docker(args...)
		if err != nil {
			t.Fatalf("docker inspect: %v (%s)", err, out)
		}
		for _, line := range strings.Split(out, "\n") {
			var name string
			var restarts int
			if _, err := fmt.Sscanf(line, "/%s %d", &name, &restarts); err != nil || restarts != 0 {
				t.Fatalf("%s: container %q has restarted %d times (%q): a node exited", what, name, restarts, line)
			}
		}
	}

	// momentum is the momentum at exactly the given height from the
	// primary endpoint.
	momentum := func(height uint64) struct {
		Hash   types.Hash `json:"hash"`
		Height uint64     `json:"height"`
	} {
		t.Helper()
		var list struct {
			List []struct {
				Hash   types.Hash `json:"hash"`
				Height uint64     `json:"height"`
			} `json:"list"`
		}
		get(t, &list, "ledger.getMomentumsByHeight", height, 1)
		if len(list.List) != 1 || list.List[0].Height != height {
			t.Fatalf("momentum at height %d: got %+v, want exactly that height", height, list.List)
		}
		return list.List[0]
	}

	// agree polls until every node answers and returns the single momentum
	// hash they all share at the height; a node on a different branch (or
	// dead) fails the deadline. The jumps between ladder heights are
	// minutes of wall time, far beyond the block-confirmation `wait`.
	agree := func(height uint64) types.Hash {
		t.Helper()
		var ref types.Hash
		deadline := time.Now().Add(wait)
		for {
			all := true
			for i, url := range allRPC {
				body, _ := json.Marshal(map[string]interface{}{
					"jsonrpc": "2.0", "id": 1, "method": "ledger.getMomentumsByHeight",
					"params": []interface{}{height, 1},
				})
				resp, err := http.Post(url, "application/json", bytes.NewReader(body))
				if err != nil {
					all = false
					break
				}
				var m struct {
					Result struct {
						List []struct {
							Hash   types.Hash `json:"hash"`
							Height uint64     `json:"height"`
						} `json:"list"`
					} `json:"result"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&m); err != nil || len(m.Result.List) != 1 || m.Result.List[0].Height != height {
					resp.Body.Close()
					all = false
					break
				}
				resp.Body.Close()
				if i == 0 {
					ref = m.Result.List[0].Hash
				} else if m.Result.List[0].Hash != ref {
					t.Fatalf("node %s disagrees at height %d: %s vs %s", url, height, m.Result.List[0].Hash, ref)
				}
			}
			if all {
				return ref
			}
			if time.Now().After(deadline) {
				t.Fatalf("not all of %v answered height %d within %v", allRPC, height, wait)
			}
			time.Sleep(2 * time.Second)
		}
	}

	// boundary waits for the chain to pass the height and then checks that
	// all five nodes share one momentum hash at H-1, H and H+1.
	boundary := func(name string, H uint64) {
		t.Helper()
		if err := net.WaitHeight(H+1, 10*time.Minute); err != nil {
			t.Fatal(err)
		}
		for _, h := range []uint64{H - 1, H, H + 1} {
			got := agree(h)
			if canonical := momentum(h).Hash; got != canonical {
				t.Errorf("node hashes at %d: %s, primary says %s", h, got, canonical)
			}
		}
		noExitYet(fmt.Sprintf("across %s enforcement at %d", name, H))
	}

	// callAcking sends tmpl with its acknowledged momentum pinned to
	// exactly the momentum at the given height: the block is judged by the
	// rules in force at that height, whatever the frontier is by the time
	// it is admitted (the acknowledgement-recency rule allows it: every
	// spork enforced at the frontier is already enforced at or below the
	// pinned height, since the pin is an enforcement height of this
	// genesis). Without the pin a prepared block acknowledges the frontier,
	// which is not "at the height" any more once the wait has passed it.
	callAcking := func(kp *wallet.KeyPair, tmpl *nom.AccountBlock, height uint64) (*Result, error) {
		m := momentum(height)
		b, err := net.Send(kp, tmpl, &Mutator{Fields: func(b *nom.AccountBlock) {
			b.MomentumAcknowledged = types.HashHeight{Hash: m.Hash, Height: height}
		}})
		if err != nil {
			return nil, err
		}
		return net.WaitContract(b.Hash, wait)
	}

	a := newAccount(t, 40, 50, 5, 500)

	// The gated calls, one per rung of the ladder.
	project := func(name string) []byte {
		return definition.ABIAccelerator.PackMethodPanic(definition.CreateProjectMethodName,
			name, "driver project", "e2e.test", coins(10), coins(100))
	}
	azCall := func(name string) *nom.AccountBlock {
		return &nom.AccountBlock{BlockType: nom.BlockTypeUserSend, ToAddress: types.AcceleratorContract,
			Data: project(name), TokenStandard: znn, Amount: constants.ProjectCreationAmount}
	}
	liqCall := &nom.AccountBlock{BlockType: nom.BlockTypeUserSend, ToAddress: types.LiquidityContract,
		Data:          definition.ABILiquidity.PackMethodPanic(definition.LiquidityStakeMethodName, int64(constants.StakeTimeMinSec)),
		TokenStandard: znn, Amount: coins(1)}
	htlcCall := func() *nom.AccountBlock {
		return &nom.AccountBlock{BlockType: nom.BlockTypeUserSend, ToAddress: types.HtlcContract,
			Data: definition.ABIHtlc.PackMethodPanic(definition.CreateHtlcMethodName,
				a.Address, time.Now().Add(2*time.Hour).Unix(), definition.HashTypeSHA3, uint8(32), hashLock()),
			TokenStandard: znn, Amount: coins(1)}
	}

	t.Run("S-1_ladder_gated_methods_refused_before_enforcement", func(t *testing.T) {
		f, err := net.Frontier()
		if err != nil {
			t.Fatal(err)
		}
		if f.Height > 35 {
			t.Skipf("run positioned below height 36: frontier is %d (set E2E_MIN_HEIGHT=8)", f.Height)
		}
		cover(t, "E7", "spork", "AcceleratorSpork", "BridgeAndLiquiditySpork", "HtlcSpork")
		cover(t, "E7", "contract-method", "accelerator.CreateProject", "liquidity.LiquidityStake", "htlc.Create")
		// Below the accelerator rung the accelerator table has Donate
		// only: CreateProject is not in the ABI.
		callReject(t, constants.ErrContractMethodNotFound.Error(), a, types.AcceleratorContract, project("e2e ladder "+runTag), znn, constants.ProjectCreationAmount)
		// Below the bridge rung the liquidity table has Donate and Update
		// only: LiquidityStake is not in the ABI.
		callReject(t, constants.ErrContractMethodNotFound.Error(), a, types.LiquidityContract,
			definition.ABILiquidity.PackMethodPanic(definition.LiquidityStakeMethodName, int64(constants.StakeTimeMinSec)), znn, coins(1))
		// Below the htlc rung the origin set has no HTLC contract at all.
		callReject(t, constants.ErrContractDoesntExist.Error(), a, types.HtlcContract, htlcCall().Data, znn, coins(1))
		noExitYet("before any ladder enforcement")
	})

	t.Run("S-2_accelerator_at_and_after_enforcement", func(t *testing.T) {
		cover(t, "E7", "spork", "AcceleratorSpork")
		cover(t, "E7", "contract-method", "accelerator.CreateProject")
		H := want["accelerator"]
		boundary("accelerator", H)
		// At the height, judged by the new table: the project is created.
		res, err := callAcking(a, azCall("e2e ladder at "+runTag), H)
		mustOK(t, res, err)
		// After: a normal call acknowledging the frontier works the same.
		callOK(t, a, types.AcceleratorContract, project("e2e ladder after "+runTag), znn, constants.ProjectCreationAmount)
	})

	t.Run("S-2_bridge_and_liquidity_at_enforcement", func(t *testing.T) {
		cover(t, "E7", "spork", "BridgeAndLiquiditySpork")
		cover(t, "E7", "contract-method", "liquidity.LiquidityStake")
		H := want["bridge"]
		boundary("bridge", H)
		// At the height the call is admitted and reaches the contract; it
		// fails there only because this genesis configures no liquidity
		// token tuple (constants.ErrInvalidToken, visible in the node log).
		// Refused-at-publish would mean the rung is still off — the
		// discriminating signal of the ladder.
		res, err := callAcking(a, liqCall, H)
		mustFail(t, res, err)
	})

	t.Run("S-2_htlc_at_and_after_enforcement", func(t *testing.T) {
		cover(t, "E7", "spork", "HtlcSpork")
		cover(t, "E7", "contract-method", "htlc.Create")
		H := want["htlc"]
		boundary("htlc", H)
		// At the height, judged by the new table: the ZNN is locked.
		res, err := callAcking(a, htlcCall(), H)
		mustOK(t, res, err)
		// After: a second lock with a fresh expiry works the same.
		callOK(t, a, types.HtlcContract, htlcCall().Data, znn, coins(1))
	})
}

// hashLock returns a fresh 32-byte hash lock per call: entries are keyed by
// the creating send block, so reuse of the lock itself would be fine, but a
// distinct lock keeps the two creations independent reads.
func hashLock() []byte {
	lock := make([]byte, 32)
	for i := range lock {
		lock[i] = byte(time.Now().UnixNano() >> (i % 8 * 8))
	}
	return lock
}
