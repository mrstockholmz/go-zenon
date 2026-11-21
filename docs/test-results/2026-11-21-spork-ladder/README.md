# Legacy spork ladder enforcement — 2026-11-21

Case: `test/e2e/sporkladder_test.go` (`TestSporkLadderEnforcement`), branch
`test/spork-cases` on top of `testing/framework` (`abe1fa6`). Environment:
linux/amd64, the five-node docker devnet on a new `ladder` genesis variant
(`genesis.py`): AcceleratorSpork at 40, BridgeAndLiquiditySpork at 55,
HtlcSpork at 70 — the three legacy rungs that every other variant enforces
from height 0 and that therefore had no before/at/after anywhere — with
libp2p at 20, Dynamic Plasma at 30 (the stock switch-over) and PTLC,
EmbeddedRefund and EpochRewardTerms from genesis. Driver and node both
built from stock constants (node image at `c5bba33`, whose node code is
identical to `abe1fa6`; the branch's commits touch test/ and docs/ only).
All five node RPCs published on host ports through a local overlay (the
stock compose collides with a mainnet node on 35997, see results
2026-11-18 finding D1, and 3699x is the lab's L6 sync node since
2026-11-20).

Covers TEST-PLAN.md S-1 and S-2 (edge class E7) for the three ladder
rungs. The run is positioned before the first ladder height
(`E2E_MIN_HEIGHT=8`; driver start height 18), so every boundary is
observed live. The gate is the method-table ladder of vm/embedded
(`sporkVariantKey`): the origin set knows Accelerator with Donate only and
Liquidity with Donate and Update; the accelerator rung adds CreateProject,
the bridge rung adds the Bridge contract and the Liquidity stake methods,
the htlc rung adds the HTLC contract.

## Results

| Subtest | Result | Time |
|---|---|---|
| S-1_ladder_gated_methods_refused_before_enforcement | PASS | 0.03 s |
| S-2_accelerator_at_and_after_enforcement | PASS | 310.2 s |
| S-2_bridge_and_liquidity_at_enforcement | PASS | 129.5 s |
| S-2_htlc_at_and_after_enforcement | PASS | 169.7 s |

Main run 649.7 s (767 s with the build); driver start height 18.

Logs: `e2e-run-ladder.log` (main run). A first run (same four subtests,
all PASS, 566 s) wrote its coverage file to a relative path and failed at
`TestMain` exit ("no such file or directory": `go test` runs the driver
with the package directory as cwd, so `E2E_COVERAGE` must be absolute);
its log is kept as `e2e-run-ladder-first.log`. Coverage:
`coverage-ladder.tsv`; tokens merged into `test/inventory/coverage.tsv`
(spork items 5/8 → 8/8).

## What each subtest asserts

- S-1, below height 40 (frontier 18): `accelerator.CreateProject` is
  refused at publish with "method not found in the abi" (the origin
  accelerator table has Donate only), `liquidity.LiquidityStake` the same
  (origin liquidity table has Donate and Update only), and `htlc.Create`
  with "contract doesn't exist" (the origin set has no HTLC contract). No
  container has ever restarted.
- S-2 accelerator at 40: all five nodes share one momentum hash at 39, 40
  and 41; a block with its acknowledged momentum pinned to the momentum
  at exactly 40 is judged by the new table — the project is created; a
  normal call afterwards creates another.
- S-2 bridge at 55: same agreement check; the pinned `LiquidityStake`
  call is admitted and reaches the contract, where it fails only because
  this genesis configures no liquidity token tuple (constants.ErrInvalid
  Token, in the node log). Refused-at-publish would mean the rung is
  still off — the discriminating signal.
- S-2 htlc at 70: same agreement check; the pinned `htlc.Create` locks
  the sent ZNN, and a second lock with a fresh expiry afterwards works
  the same.

## Why no "acknowledge H-1 after H" probe

The acknowledgement-recency rule (#280, case S-7, in force here because
Dynamic Plasma is) forbids a block from acknowledging a momentum from
before the newest enforced spork, so a post-height block pinned to H-1 is
refused by that rule, not by the ladder — it would not be evidence about
the ladder. The S-1 phase below the height is the pre-height evidence;
the pinned "at" call is the at-height evidence.

## Reproduce

```
python3 test/devnet/genesis.py --list     # ladder: az@40, bridge-liq@55, htlc@70
test/devnet/up.sh ladder                  # plus a local port overlay if 35997 is taken
E2E_RPC=http://localhost:<rpc-port> E2E_CHAIN_ID=69 E2E_MIN_HEIGHT=8 \
E2E_ALL_RPC=<five node RPC urls> \
E2E_COVERAGE=<absolute path>/coverage-ladder.tsv \
go test -tags e2e ./test/e2e/ -run TestSporkLadderEnforcement -v
test/devnet/down.sh
```
