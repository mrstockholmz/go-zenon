# Fault peer (TEST-PLAN.md section 8, starting point)

A fault peer speaks the wire protocol and misbehaves on request. This
directory ports the phase-29 lab probe (zenon-resilience-lab,
`results/phase-29-pr-checks/liar-patch-v6.diff`) into the framework as an
overlay build, the same pattern as the fast test constants
(`test/devnet/fast/`): the source tree is never edited, nothing extra is
compiled unless the overlay is passed explicitly, and the misbehaviour only
activates when the `ZNN_LIAR` env var is set on the container.

## What the overlay adds

`handler.go.in` is `protocol/handler.go` plus three `ZNN_LIAR`-gated blocks:

| `ZNN_LIAR` | Behaviour |
|---|---|
| `1` | `GetBlocksMsg` is swallowed without a reply: a download session stalls with a request in flight and imports nothing |
| `2` | the drip modes: `GetBlockHashesMsg` is answered with one fixed unknown hash; `GetBlockHashesFromNumberMsg` is answered with fresh unknown hashes derived from the requested height (delayed cross-check replies), then empty replies — the requester's session walks ancestor searches without ever importing |

The shapes are the ones behind issue #291 / the fix in #306 (session that
imports nothing) — see the phase-29 report for what the drip liar does and
does not reproduce.

## Build

```sh
test/devnet/fault/overlay.sh "$PWD" > build/fault-overlay.json
go build -overlay build/fault-overlay.json -o build/linux-fault/znnd ./cmd/znnd
```

or the image built by `FAULT=1 test/devnet/build-image.sh` (image name
`go-zenon-devnet-fault:latest`).

## Scope and what is still missing

- **The liar needs the devnet's genesis.** The image bakes the stock
  `docker/devnet/genesis.json`, and the entrypoint only copies
  `/devnet/genesis.json` into a data dir that has no `config.json`
  yet. A fault container started with a plain `docker run` (as the
  driver case does) therefore inits the stock genesis block; on any
  non-stock variant every pillar refuses it at the eth/61 handshake
  with `Genesis block mismatch` and the liar syncs nothing — a
  silently vacuous run (observed 2026-11-20: the rate assertion
  passed while the liar sat at height 1). The case copies the genesis
  out of a running devnet container and bind-mounts it over
  `/devnet/genesis.json`; keep that in any other fault-container
  tooling.
- **Legacy backend only.** The overlay patches `protocol/handler.go`, the
  legacy devp2p handler. On variants where the libp2p spork is in force
  (staged/fast/all-on past its height) the liar is inert; a libp2p fault
  peer is still to be written. `TestFaultPeerDrip` skips there.
- **Fault role.** The devnet image configures roles from
  `docker/devnet/<role>`; the fault role ships in this branch
  (`docker/devnet/fault`, an observer-style config that dials all four
  pillars; znnd generates a fresh network key on first boot, so the liar
  never reuses another node's identity).
- **Never on any real network.** The fault binary must never join mainnet,
  testnet, or any network with value.

The driver case `TestFaultPeerDrip` in `test/e2e/` (gated on `E2E_FAULT=1`)
measures production rate with and without the liar connected and asserts
production continues (the post-#306 expectation); it skips when the
libp2p spork is live. It is a capability harness for N-5, not yet a fix
discriminator (the phase-29 rehearsal showed this liar's sessions end
fast on both pre- and post-fix binaries).

**Run end-to-end 2026-11-20** (evidence `docs/test-results/2026-11-20-fault-peer/`):
PASS in 481 s on `all-off`, baseline 6.00 momentums/min, with the liar 6.00,
liar syncer asserted at the network frontier (195/195), 96 pillar-side
`peer connected ... devnet-fault` lines. Two gotchas cost a run each and
are now handled in the case: the liar needs the devnet's genesis mounted
(see above — otherwise the run is silently vacuous), and `go test` caches
a SKIP (use `-count=1`). The drip itself stays inert at the frontier (no
node requests anything); the fix-discriminating shape (a downloading
victim behind the liar) is part of the remaining fault-peer work.
