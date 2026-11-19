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

or a fault image by passing the overlay to the docker build the same way
`FAST=1` does it in `test/devnet/build-image.sh` (image name
`go-zenon-devnet-fault:latest`).

## Scope and what is still missing

- **Legacy backend only.** The overlay patches `protocol/handler.go`, the
  legacy devp2p handler. On variants where the libp2p spork is in force
  (staged/fast/all-on past its height) the liar is inert; a libp2p fault
  peer is still to be written. `TestFaultPeerDrip` skips there.
- **No fault role yet.** The devnet image configures roles from
  `docker/devnet/<role>`; a `fault` role (config with the pillars as
  bootstrap peers) is part of the remaining work, as is wiring
  `FAULT=1 test/devnet/build-image.sh`.
- **Never on any real network.** The fault binary must never join mainnet,
  testnet, or any network with value.

The driver case `TestFaultPeerDrip` in `test/e2e/` (gated on `E2E_FAULT=1`)
measures production rate with and without the liar connected; it is a
capability harness for N-5, not yet a fix discriminator (the phase-29
rehearsal showed this liar's sessions end fast on both pre- and post-fix
binaries).
