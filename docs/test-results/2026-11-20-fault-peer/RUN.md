# N-5 fault peer — first end-to-end run (2026-11-20)

Variant `all-off` (no sporks in force → the legacy devp2p backend, which is
the backend this liar poisons). Five-node devnet, image
`go-zenon-devnet:latest` and fault image `go-zenon-devnet-fault:latest`,
both built from `c5bba33` (`FAULT=1 test/devnet/build-image.sh`, the
overlay documented in `test/devnet/fault/README.md`). Driver:
`E2E_RPC=http://localhost:35991 E2E_FAULT=1 go test -tags e2e ./test/e2e/ -run TestFaultPeerDrip -v -count=1`.

## Result

PASS in 480.8 s.

- baseline production: 6.00 momentums/min
- production with the fault peer connected: 6.00 momentums/min
- fault peer sync state at the end: height 195, target 195, network
  frontier 195 — the liar genuinely joined, caught up and stayed at the
  frontier (asserted in the case; without it the rate assertion is
  vacuous, see below)
- pillar-side evidence: 96 `peer connected ... devnet-fault` lines in
  the pillar's `zenon.log` during the run (`pillar-fault-connections.log`
  in this directory) — the liar's sessions repeatedly end and it redials,
  the phase-29 drip shape

Coverage unchanged (198/501): this is a capability harness, it records no
inventory token.

## The vacuous-run trap found on the way (fixed in the case)

First run PASSED with the liar never on the network. Cause: the devnet
image bakes the stock `docker/devnet/genesis.json`, the entrypoint only
seeds a data dir that has no `config.json` yet, and the case started the
liar with a plain `docker run` — no variant genesis mount. On the
`all-off` devnet the liar inited its own stock genesis block and every
pillar refused it at the eth/61 handshake:

    msg="peer connected" ... name="... devnet-fault"
    msg="handshake failed" ...
    Protocol eth/61 error: Genesis block mismatch - 653062... (!= 333465...)

It sat at height 1 (`stats.syncInfo` state 3, target 0) while the rate
assertion measured the honest network twice. The case now copies the
genesis out of a running devnet container
(`docker cp <net>-pillar:/root/.znn/genesis.json`) and bind-mounts it
over `/devnet/genesis.json`, and asserts the liar's syncer reached the
network frontier — a run where the liar stays at genesis now FAILS.

## Notes

- Run against a mainnet-znnd port remap (`compose-ports.yml` in the lab
  workspace): rpc 36997/36998, driver on the pillar's 35991.
- `go test` caches a SKIP result: always `-count=1` when iterating on a
  gated case.
- The drip never fired here in earnest: at the frontier no node requests
  hashes or blocks, so the liar serves nothing and the rates are
  identical. The phase-29 rehearsal (zenon-resilience-lab) already
  documented that this liar's sessions die fast on pre- and post-fix
  binaries — a fix discriminator needs a downloading victim positioned
  behind the liar (his syncnode role is the natural one) and is left for
  the libp2p fault-peer work.
