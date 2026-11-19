#!/bin/sh
# Print the Go build overlay that turns a source tree into the fault-peer
# build. Usage: overlay.sh <source-root> [root-as-the-build-sees-it]
#
# The overlay replaces protocol/handler.go with handler.go.in: the same file
# plus three ZNN_LIAR-gated misbehaviours copied from the phase-29 lab probe
# (zenon-resilience-lab, liar-patch-v6). Nothing is compiled in unless the
# overlay is passed explicitly (go build -overlay / docker build argument),
# and the misbehaviour only activates when the ZNN_LIAR env var is set on the
# running container, so stock images and stock nodes are unaffected.
#
# Scope: this is the LEGACY devp2p handler. It only affects peers talking the
# legacy backend; on variants where the libp2p backend is active (staged/fast
# past the libp2p spork height) a libp2p fault peer is still to be written
# (docs/TEST-PLAN.md section 8). See README.md in this directory.
set -eu
ROOT="$1"; SEEN="${2:-$1}"
cat <<JSON
{"Replace": {
  "$SEEN/protocol/handler.go": "$SEEN/test/devnet/fault/handler.go.in"
}}
JSON
