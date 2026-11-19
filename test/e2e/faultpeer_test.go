//go:build e2e

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestFaultPeerDrip is the starting point for TEST-PLAN.md N-5's fault
// peer: a peer that answers hash requests with unknown hashes and swallows
// block requests, so a download session against it imports nothing (the
// shape of issue #291, fixed by #306).
//
// It measures the network's momentum rate with and without the fault peer
// connected and asserts production continues while the peer drips — after
// #306 a session that imports nothing must not hold production past the
// stall window (#245, downloader no-progress deadline).
//
// Scope, stated honestly:
//   - The fault binary is the LEGACY devp2p liar (test/devnet/fault, an
//     overlay that adds ZNN_LIAR-gated behaviour to protocol/handler.go).
//     It only affects nodes on the legacy backend, so this case skips when
//     the libp2p spork is live (staged/fast/all-on variants past its
//     height); a libp2p fault peer is still to be written.
//   - The phase-29 lab rehearsal (zenon-resilience-lab) found this
//     particular liar does NOT reproduce the pre-fix hold even on a
//     pre-fix binary: its sessions end in seconds because the hash packs
//     are served and only blocks are withheld. So this case asserts the
//     post-fix expectation (production continues) and records both rates;
//     it is a capability harness, not a fix discriminator, until a liar
//     whose sessions stay open exists here.
//
// Run it on a stock-variant devnet with the fault image built:
//
//	FAULT=1 test/devnet/build-image.sh   # documented in test/devnet/fault/README.md
//	test/devnet/up.sh stock
//	E2E_FAULT=1 go test -tags e2e ./test/e2e/ -run TestFaultPeerDrip -v
//
// E2E_FAULT_IMAGE names the image (default go-zenon-devnet-fault:latest)
// and E2E_DOCKER_NET the container prefix of the running devnet.
func TestFaultPeerDrip(t *testing.T) {
	if os.Getenv("E2E_FAULT") != "1" {
		t.Skip("set E2E_FAULT=1; starts a liar container and needs the fault image")
	}
	if live["libp2p"] {
		t.Skip("the fault peer speaks the legacy devp2p protocol; the libp2p backend is live on this chain (a libp2p fault peer is still to be written)")
	}

	image := envOr("E2E_FAULT_IMAGE", "go-zenon-devnet-fault:latest")
	prefix := envOr("E2E_DOCKER_NET", "znnd-devnet")
	liar := prefix + "-fault"

	// The liar dials the pillars as bootstrap peers; inbound slots are free
	// on a five-node devnet. It joins the running devnet's compose network.
	if out, err := exec.Command("docker", "image", "inspect", image, "--format", "{{.Id}}").Output(); err != nil {
		t.Skipf("fault image %s not present: %v (%s)", image, err, strings.TrimSpace(string(out)))
	}
	netOut, err := exec.Command("docker", "inspect", prefix+"-pillar",
		"--format", "{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}").Output()
	if err != nil {
		t.Skipf("no running devnet container %s-pillar: %v", prefix, err)
	}
	dockerNet := strings.TrimSpace(string(netOut))

	rate := func(window time.Duration) float64 {
		start, err := net.Frontier()
		if err != nil {
			t.Fatalf("frontier: %v", err)
		}
		time.Sleep(window)
		end, err := net.Frontier()
		if err != nil {
			t.Fatalf("frontier: %v", err)
		}
		return float64(end.Height-start.Height) / window.Minutes()
	}

	base := rate(3 * time.Minute)
	t.Logf("baseline production: %.2f momentums/min", base)

	run := exec.Command("docker", "run", "-d", "--rm", "--name", liar,
		"--network", dockerNet,
		"-e", "ZNND_ROLE=fault",
		"-e", "ZNN_LIAR=2",
		image)
	if out, err := run.CombinedOutput(); err != nil {
		t.Fatalf("start fault peer: %v: %s", err, strings.TrimSpace(string(out)))
	}
	defer exec.Command("docker", "rm", "-f", liar).Run()

	withLiar := rate(5 * time.Minute)
	t.Logf("production with the fault peer: %.2f momentums/min (baseline %.2f)", withLiar, base)

	// After #306 the only allowed hold is the bounded stall window, so the
	// rate may sag but must not collapse. Half of baseline is a generous
	// floor that no honest flake on a five-node devnet should reach; the
	// recorded numbers, not the threshold, are the evidence.
	if withLiar < base/2 {
		t.Errorf("production collapsed with the fault peer: %.2f momentums/min vs baseline %.2f", withLiar, base)
	}
	fmt.Printf("N-5 drip: baseline %.2f momentums/min, with fault peer %.2f momentums/min\n", base, withLiar)
}
