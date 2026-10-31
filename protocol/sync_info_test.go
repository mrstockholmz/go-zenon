package protocol

import (
	"net"
	"testing"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/p2p"
	"github.com/zenon-network/go-zenon/p2p/discover"
)

// stubChainBridgeWithHeight is a ChainBridge whose CurrentBlock returns a
// momentum at a configurable height, so syncInfo can be tested against
// different local frontier states.
type stubChainBridgeWithHeight struct {
	stubChainBridge
	height uint64
}

func (s *stubChainBridgeWithHeight) CurrentBlock() *nom.Momentum {
	return &nom.Momentum{Height: s.height}
}

// stubP2pPeer implements p2p.Peer for newPeer.
type stubP2pPeer struct {
	id discover.NodeID
}

func (s *stubP2pPeer) ID() discover.NodeID              { return s.id }
func (s *stubP2pPeer) Name() string                     { return "stub" }
func (s *stubP2pPeer) Caps() []p2p.Cap                  { return nil }
func (s *stubP2pPeer) RemoteAddr() net.Addr             { return nil }
func (s *stubP2pPeer) Disconnect(reason p2p.DiscReason) {}
func (s *stubP2pPeer) String() string                   { return "stub" }

// stubMsgRW implements p2p.MsgReadWriter with no-ops.
type stubMsgRW struct{}

func (s *stubMsgRW) ReadMsg() (p2p.Msg, error) { return p2p.Msg{}, nil }
func (s *stubMsgRW) WriteMsg(p2p.Msg) error    { return nil }

// newTestPeer creates a protocol peer with the given advertised height.
func newTestPeer(t *testing.T, id byte, td uint64) *peer {
	nodeID := discover.NodeID{}
	nodeID[0] = id
	p := newPeer(1, 1, &stubP2pPeer{id: nodeID}, &stubMsgRW{})
	p.SetTd(td)
	return p
}

// When a peer advertises a higher head but the downloader is not
// synchronising, the pillar must not be muted.  An unverified advertised
// height alone must not hold the gate closed (issue #240).
func TestSyncInfoAdvertisedHeightNotSyncing(t *testing.T) {
	pm := NewProtocolManager(1, 1, &stubChainBridgeWithHeight{height: 100})

	peer := newTestPeer(t, 1, 200)
	if err := pm.peers.Register(peer); err != nil {
		t.Fatal(err)
	}

	info := pm.syncInfo()
	if info.State != SyncDone {
		t.Fatalf("expected SyncDone when downloader is idle despite higher advertised height, got %v", info.State)
	}
	if info.CurrentHeight != 100 {
		t.Fatalf("expected CurrentHeight 100, got %d", info.CurrentHeight)
	}
	if info.TargetHeight != 200 {
		t.Fatalf("expected TargetHeight 200, got %d", info.TargetHeight)
	}
}

// When the local frontier is at or above the best peer's advertised
// height, State is SyncDone regardless of downloader activity.
func TestSyncInfoCaughtUp(t *testing.T) {
	pm := NewProtocolManager(1, 1, &stubChainBridgeWithHeight{height: 200})

	peer := newTestPeer(t, 1, 100)
	if err := pm.peers.Register(peer); err != nil {
		t.Fatal(err)
	}

	info := pm.syncInfo()
	if info.State != SyncDone {
		t.Fatalf("expected SyncDone when caught up, got %v", info.State)
	}
}

// When there are fewer peers than minPeers, State is NotEnoughPeers
// regardless of heights or downloader activity.
func TestSyncInfoNotEnoughPeers(t *testing.T) {
	pm := NewProtocolManager(5, 1, &stubChainBridgeWithHeight{height: 100})

	peer := newTestPeer(t, 1, 200)
	if err := pm.peers.Register(peer); err != nil {
		t.Fatal(err)
	}

	info := pm.syncInfo()
	if info.State != NotEnoughPeers {
		t.Fatalf("expected NotEnoughPeers, got %v", info.State)
	}
}

// When no peers are connected and minPeers is 0, State is SyncDone
// (targetHeight 0 means there is nothing to sync to).
func TestSyncInfoNoPeers(t *testing.T) {
	pm := NewProtocolManager(0, 1, &stubChainBridgeWithHeight{height: 100})

	info := pm.syncInfo()
	if info.State != SyncDone {
		t.Fatalf("expected SyncDone with no peers, got %v", info.State)
	}
	if info.TargetHeight != 0 {
		t.Fatalf("expected TargetHeight 0, got %d", info.TargetHeight)
	}
}
