package switcher

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/p2p"
	"github.com/zenon-network/go-zenon/p2p/discover"
	"github.com/zenon-network/go-zenon/p2p/legacy"
	"github.com/zenon-network/go-zenon/p2p/libp2p"
)

// sporkPollInterval is how often the sunset watcher checks the
// oracle once Start() has launched it. 1s is fast enough that the
// sunset fires well within a single momentum slot (~10s); the check itself is
// a cached compare, so the cost is negligible.
const sporkPollInterval = 1 * time.Second

// Server is the p2p server that owns both the legacy (devp2p/RLPX) and
// libp2p networking backends. Both backends are started unconditionally
// at Start() time on separate ports. When the libp2p activation spork's
// EnforcementHeight is reached on the local chain, the legacy backend is
// retired (sunset) and libp2p continues as the sole transport.
//
// Starting both backends simultaneously fixes the bootstrap deadlock
// described in issue #105: a fresh node whose local chain hasn't reached
// the spork enforcement height can still bootstrap from libp2p peers,
// because libp2p is already listening regardless of the oracle's state.
//
// Server implements p2p.Server so callers (node, rpc) hold it via the
// interface and never reach an individual backend directly.
//
// All public fields are read-only after Start().
type Server struct {
	// ---- shared config ----
	PrivateKey        *ecdsa.PrivateKey
	Name              string
	MaxPeers          int // must be > 0; Start() rejects anything else
	MinConnectedPeers int
	MaxPendingPeers   int
	ListenAddr        string // "host:port" for the legacy backend
	Protocols         []p2p.Protocol

	// ---- legacy backend config ----
	LegacyBootstrapNodes []*discover.Node
	NodeDatabase         string

	// ---- libp2p backend config ----
	Libp2pBootstrapPeers []peer.AddrInfo
	// Libp2pListenAddr is the listen address for the libp2p backend.
	// Set from node.go as "host:port". Must differ from ListenAddr.
	Libp2pListenAddr string
	// NATPortMap, when true, enables UPnP / NAT-PMP port mapping in
	// the libp2p backend. Default false to match the pre-libp2p
	// network's behaviour (legacy had NAT mapping unset). Operators
	// behind home routers can opt-in via the Net.NATPortMap field in
	// config.json.
	NATPortMap bool
	// PeerstoreDir is the on-disk path for the libp2p backend's peer
	// database (warm-bootstrap candidates recorded across restarts).
	// Forwarded as-is to the libp2p backend. Empty string disables
	// peer persistence.
	PeerstoreDir string

	// ---- activation gate ----
	Oracle SporkOracle

	// ---- test hooks (nil = use real backends) ----
	// NewLegacy, if non-nil, is called instead of constructing a
	// legacy.Server. Tests inject a stub here to avoid real network I/O.
	NewLegacy func() backend
	// NewLibp2p, if non-nil, is called instead of buildLibp2p(). The
	// returned backend is started alongside legacy at Start() time.
	// Tests inject a stub here to control error injection and timing.
	NewLibp2p func() backend

	// ---- internal state (do not set from outside) ----
	mu       sync.RWMutex
	legacy   backend // legacy backend (nil when stopped or after sunset)
	libp2p   backend // libp2p backend (nil when stopped or not yet started)
	swapOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup // tracks the sunset watcher goroutine
}

// backend is the minimal API the switcher needs from each transport
// backend. Both legacy.Server and libp2p.Server already satisfy it; this
// declaration is private documentation of the contract.
type backend interface {
	Start() error
	Stop()
	Peers() []p2p.Peer
	PeerCount() int
	AddPeer(node *discover.Node)
	Self() *discover.Node
}

// Start launches the server.
//
// Both backends are started unconditionally: legacy on ListenAddr and
// libp2p on Libp2pListenAddr. This ensures that a fresh node whose local
// chain hasn't reached the spork enforcement height can still bootstrap
// from libp2p peers (issue #105). If the libp2p backend fails to start
// the error is logged at Crit level but is not fatal — the node continues
// in legacy-only mode.
//
// If an oracle is configured, a sunset watcher goroutine is launched. It
// polls the oracle on a 1s ticker; on the first true reading it triggers
// the legacy sunset (retires the legacy backend, leaving libp2p as the
// sole transport).
func (srv *Server) Start() error {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.stopCh != nil {
		return errors.New("switcher: server already started")
	}

	// Both backends treat this as a hard capacity bound and the libp2p
	// backend's every capacity check is len(peerMap) >= MaxPeers, so a
	// non-positive value means "accept nothing". Reject it here, before
	// either backend starts.
	if srv.MaxPeers <= 0 {
		return fmt.Errorf("switcher: MaxPeers must be > 0 (got %d)", srv.MaxPeers)
	}

	srv.stopCh = make(chan struct{})

	// Advance notice for operators still on an empty libp2p bootstrap
	// list (normal during Phase A of the rollout, mandatory to fix
	// before activation is scheduled). The libp2p backend repeats this
	// louder — at Crit — if it actually starts with no bootstrap
	// entries and no remembered peers.
	if len(srv.Libp2pBootstrapPeers) == 0 {
		common.P2PLogger.Warn("libp2p bootstrap list is empty; after the activation spork this node will rely on its peer database and inbound connections — populate Net.BootstrapPeers before activation is scheduled")
	}

	// Start legacy first (it was the original backend, existing behavior).
	if err := srv.startLegacyLocked(); err != nil {
		return err
	}

	// Start libp2p on its own port. Failure here is not fatal — the node
	// can still operate on legacy — but it is logged loudly.
	if err := srv.startLibp2pLocked(); err != nil {
		common.P2PLogger.Crit("libp2p backend failed to start; node will operate in legacy-only mode", "err", err)
		// Don't return error — legacy is running, node is functional.
	}

	// Start the sunset watcher if oracle is available.
	if srv.Oracle != nil {
		srv.wg.Add(1)
		go srv.watchSunset()
	}
	return nil
}

// Stop terminates both backends and shuts down the sunset watcher. Safe
// to call before Start() (it's a no-op) and safe to call multiple times
// (subsequent calls are no-ops).
//
// Each backend's Stop() is called outside the lock so readers
// (Peers/PeerCount/RPC) aren't blocked for the duration of the teardown.
// After Stop() returns, both backend references are nil and all
// delegating methods return zero values.
func (srv *Server) Stop() {
	srv.mu.Lock()
	if srv.stopCh == nil {
		srv.mu.Unlock()
		return
	}
	close(srv.stopCh)
	srv.stopCh = nil
	legacySrv := srv.legacy
	libp2pSrv := srv.libp2p
	srv.legacy = nil
	srv.libp2p = nil
	srv.mu.Unlock()

	if legacySrv != nil {
		legacySrv.Stop()
	}
	if libp2pSrv != nil {
		libp2pSrv.Stop()
	}
	srv.wg.Wait()
}

// Peers returns the union of currently-connected peers from both
// backends, with duplicates removed. libp2p peers are listed first.
// Returns nil if the server is stopped.
func (srv *Server) Peers() []p2p.Peer {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	var peers []p2p.Peer
	seen := make(map[discover.NodeID]bool)
	// Prefer libp2p peers (listed first).
	if srv.libp2p != nil {
		for _, p := range srv.libp2p.Peers() {
			id := p.ID()
			if !seen[id] {
				seen[id] = true
				peers = append(peers, p)
			}
		}
	}
	if srv.legacy != nil {
		for _, p := range srv.legacy.Peers() {
			id := p.ID()
			if !seen[id] {
				seen[id] = true
				peers = append(peers, p)
			}
		}
	}
	return peers
}

// PeerCount returns the number of unique currently-connected peers across
// both backends. Returns 0 if the server is stopped.
func (srv *Server) PeerCount() int {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	seen := make(map[discover.NodeID]bool)
	count := 0
	if srv.libp2p != nil {
		for _, p := range srv.libp2p.Peers() {
			id := p.ID()
			if !seen[id] {
				seen[id] = true
				count++
			}
		}
	}
	if srv.legacy != nil {
		for _, p := range srv.legacy.Peers() {
			id := p.ID()
			if !seen[id] {
				seen[id] = true
				count++
			}
		}
	}
	return count
}

// AddPeer requests the legacy backend to dial and maintain a connection
// to the given node. libp2p doesn't support AddPeer of discover.Node (it
// uses multiaddr bootstrap); this is fine: libp2p has its own bootstrap
// and peer discovery.
func (srv *Server) AddPeer(node *discover.Node) {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	if srv.legacy != nil {
		srv.legacy.AddPeer(node)
	}
}

// Self returns the local node's endpoint information, preferring the
// libp2p backend if available, falling back to legacy, or returning an
// empty Node if the server is stopped.
func (srv *Server) Self() *discover.Node {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	if srv.libp2p != nil {
		return srv.libp2p.Self()
	}
	if srv.legacy != nil {
		return srv.legacy.Self()
	}
	return &discover.Node{}
}

// startLegacyLocked constructs and starts the legacy backend. Caller
// must hold srv.mu.
func (srv *Server) startLegacyLocked() error {
	if srv.NewLegacy != nil {
		srv.legacy = srv.NewLegacy()
	} else {
		srv.legacy = &legacy.Server{
			PrivateKey:        srv.PrivateKey,
			MaxPeers:          srv.MaxPeers,
			MinConnectedPeers: srv.MinConnectedPeers,
			MaxPendingPeers:   srv.MaxPendingPeers,
			Discovery:         true,
			Name:              srv.Name,
			BootstrapNodes:    srv.LegacyBootstrapNodes,
			NodeDatabase:      srv.NodeDatabase,
			Protocols:         srv.Protocols,
			ListenAddr:        srv.ListenAddr,
		}
	}
	if err := srv.legacy.Start(); err != nil {
		srv.legacy = nil
		return fmt.Errorf("switcher: start legacy backend: %w", err)
	}
	return nil
}

// startLibp2pLocked constructs and starts the libp2p backend. Caller
// must hold srv.mu.
func (srv *Server) startLibp2pLocked() error {
	if srv.NewLibp2p != nil {
		srv.libp2p = srv.NewLibp2p()
	} else {
		srv.libp2p = srv.buildLibp2p()
	}
	if err := srv.libp2p.Start(); err != nil {
		srv.libp2p = nil
		return fmt.Errorf("switcher: start libp2p backend: %w", err)
	}
	return nil
}

// buildLibp2p constructs a *libp2p.Server from the switcher's
// configuration without starting it. Pure — no locks, no I/O, no field
// reads on srv beyond the immutable-after-Start config fields.
func (srv *Server) buildLibp2p() *libp2p.Server {
	return &libp2p.Server{
		PrivateKey:        srv.PrivateKey,
		Name:              srv.Name,
		MaxPeers:          srv.MaxPeers,
		MinConnectedPeers: srv.MinConnectedPeers,
		MaxPendingPeers:   srv.MaxPendingPeers,
		Discovery:         true,
		NoDial:            false,
		BootstrapPeers:    srv.Libp2pBootstrapPeers,
		NATPortMap:        srv.NATPortMap,
		PeerstoreDir:      srv.PeerstoreDir,
		ListenAddr:        srv.Libp2pListenAddr,
		Protocols:         srv.Protocols,
	}
}

// sunsetLegacy retires the legacy backend once the libp2p spork's
// EnforcementHeight has been reached on the local chain. The libp2p
// backend is already running (started at Start() time), so this simply
// stops legacy and clears the reference. Guarded by sync.Once so
// concurrent triggers cannot double-sunset.
func (srv *Server) sunsetLegacy() {
	srv.swapOnce.Do(func() {
		common.P2PLogger.Info("libp2p spork EnforcementHeight reached; retiring legacy backend")

		srv.mu.Lock()
		legacySrv := srv.legacy
		srv.legacy = nil
		srv.mu.Unlock()

		if legacySrv != nil {
			legacySrv.Stop()
		}

		common.P2PLogger.Info("legacy backend retired; libp2p continues")
	})
}

// watchSunset polls the spork oracle until either the spork activates
// (triggering sunsetLegacy()) or Stop() is called.
//
// The polling cadence is sub-second so the sunset fires well within a
// single momentum slot once the chain crosses EnforcementHeight. The
// check itself is cheap (one map lookup against cached frontier state),
// so polling rather than wiring into a momentum-event bus keeps the
// switcher decoupled from the chain package.
func (srv *Server) watchSunset() {
	defer srv.wg.Done()

	// Capture stopCh once so we never read a nil channel (which blocks
	// forever) if Stop() closes and nils srv.stopCh while we're waiting.
	srv.mu.RLock()
	stopCh := srv.stopCh
	srv.mu.RUnlock()
	if stopCh == nil {
		return
	}

	ticker := time.NewTicker(sporkPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			if srv.Oracle.IsLibp2pActive() {
				srv.sunsetLegacy()
				return
			}
		}
	}
}
