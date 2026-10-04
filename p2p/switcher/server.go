package switcher

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/p2p"
	"github.com/zenon-network/go-zenon/p2p/discover"
	"github.com/zenon-network/go-zenon/p2p/legacy"
	"github.com/zenon-network/go-zenon/p2p/libp2p"
)

// sporkPollInterval is how often the activation watcher checks the
// oracle once Start() has launched it. 1s is fast enough that the swap
// fires well within a single momentum slot (~10s); the check itself is
// a cached compare, so the cost is negligible.
const sporkPollInterval = 1 * time.Second

// libp2pRetryBase and libp2pRetryCap bound the exponential backoff
// between libp2p start attempts during initial Start(). A node whose
// libp2p startup hits a transient failure (FD pressure, slow NAT probe)
// must self-heal rather than sit without a libp2p listener until an
// operator intervenes. Vars rather than consts so tests can compress
// the schedule.
var (
	libp2pRetryBase = 1 * time.Second
	libp2pRetryCap  = 60 * time.Second
)

// Server is the dual-transport p2p server. It starts both the legacy
// (devp2p/RLPX) and libp2p backends at Start() time, on separate listen
// ports. When the libp2p activation spork's EnforcementHeight is
// reached on the local chain, the legacy backend is sunset (stopped)
// and libp2p continues running. This ensures a fresh node can always
// bootstrap from libp2p peers regardless of its local chain height,
// fixing the bootstrap deadlock where a pre-spork node could never sync
// the history needed to activate its own libp2p backend.
//
// Server implements p2p.Server so callers (node, rpc) hold it via the
// interface and never reach the active backend directly.
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

	// Libp2pListenAddr is the listen address for the libp2p backend
	// (e.g. "host:port"). Must differ from ListenAddr so both backends
	// can bind concurrently. When empty, defaults to ListenAddr with
	// port+1.
	Libp2pListenAddr string

	// ---- legacy backend config ----
	LegacyBootstrapNodes []*discover.Node
	NodeDatabase         string

	// ---- libp2p backend config ----
	Libp2pBootstrapPeers []peer.AddrInfo
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
	// legacy.Server. The returned backend is used for the pre-activation
	// phase. Tests inject a stub here to avoid real network I/O.
	NewLegacy func() backend
	// NewLibp2p, if non-nil, is called instead of buildLibp2p(). The
	// returned backend is used when the spork activates. Tests inject a
	// stub here to control swap timing and error injection.
	NewLibp2p func() backend

	// ---- internal state (do not set from outside) ----
	mu     sync.RWMutex
	legacy backend // legacy backend (nil when stopped or not yet started)
	libp2p backend // libp2p backend (nil when stopped or not yet started)
	stopCh chan struct{}
	wg     sync.WaitGroup // tracks the activation watcher goroutine
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
// Both backends are started unconditionally:
//   - The legacy backend binds ListenAddr and serves pre-spork peers.
//   - The libp2p backend binds Libp2pListenAddr (default: ListenAddr
//     with port+1) and serves post-spork peers.
//
// This dual-start design ensures a fresh node can always bootstrap from
// libp2p peers regardless of its local chain height. The spork oracle
// is used only to determine when to sunset (stop) the legacy backend.
//
// If the oracle reports the spork as already active at Start() time,
// the legacy backend is skipped entirely — there is no pre-spork
// history to serve on this chain.
func (srv *Server) Start() error {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if srv.stopCh != nil {
		return errors.New("switcher: server already started")
	}

	if srv.MaxPeers <= 0 {
		return fmt.Errorf("switcher: MaxPeers must be > 0 (got %d)", srv.MaxPeers)
	}

	srv.stopCh = make(chan struct{})

	// Default Libp2pListenAddr to ListenAddr with port+1.
	if srv.Libp2pListenAddr == "" {
		srv.Libp2pListenAddr = defaultLibp2pAddr(srv.ListenAddr)
	}

	// Validate that the two backends will not collide on the same port.
	if srv.Libp2pListenAddr == srv.ListenAddr {
		return fmt.Errorf("switcher: Libp2pListenAddr %q must differ from ListenAddr %q", srv.Libp2pListenAddr, srv.ListenAddr)
	}

	libp2pActive := false
	if srv.Oracle != nil {
		libp2pActive = srv.Oracle.IsLibp2pActive()
	}

	if libp2pActive {
		common.P2PLogger.Info("libp2p spork already active on local chain; starting libp2p backend only")
		return srv.startLibp2pLocked()
	}

	common.P2PLogger.Info("starting both p2p backends (legacy + libp2p)")
	if len(srv.Libp2pBootstrapPeers) == 0 {
		common.P2PLogger.Warn("libp2p bootstrap list is empty; this node will rely on its peer database and inbound connections — populate Net.BootstrapPeers before activation is scheduled")
	}

	// Start legacy first (it is the pre-spork default).
	if err := srv.startLegacyLocked(); err != nil {
		return err
	}

	// Start libp2p concurrently — its startup (DHT init, NAT probe) can
	// take seconds and must not block legacy startup. Failures are
	// retried with backoff in the background.
	srv.wg.Add(1)
	go srv.startLibp2pAsync()

	if srv.Oracle != nil {
		srv.wg.Add(1)
		go srv.watchActivation()
	} else {
		common.P2PLogger.Warn("no spork oracle configured; activation watcher not started")
	}
	return nil
}

// Stop terminates both backends and shuts down the activation watcher.
// Safe to call before Start() (it's a no-op) and safe to call multiple
// times (subsequent calls are no-ops).
//
// Each backend's Stop() is called outside the lock so readers
// (Peers/PeerCount/RPC) aren't blocked for the duration of the
// teardown. After Stop() returns, both backend references are nil and
// all delegating methods return zero values.
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

// legacyBackend returns the legacy backend (or nil) under an RLock.
func (srv *Server) legacyBackend() backend {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	return srv.legacy
}

// libp2pBackend returns the libp2p backend (or nil) under an RLock.
func (srv *Server) libp2pBackend() backend {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	return srv.libp2p
}

// Peers returns the union of peers from both backends, deduplicated
// by NodeID. When both backends have a peer with the same NodeID, the
// libp2p peer is preferred. Returns nil if the server is stopped.
func (srv *Server) Peers() []p2p.Peer {
	legacy := srv.legacyBackend()
	libp2p := srv.libp2pBackend()

	if legacy == nil && libp2p == nil {
		return nil
	}

	seen := make(map[discover.NodeID]bool)
	var result []p2p.Peer

	// Collect libp2p peers first (preferred).
	if libp2p != nil {
		for _, p := range libp2p.Peers() {
			id := p.ID()
			if !seen[id] {
				seen[id] = true
				result = append(result, p)
			}
		}
	}

	// Add legacy peers not already seen.
	if legacy != nil {
		for _, p := range legacy.Peers() {
			id := p.ID()
			if !seen[id] {
				seen[id] = true
				result = append(result, p)
			}
		}
	}

	return result
}

// PeerCount returns the number of unique peers across both backends,
// deduplicated by NodeID. Returns 0 if the server is stopped.
func (srv *Server) PeerCount() int {
	return len(srv.Peers())
}

// AddPeer requests the legacy backend to dial and maintain a
// connection to the given node. Discarded if the server is stopped.
// (libp2p uses multiaddr bootstrap, not AddPeer.)
func (srv *Server) AddPeer(node *discover.Node) {
	if b := srv.legacyBackend(); b != nil {
		b.AddPeer(node)
	}
}

// Self returns the local node's endpoint information, preferring the
// libp2p backend and falling back to legacy. Returns an empty Node if
// the server is stopped.
func (srv *Server) Self() *discover.Node {
	if b := srv.libp2pBackend(); b != nil {
		return b.Self()
	}
	if b := srv.legacyBackend(); b != nil {
		return b.Self()
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

// startLibp2pAsync starts the libp2p backend in the background with
// retry-on-failure. It is called as a goroutine from Start(). The
// first start attempt happens immediately; subsequent attempts use
// exponential backoff.
func (srv *Server) startLibp2pAsync() {
	defer srv.wg.Done()

	srv.mu.RLock()
	stopCh := srv.stopCh
	srv.mu.RUnlock()
	if stopCh == nil {
		return
	}

	delay := libp2pRetryBase
	for attempt := 1; ; attempt++ {
		if err := srv.tryStartLibp2p(); err == nil {
			if attempt > 1 {
				common.P2PLogger.Info("libp2p backend started after retries", "attempts", attempt)
			}
			return
		} else {
			common.P2PLogger.Crit("failed to start libp2p backend; will retry",
				"err", err, "attempt", attempt, "next-retry", delay)
		}

		select {
		case <-stopCh:
			return
		case <-time.After(delay):
		}
		delay *= 2
		if delay > libp2pRetryCap {
			delay = libp2pRetryCap
		}
	}
}

// tryStartLibp2p constructs and starts the libp2p backend. Returns nil
// on success. On failure, cleans up the partially-started backend.
func (srv *Server) tryStartLibp2p() error {
	srv.mu.Lock()
	if srv.stopCh == nil {
		srv.mu.Unlock()
		return errors.New("switcher: stopped")
	}
	if srv.NewLibp2p != nil {
		srv.libp2p = srv.NewLibp2p()
	} else {
		srv.libp2p = srv.buildLibp2p()
	}
	backend := srv.libp2p
	srv.mu.Unlock()

	if err := backend.Start(); err != nil {
		backend.Stop()
		srv.mu.Lock()
		srv.libp2p = nil
		srv.mu.Unlock()
		return fmt.Errorf("switcher: start libp2p backend: %w", err)
	}
	return nil
}

// startLibp2pLocked constructs and starts the libp2p backend. Caller
// must hold srv.mu. Used at Start() time when the spork is already
// active and only libp2p is needed.
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

// defaultLibp2pAddr returns the default libp2p listen address given
// the legacy ListenAddr. It increments the port by 1.
func defaultLibp2pAddr(listenAddr string) string {
	_, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		// If we can't parse, return as-is; the backend will fail to start
		// and the retry loop will log it.
		return listenAddr
	}
	var p int
	fmt.Sscanf(port, "%d", &p)
	return net.JoinHostPort(mustHost(listenAddr), fmt.Sprintf("%d", p+1))
}

// mustHost extracts the host part from a host:port string.
func mustHost(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

// watchActivation polls the spork oracle until either the spork
// activates (triggering sunsetLegacy()) or Stop() is called.
//
// The polling cadence is 1s so the sunset fires well within a single
// momentum slot once the chain crosses EnforcementHeight. The check
// itself is cheap (one map lookup against cached frontier state),
// so polling rather than wiring into a momentum-event bus keeps the
// switcher decoupled from the chain package.
func (srv *Server) watchActivation() {
	defer srv.wg.Done()

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

// sunsetLegacy stops the legacy backend when the libp2p activation
// spork fires. The libp2p backend (already running since Start())
// continues serving. This replaces the old swap() which tore down
// legacy and started libp2p atomically — a design that caused a
// bootstrap deadlock for fresh nodes.
//
// The sunset is idempotent: calling it multiple times is safe.
func (srv *Server) sunsetLegacy() {
	srv.mu.Lock()
	if srv.stopCh == nil {
		srv.mu.Unlock()
		return // already stopped
	}
	legacySrv := srv.legacy
	if legacySrv == nil {
		srv.mu.Unlock()
		return // already sunset
	}
	srv.legacy = nil
	srv.mu.Unlock()

	common.P2PLogger.Info("libp2p spork EnforcementHeight reached; sunsetting legacy backend")
	legacySrv.Stop()
	common.P2PLogger.Info("legacy backend sunset complete; libp2p continues")
}
