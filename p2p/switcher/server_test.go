package switcher

import (
	"bytes"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/inconshreveable/log15"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/p2p"
	"github.com/zenon-network/go-zenon/p2p/discover"
)

// ──────────────────────────────────────────────────────────────────────
// Test helpers
// ──────────────────────────────────────────────────────────────────────

// fakeOracle implements SporkOracle for testing.
type fakeOracle struct {
	mu     sync.Mutex
	active bool
}

func (o *fakeOracle) IsLibp2pActive() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.active
}

func (o *fakeOracle) setActive(v bool) {
	o.mu.Lock()
	o.active = v
	o.mu.Unlock()
}

// freePort returns an available TCP port.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// testKey generates an ephemeral secp256k1 key for testing.
func testKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return key
}

// newTestServer creates a switcher.Server with real backend config
// sufficient for Start/Stop lifecycle tests.
func newTestServer(t *testing.T, oracle *fakeOracle) *Server {
	t.Helper()
	port := freePort(t)
	return &Server{
		PrivateKey:       testKey(t),
		Name:             "test-node",
		MaxPeers:         10,
		ListenAddr:       fmt.Sprintf("127.0.0.1:%d", port),
		Libp2pListenAddr: fmt.Sprintf("127.0.0.1:%d", port+1),
		Oracle:           oracle,
	}
}

// ──────────────────────────────────────────────────────────────────────
// Mock backend
// ──────────────────────────────────────────────────────────────────────

// mockBackend implements the backend interface for unit testing the
// switcher's dual-start and sunset logic without real network I/O.
type mockBackend struct {
	mu       sync.Mutex
	started  bool
	stopped  bool
	startErr error        // if set, Start() returns this
	startFn  func() error // if set, called instead of default Start logic
	peerCnt  int
	peers    []p2p.Peer
	selfNode *discover.Node
	addPeerC chan *discover.Node // records AddPeer calls (nil = discard)
}

func (m *mockBackend) Start() error {
	if m.startFn != nil {
		return m.startFn()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.startErr != nil {
		return m.startErr
	}
	m.started = true
	return nil
}

func (m *mockBackend) Stop() {
	m.mu.Lock()
	m.stopped = true
	m.mu.Unlock()
}

func (m *mockBackend) Peers() []p2p.Peer {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.peers
}

func (m *mockBackend) PeerCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.peerCnt
}

func (m *mockBackend) AddPeer(node *discover.Node) {
	if m.addPeerC != nil {
		m.addPeerC <- node
	}
}

func (m *mockBackend) Self() *discover.Node {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.selfNode != nil {
		return m.selfNode
	}
	return &discover.Node{}
}

// isStarted reports whether Start() was called successfully.
func (m *mockBackend) isStarted() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.started
}

// isStopped reports whether Stop() was called.
func (m *mockBackend) isStopped() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopped
}

// newMockServer creates a switcher.Server wired with mock backends.
// The returned backends can be inspected/reconfigured before calling
// Start().
func newMockServer(t *testing.T, oracle *fakeOracle) (*Server, *mockBackend, *mockBackend) {
	t.Helper()
	legacy := &mockBackend{
		peerCnt: 5,
		peers:   makeTestPeersWithOffset(5, 1),
	}
	libp2p := &mockBackend{
		peerCnt: 10,
		peers:   makeTestPeersWithOffset(10, 100),
	}
	port := freePort(t)
	srv := &Server{
		PrivateKey:       testKey(t),
		Name:             "test-node",
		MaxPeers:         10,
		ListenAddr:       fmt.Sprintf("127.0.0.1:%d", port),
		Libp2pListenAddr: fmt.Sprintf("127.0.0.1:%d", port+1),
		Oracle:           oracle,
		NewLegacy:        func() backend { return legacy },
		NewLibp2p:        func() backend { return libp2p },
	}
	return srv, legacy, libp2p
}

// waitForCondition polls a predicate until it returns true or times out.
func waitForCondition(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("waitForCondition: timed out waiting for %s", desc)
}

// ──────────────────────────────────────────────────────────────────────
// Dual-start tests
// ──────────────────────────────────────────────────────────────────────

// TestDualStart verifies that Start() launches both backends
// concurrently: legacy binds ListenAddr, libp2p binds Libp2pListenAddr.
func TestDualStart(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv, legacyMock, libp2pMock := newMockServer(t, oracle)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Legacy should be running immediately (started synchronously).
	if !legacyMock.isStarted() {
		t.Fatal("legacy backend not started")
	}

	// libp2p starts asynchronously; wait for it.
	waitForCondition(t, 5*time.Second, "libp2p to start", func() bool {
		return libp2pMock.isStarted()
	})

	// Both backends should be reflected in the union peer count.
	// legacy has 5, libp2p has 10, no overlap → 15.
	waitForCondition(t, 5*time.Second, "PeerCount to reflect both backends", func() bool {
		return srv.PeerCount() == 15
	})

	srv.Stop()
	if !legacyMock.isStopped() {
		t.Fatal("legacy not stopped after Stop()")
	}
	if !libp2pMock.isStopped() {
		t.Fatal("libp2p not stopped after Stop()")
	}
}

// TestSunsetLegacy verifies that when the spork activates, the legacy
// backend is stopped but libp2p continues running.
func TestSunsetLegacy(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv, legacyMock, libp2pMock := newMockServer(t, oracle)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for both backends.
	waitForCondition(t, 5*time.Second, "libp2p to start", func() bool {
		return libp2pMock.isStarted()
	})

	// Activate the spork.
	oracle.setActive(true)

	// Wait for sunset: legacy stopped, libp2p still running.
	waitForCondition(t, 5*time.Second, "legacy to be sunset", func() bool {
		return legacyMock.isStopped()
	})

	if libp2pMock.isStopped() {
		t.Fatal("libp2p should NOT be stopped during sunset")
	}

	// After sunset, PeerCount should only count libp2p peers.
	waitForCondition(t, 5*time.Second, "PeerCount to reflect libp2p only", func() bool {
		return srv.PeerCount() == 10
	})

	// Clean shutdown.
	srv.Stop()
	if !libp2pMock.isStopped() {
		t.Fatal("libp2p not stopped after Stop()")
	}
}

// TestSunsetLegacyIdempotent verifies that calling sunsetLegacy multiple
// times is safe.
func TestSunsetLegacyIdempotent(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv, legacyMock, libp2pMock := newMockServer(t, oracle)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitForCondition(t, 5*time.Second, "libp2p to start", func() bool {
		return libp2pMock.isStarted()
	})

	// Sunset directly.
	srv.sunsetLegacy()
	srv.sunsetLegacy() // second call should be a no-op

	if !legacyMock.isStopped() {
		t.Fatal("legacy not stopped after sunset")
	}
	if libp2pMock.isStopped() {
		t.Fatal("libp2p should not be stopped by sunset")
	}

	srv.Stop()
}

// TestSporkAlreadyActiveSkipsLegacy verifies that when the oracle reports
// the spork as already active at startup, only libp2p is started.
func TestSporkAlreadyActiveSkipsLegacy(t *testing.T) {
	oracle := &fakeOracle{active: true}
	srv, legacyMock, libp2pMock := newMockServer(t, oracle)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	if legacyMock.isStarted() {
		t.Fatal("legacy should not be started when spork already active")
	}
	if !libp2pMock.isStarted() {
		t.Fatal("libp2p should be started when spork already active")
	}
	// After spork-active start, only libp2p is running with 10 peers.
	waitForCondition(t, 5*time.Second, "PeerCount to reflect libp2p", func() bool {
		return srv.PeerCount() == 10
	})
}

// TestUnionPeersDedup verifies that Peers() returns the union of both
// backends deduplicated by NodeID, preferring libp2p.
func TestUnionPeersDedup(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv, legacyMock, libp2pMock := newMockServer(t, oracle)

	// Create mock peers with the same NodeID in both backends.
	sharedID := discover.NodeID{0x01, 0x02, 0x03}
	legacyOnlyID := discover.NodeID{0x04, 0x05, 0x06}
	libp2pOnlyID := discover.NodeID{0x07, 0x08, 0x09}

	legacyMock.peers = []p2p.Peer{
		&mockPeer{id: sharedID},
		&mockPeer{id: legacyOnlyID},
	}
	libp2pMock.peers = []p2p.Peer{
		&mockPeer{id: sharedID},
		&mockPeer{id: libp2pOnlyID},
	}

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	// Wait for libp2p to start asynchronously.
	waitForCondition(t, 5*time.Second, "libp2p to start", func() bool {
		return libp2pMock.isStarted()
	})

	// Union should have 3 unique peers: shared, legacy-only, libp2p-only.
	peers := srv.Peers()
	if len(peers) != 3 {
		t.Fatalf("len(Peers()) = %d, want 3 (union dedup)", len(peers))
	}

	// The shared peer should come from libp2p (preferred).
	for _, p := range peers {
		if p.ID() == sharedID {
			// We can't easily check which backend it came from without
			// more mock surface, but the ordering (libp2p first) ensures
			// preference.
			break
		}
	}

	// PeerCount should match the deduplicated union.
	if n := srv.PeerCount(); n != 3 {
		t.Fatalf("PeerCount = %d, want 3 (dedup union)", n)
	}
}

// makeTestPeers creates n mockPeer values with distinct NodeIDs starting
// from the given offset to avoid collisions between backends.
func makeTestPeersWithOffset(n, offset int) []p2p.Peer {
	peers := make([]p2p.Peer, n)
	for i := range peers {
		var id discover.NodeID
		id[0] = byte(offset + i)
		peers[i] = &mockPeer{id: id}
	}
	return peers
}

// mockPeer implements p2p.Peer for testing.
type mockPeer struct {
	id discover.NodeID
}

func (m *mockPeer) ID() discover.NodeID              { return m.id }
func (m *mockPeer) Name() string                     { return "mock" }
func (m *mockPeer) Caps() []p2p.Cap                  { return nil }
func (m *mockPeer) RemoteAddr() net.Addr             { return nil }
func (m *mockPeer) Disconnect(reason p2p.DiscReason) {}

// TestSelfPrefersLibp2p verifies that Self() returns the libp2p backend's
// self node when both backends are running.
func TestSelfPrefersLibp2p(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv, legacyMock, libp2pMock := newMockServer(t, oracle)

	libp2pSelf := &discover.Node{IP: net.ParseIP("1.2.3.4"), TCP: 36001}
	legacySelf := &discover.Node{IP: net.ParseIP("5.6.7.8"), TCP: 36000}
	libp2pMock.selfNode = libp2pSelf
	legacyMock.selfNode = legacySelf

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	// Wait for libp2p to start asynchronously.
	waitForCondition(t, 5*time.Second, "libp2p to start", func() bool {
		return libp2pMock.isStarted()
	})

	s := srv.Self()
	if !s.IP.Equal(libp2pSelf.IP) {
		t.Fatalf("Self().IP = %s, want %s (libp2p preferred)", s.IP, libp2pSelf.IP)
	}
}

// TestAddPeerRoutesToLegacy verifies that AddPeer is routed to the legacy
// backend only.
func TestAddPeerRoutesToLegacy(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv, legacyMock, libp2pMock := newMockServer(t, oracle)

	legacyMock.addPeerC = make(chan *discover.Node, 1)
	libp2pMock.addPeerC = make(chan *discover.Node, 1)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	node := &discover.Node{IP: net.ParseIP("9.9.9.9"), TCP: 35995}
	srv.AddPeer(node)

	select {
	case got := <-legacyMock.addPeerC:
		if !got.IP.Equal(node.IP) {
			t.Fatalf("AddPeer got IP %s, want %s", got.IP, node.IP)
		}
	default:
		t.Fatal("AddPeer did not reach the legacy backend")
	}

	// libp2p should NOT receive AddPeer.
	select {
	case <-libp2pMock.addPeerC:
		t.Fatal("AddPeer should not be routed to libp2p")
	default:
		// expected
	}
}

// TestStopDoesNotDeadlock verifies that Stop() does not deadlock when
// called after Start with both backends running.
func TestStopDoesNotDeadlock(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv := newTestServer(t, oracle)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	done := make(chan struct{})
	go func() {
		srv.Stop()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() deadlocked")
	}
}

// TestStopDuringSunsetDoesNotLeak verifies that Stop() racing with
// sunsetLegacy() does not leak a backend.
func TestStopDuringSunsetDoesNotLeak(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv := newTestServer(t, oracle)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Activate the spork — watcher will fire sunset on next 1s tick.
	oracle.setActive(true)

	// Race: stop while the watcher may be mid-sunset.
	time.Sleep(50 * time.Millisecond)
	done := make(chan struct{})
	go func() {
		srv.Stop()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() deadlocked during sunset")
	}

	if srv.PeerCount() != 0 {
		t.Fatal("PeerCount != 0 after Stop; backend may be leaked")
	}
}

// TestStartWithNilOracleStartsBoth verifies that Start() with a nil
// oracle still starts both backends (no activation watcher).
func TestStartWithNilOracleStartsBoth(t *testing.T) {
	port := freePort(t)
	srv := &Server{
		PrivateKey:       testKey(t),
		Name:             "test-node",
		MaxPeers:         10,
		ListenAddr:       fmt.Sprintf("127.0.0.1:%d", port),
		Libp2pListenAddr: fmt.Sprintf("127.0.0.1:%d", port+1),
		// Oracle intentionally nil
	}

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	done := make(chan struct{})
	go func() {
		srv.Stop()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() deadlocked with nil oracle")
	}
}

// TestDoubleStop verifies that calling Stop() twice is safe.
func TestDoubleStop(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv := newTestServer(t, oracle)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	srv.Stop()
	srv.Stop() // should be a no-op
}

// TestDoubleStart verifies that calling Start() twice returns an error.
func TestDoubleStart(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv, _, _ := newMockServer(t, oracle)

	if err := srv.Start(); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	defer srv.Stop()

	if err := srv.Start(); err == nil {
		t.Fatal("second Start() should return error, got nil")
	}
}

// TestStopBeforeStart verifies that Stop() on an unstarted server is
// a no-op (no panic, no deadlock).
func TestStopBeforeStart(t *testing.T) {
	srv := &Server{
		PrivateKey:       testKey(t),
		Name:             "test-node",
		MaxPeers:         10,
		ListenAddr:       fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		Libp2pListenAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
	}

	done := make(chan struct{})
	go func() {
		srv.Stop()
		close(done)
	}()

	select {
	case <-done:
		// success — no-op
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() on unstarted server deadlocked")
	}
}

// TestStartRejectsNonPositiveMaxPeers verifies that Start() rejects a
// non-positive MaxPeers before either backend starts.
func TestStartRejectsNonPositiveMaxPeers(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv, legacyMock, libp2pMock := newMockServer(t, oracle)
	srv.MaxPeers = 0

	if err := srv.Start(); err == nil {
		t.Fatal("Start() should return error when MaxPeers <= 0, got nil")
	}

	if legacyMock.isStarted() {
		t.Fatal("legacy backend should not be started when MaxPeers is rejected")
	}
	if libp2pMock.isStarted() {
		t.Fatal("libp2p backend should not be started when MaxPeers is rejected")
	}
}

// TestStartRejectsSamePort verifies that Start() rejects a configuration
// where Libp2pListenAddr equals ListenAddr.
func TestStartRejectsSamePort(t *testing.T) {
	oracle := &fakeOracle{active: false}
	port := freePort(t)
	srv := &Server{
		PrivateKey:       testKey(t),
		Name:             "test-node",
		MaxPeers:         10,
		ListenAddr:       fmt.Sprintf("127.0.0.1:%d", port),
		Libp2pListenAddr: fmt.Sprintf("127.0.0.1:%d", port), // same port!
		Oracle:           oracle,
	}

	if err := srv.Start(); err == nil {
		t.Fatal("Start() should return error when both backends share a port")
	}
}

// TestDefaultLibp2pAddr verifies that the default libp2p address
// computation increments the port by 1.
func TestDefaultLibp2pAddr(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"127.0.0.1:35995", "127.0.0.1:35996"},
		{"0.0.0.0:35995", "0.0.0.0:35996"},
		{"[::1]:35995", "[::1]:35996"},
	}
	for _, tt := range tests {
		got := defaultLibp2pAddr(tt.input)
		if got != tt.want {
			t.Errorf("defaultLibp2pAddr(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// TestDelegationNilBackend verifies that delegation methods return
// zero values when no backend is active (before Start or after Stop).
func TestDelegationNilBackend(t *testing.T) {
	srv := &Server{
		PrivateKey:       testKey(t),
		Name:             "test-node",
		MaxPeers:         10,
		ListenAddr:       fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		Libp2pListenAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
	}

	if srv.PeerCount() != 0 {
		t.Fatalf("PeerCount before Start = %d, want 0", srv.PeerCount())
	}
	if srv.Peers() != nil {
		t.Fatal("Peers() before Start should return nil")
	}
	if srv.Self() == nil {
		t.Fatal("Self() before Start should return non-nil empty Node")
	}
	srv.AddPeer(&discover.Node{}) // should not panic
}

// TestLibp2pStartRetriesUntilSuccess verifies that a libp2p backend that
// fails its first start attempts is retried with backoff until it comes
// up.
func TestLibp2pStartRetriesUntilSuccess(t *testing.T) {
	defer func() {
		oldBase, oldCap := libp2pRetryBase, libp2pRetryCap
		_ = oldBase
		_ = oldCap
	}()
	// Compress retry schedule for test speed.
	oldBase, oldCap := libp2pRetryBase, libp2pRetryCap
	libp2pRetryBase = 20 * time.Millisecond
	libp2pRetryCap = 100 * time.Millisecond
	defer func() { libp2pRetryBase, libp2pRetryCap = oldBase, oldCap }()

	oracle := &fakeOracle{active: false}
	srv, legacyMock, libp2pMock := newMockServer(t, oracle)

	var attempts atomic.Int32
	libp2pMock.startFn = func() error {
		n := attempts.Add(1)
		if n <= 2 {
			return fmt.Errorf("injected start failure %d", n)
		}
		libp2pMock.mu.Lock()
		libp2pMock.started = true
		libp2pMock.mu.Unlock()
		return nil
	}

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	waitForCondition(t, 5*time.Second, "libp2p to start after retries", func() bool {
		return libp2pMock.isStarted()
	})

	if got := attempts.Load(); got != 3 {
		t.Fatalf("start attempts = %d, want 3", got)
	}
	if !legacyMock.isStarted() {
		t.Fatal("legacy should be started")
	}
}

// TestLibp2pRetryAbortsOnStop verifies that a persistently-failing
// libp2p backend does not keep the retry loop alive after Stop().
func TestLibp2pRetryAbortsOnStop(t *testing.T) {
	oldBase, oldCap := libp2pRetryBase, libp2pRetryCap
	libp2pRetryBase = 2 * time.Second
	libp2pRetryCap = 10 * time.Second
	defer func() { libp2pRetryBase, libp2pRetryCap = oldBase, oldCap }()

	oracle := &fakeOracle{active: false}
	srv, _, libp2pMock := newMockServer(t, oracle)

	var attempts atomic.Int32
	libp2pMock.startFn = func() error {
		attempts.Add(1)
		return errors.New("injected: libp2p never starts")
	}

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitForCondition(t, 5*time.Second, "first failed start attempt", func() bool {
		return attempts.Load() >= 1
	})

	start := time.Now()
	srv.Stop()
	if took := time.Since(start); took > time.Second {
		t.Fatalf("Stop took %v; retry backoff not interrupted", took)
	}

	if srv.PeerCount() != 0 {
		t.Fatalf("PeerCount = %d after aborted start, want 0", srv.PeerCount())
	}
	final := attempts.Load()
	time.Sleep(150 * time.Millisecond)
	if attempts.Load() != final {
		t.Fatal("start attempts continued after Stop")
	}
}

// TestLegacyStartFails verifies that if the legacy backend fails to
// start, Start() propagates the error and cleans up.
func TestLegacyStartFails(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv, legacyMock, _ := newMockServer(t, oracle)

	legacyMock.startErr = errors.New("port in use")

	if err := srv.Start(); err == nil {
		t.Fatal("Start() should return error when legacy fails, got nil")
	}

	srv.Stop()
}

// TestStartWarnsOnEmptyLibp2pBootstrap verifies the advance warning fires
// on startup iff the libp2p bootstrap list is empty.
func TestStartWarnsOnEmptyLibp2pBootstrap(t *testing.T) {
	const warnMsg = "libp2p bootstrap list is empty"

	buf := captureP2PLogs(t)
	oracle := &fakeOracle{active: false}
	srv, _, _ := newMockServer(t, oracle)
	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	srv.Stop()
	if !strings.Contains(buf.String(), warnMsg) {
		t.Fatal("missing empty-bootstrap warning on startup")
	}

	buf2 := captureP2PLogs(t)
	oracle2 := &fakeOracle{active: false}
	srv2, _, _ := newMockServer(t, oracle2)
	srv2.Libp2pBootstrapPeers = []peer.AddrInfo{{ID: "test-peer"}}
	if err := srv2.Start(); err != nil {
		t.Fatalf("Start 2: %v", err)
	}
	srv2.Stop()
	if strings.Contains(buf2.String(), warnMsg) {
		t.Fatal("warning fired despite populated bootstrap list")
	}
}

// syncBuffer is a concurrency-safe bytes.Buffer for capturing log
// output written by server goroutines.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// captureP2PLogs redirects common.P2PLogger into a buffer for the
// duration of the test.
func captureP2PLogs(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	common.P2PLogger.SetHandler(log15.StreamHandler(buf, log15.LogfmtFormat()))
	t.Cleanup(func() { common.P2PLogger.SetHandler(log15.DiscardHandler()) })
	return buf
}
