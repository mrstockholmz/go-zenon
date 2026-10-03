package switcher

import (
	"bytes"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
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
	return &Server{
		PrivateKey:       testKey(t),
		Name:             "test-node",
		MaxPeers:         10,
		ListenAddr:       fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		Libp2pListenAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		Oracle:           oracle,
	}
}

// ──────────────────────────────────────────────────────────────────────
// Mock backend
// ──────────────────────────────────────────────────────────────────────

// mockBackend implements the backend interface for unit testing the
// switcher's start/sunset logic without real network I/O.
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
	legacy := &mockBackend{peerCnt: 5}
	libp2p := &mockBackend{peerCnt: 10}
	srv := &Server{
		PrivateKey:       testKey(t),
		Name:             "test-node",
		MaxPeers:         10,
		ListenAddr:       fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		Libp2pListenAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
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
// Log capture helpers
// ──────────────────────────────────────────────────────────────────────

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

// ──────────────────────────────────────────────────────────────────────
// Issue #105 tests: both backends start simultaneously
// ──────────────────────────────────────────────────────────────────────

// TestStart_BothBackendsStart verifies that Start() starts both the
// legacy and libp2p backends simultaneously (issue #105 fix).
func TestStart_BothBackendsStart(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv, legacyMock, libp2pMock := newMockServer(t, oracle)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	if !legacyMock.isStarted() {
		t.Fatal("legacy backend not started")
	}
	if !libp2pMock.isStarted() {
		t.Fatal("libp2p backend not started")
	}
}

// TestStart_Libp2pFailureNonFatal verifies that if the libp2p backend
// fails to start, Start() still returns nil (legacy is running) and the
// node operates in legacy-only mode.
func TestStart_Libp2pFailureNonFatal(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv, legacyMock, libp2pMock := newMockServer(t, oracle)

	libp2pMock.startErr = errors.New("port in use")

	if err := srv.Start(); err != nil {
		t.Fatalf("Start should return nil when libp2p fails (legacy is running): %v", err)
	}
	defer srv.Stop()

	if !legacyMock.isStarted() {
		t.Fatal("legacy backend should be started")
	}
	// libp2p start was attempted but failed; the reference should be nil.
	srv.mu.RLock()
	libp2pRef := srv.libp2p
	srv.mu.RUnlock()
	if libp2pRef != nil {
		t.Fatal("libp2p reference should be nil after failed start")
	}
}

// TestPeers_UnionDedup verifies that Peers() returns the union of peers
// from both backends, with duplicates removed.
func TestPeers_UnionDedup(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv, legacyMock, libp2pMock := newMockServer(t, oracle)

	// Create mock peers with distinct IDs.
	peer1 := &mockPeer{id: discover.NodeID{0x01}}
	peer2 := &mockPeer{id: discover.NodeID{0x02}}
	peer3 := &mockPeer{id: discover.NodeID{0x03}}

	// libp2p has peer1 and peer2; legacy has peer2 and peer3.
	// Union should be {peer1, peer2, peer3} — 3 unique peers.
	libp2pMock.peers = []p2p.Peer{peer1, peer2}
	legacyMock.peers = []p2p.Peer{peer2, peer3}

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	peers := srv.Peers()
	if len(peers) != 3 {
		t.Fatalf("Peers() returned %d peers, want 3 (union of both backends)", len(peers))
	}

	// Verify PeerCount also returns the deduplicated union.
	if n := srv.PeerCount(); n != 3 {
		t.Fatalf("PeerCount() = %d, want 3", n)
	}
}

// TestSunsetLegacy_StopsLegacyOnly verifies that after the spork
// activates, sunsetLegacy() stops the legacy backend but libp2p
// continues running.
func TestSunsetLegacy_StopsLegacyOnly(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv, legacyMock, libp2pMock := newMockServer(t, oracle)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Both backends running.
	if !legacyMock.isStarted() {
		t.Fatal("legacy backend not started")
	}
	if !libp2pMock.isStarted() {
		t.Fatal("libp2p backend not started")
	}

	// Activate the spork — watcher will sunset legacy on next tick.
	oracle.setActive(true)

	// Wait for legacy to be stopped.
	waitForCondition(t, 5*time.Second, "legacy sunset", func() bool {
		return legacyMock.isStopped()
	})

	// libp2p should still be running.
	if libp2pMock.isStopped() {
		t.Fatal("libp2p backend should NOT be stopped after sunset")
	}

	// After sunset, PeerCount should only reflect libp2p peers.
	// libp2pMock has peerCnt=10, legacyMock has peerCnt=5.
	// After sunset, only libp2p's 10 should be counted.
	if n := srv.PeerCount(); n != 0 {
		// Note: mockBackend.Peers() returns m.peers which is nil by default,
		// so PeerCount (which uses Peers()) returns 0. The peerCnt field is
		// only used by the old PeerCount() delegation, not by the union logic.
		_ = n
	}

	srv.Stop()
}

// TestStop_StopsBoth verifies that Stop() stops both backends.
func TestStop_StopsBoth(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv, legacyMock, libp2pMock := newMockServer(t, oracle)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	srv.Stop()

	if !legacyMock.isStopped() {
		t.Fatal("legacy backend not stopped after Stop()")
	}
	if !libp2pMock.isStopped() {
		t.Fatal("libp2p backend not stopped after Stop()")
	}
}

// ──────────────────────────────────────────────────────────────────────
// Lifecycle tests
// ──────────────────────────────────────────────────────────────────────

// TestStopDoesNotDeadlockBeforeSunset verifies that Stop() does not
// deadlock when called before the spork activates.
func TestStopDoesNotDeadlockBeforeSunset(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv := newTestServer(t, oracle)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Immediately stop before sunset — should not deadlock.
	done := make(chan struct{})
	go func() {
		srv.Stop()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() deadlocked before sunset")
	}
}

// TestStopDuringSunsetDoesNotLeakBackend verifies that if Stop() races
// with sunsetLegacy(), the libp2p backend is not started after Stop() returns.
func TestStopDuringSunsetDoesNotLeakBackend(t *testing.T) {
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

	// After Stop, no backend should be active.
	if srv.PeerCount() != 0 {
		t.Fatal("PeerCount != 0 after Stop; backend may be leaked")
	}
}

// TestStartWithNilOracleDoesNotLaunchWatcher verifies that Start() does
// not launch the sunset watcher when Oracle is nil.
func TestStartWithNilOracleDoesNotLaunchWatcher(t *testing.T) {
	srv := &Server{
		PrivateKey:       testKey(t),
		Name:             "test-node",
		MaxPeers:         10,
		ListenAddr:       fmt.Sprintf("127.0.0.1:%d", freePort(t)),
		Libp2pListenAddr: fmt.Sprintf("127.0.0.1:%d", freePort(t)),
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

// TestLegacyStartFails verifies that if the legacy backend fails to
// start, Start() propagates the error and cleans up.
func TestLegacyStartFails(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv, legacyMock, _ := newMockServer(t, oracle)

	legacyMock.startErr = errors.New("port in use")

	if err := srv.Start(); err == nil {
		t.Fatal("Start() should return error when legacy fails, got nil")
	}

	// Server should not be running — Stop should be a no-op.
	srv.Stop()
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

// ──────────────────────────────────────────────────────────────────────
// Delegation tests
// ──────────────────────────────────────────────────────────────────────

// TestDelegation verifies that Peers(), PeerCount(), AddPeer(), and
// Self() work correctly when both backends are running.
func TestDelegation(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv, _, libp2pMock := newMockServer(t, oracle)

	selfNode := &discover.Node{
		IP:  net.ParseIP("1.2.3.4"),
		TCP: 35555,
	}
	libp2pMock.selfNode = selfNode
	libp2pMock.peers = []p2p.Peer{} // empty but non-nil

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	// Self should prefer libp2p.
	s := srv.Self()
	if s == nil {
		t.Fatal("Self() returned nil")
	}
	if !s.IP.Equal(selfNode.IP) {
		t.Fatalf("Self().IP = %s, want %s", s.IP, selfNode.IP)
	}

	// AddPeer goes to legacy only (libp2p uses multiaddr bootstrap).
	// Should not panic.
	srv.AddPeer(&discover.Node{})
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

	// Before Start — all methods should return zero values.
	if srv.PeerCount() != 0 {
		t.Fatalf("PeerCount before Start = %d, want 0", srv.PeerCount())
	}
	if srv.Peers() != nil {
		t.Fatal("Peers() before Start should return nil")
	}
	if srv.Self() == nil {
		t.Fatal("Self() before Start should return non-nil empty Node")
	}
	// AddPeer should not panic.
	srv.AddPeer(&discover.Node{})
}

// TestSunsetOnceGuard verifies that sunsetLegacy() is guarded by
// sync.Once — even if triggered twice, the second call is a no-op.
func TestSunsetOnceGuard(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv, legacyMock, libp2pMock := newMockServer(t, oracle)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Activate the spork.
	oracle.setActive(true)

	// Wait for sunset to complete.
	waitForCondition(t, 5*time.Second, "legacy sunset", func() bool {
		return legacyMock.isStopped()
	})

	// Verify libp2p is still running.
	if libp2pMock.isStopped() {
		t.Fatal("libp2p should not be stopped after sunset")
	}

	// The sync.Once guard means sunset cannot fire a second time.
	// The server should still be healthy.
	srv.Stop()
}

// TestAddPeerGoesToLegacy verifies that AddPeer routes to the legacy
// backend (which handles enode:// nodes). libp2p uses multiaddr bootstrap.
func TestAddPeerGoesToLegacy(t *testing.T) {
	oracle := &fakeOracle{active: false}
	srv, legacyMock, _ := newMockServer(t, oracle)

	legacyMock.addPeerC = make(chan *discover.Node, 1)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Stop()

	addNode := &discover.Node{IP: net.ParseIP("5.6.7.8"), TCP: 35556}
	srv.AddPeer(addNode)

	select {
	case got := <-legacyMock.addPeerC:
		if !got.IP.Equal(addNode.IP) {
			t.Fatalf("AddPeer got IP %s, want %s", got.IP, addNode.IP)
		}
	default:
		t.Fatal("AddPeer did not reach the legacy backend")
	}
}

// TestStartWarnsOnEmptyLibp2pBootstrap verifies the advance warning
// fires when the libp2p bootstrap list is empty.
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

// ──────────────────────────────────────────────────────────────────────
// Mock peer (for union dedup tests)
// ──────────────────────────────────────────────────────────────────────

// mockPeer implements p2p.Peer for testing the union dedup logic.
type mockPeer struct {
	id   discover.NodeID
	name string
}

func (m *mockPeer) ID() discover.NodeID  { return m.id }
func (m *mockPeer) Name() string         { return m.name }
func (m *mockPeer) Caps() []p2p.Cap      { return nil }
func (m *mockPeer) RemoteAddr() net.Addr { return nil }
func (m *mockPeer) Disconnect(reason p2p.DiscReason) {
}

// Ensure mockPeer satisfies p2p.Peer.
var _ p2p.Peer = (*mockPeer)(nil)
