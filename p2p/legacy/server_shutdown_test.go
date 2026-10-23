package legacy

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/zenon-network/go-zenon/p2p"
	"github.com/zenon-network/go-zenon/p2p/discover"
)

// startShutdownTestServer starts a minimal server suitable for shutdown
// tests: one listener, no discovery, no dialing, and a hook that reports
// every peer that completes its handshake.
func startShutdownTestServer(t *testing.T, maxPeers int) (*Server, <-chan *Peer) {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	added := make(chan *Peer, 16)
	srv := &Server{
		PrivateKey:  key,
		MaxPeers:    maxPeers,
		ListenAddr:  "127.0.0.1:0",
		NoDial:      true,
		newPeerHook: func(p *Peer) { added <- p },
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	return srv, added
}

// dialServer dials the server and completes both handshakes, returning the
// raw connection and a client-side rlpx transport ready for protocol
// messages. The caller is responsible for closing the connection.
func dialServer(t *testing.T, srv *Server) (net.Conn, *rlpx) {
	t.Helper()
	fd, err := net.Dial("tcp", srv.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	addr := srv.listener.Addr().(*net.TCPAddr)
	dest := &discover.Node{
		ID:  discover.PubkeyID(&srv.PrivateKey.PublicKey),
		IP:  addr.IP,
		TCP: uint16(addr.Port),
	}
	cliKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	cli := newRLPX(fd).(*rlpx)
	if _, err := cli.doEncHandshake(cliKey, dest); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.doProtoHandshake(&protoHandshake{
		Version: baseProtocolVersion,
		Name:    "shutdown-test-client",
		ID:      discover.PubkeyID(&cliKey.PublicKey),
	}); err != nil {
		t.Fatal(err)
	}
	return fd, cli
}

// TestStopJoinsRunLoop verifies that Stop does not return before the run
// loop has disconnected all peers and removed them from the peer set. A
// gated-close test: the server's Stop must wait long enough for the peer's
// runPeer goroutine to send on delpeer and for the run loop to process it.
func TestStopJoinsRunLoop(t *testing.T) {
	srv, added := startShutdownTestServer(t, 10)
	fd, cli := dialServer(t, srv)
	defer fd.Close()

	// Wait until the peer is fully registered.
	select {
	case <-added:
	case <-time.After(5 * time.Second):
		t.Fatal("peer not admitted")
	}

	// Verify the peer is in the peer set.
	if n := srv.PeerCount(); n != 1 {
		t.Fatalf("PeerCount = %d, want 1", n)
	}

	// Stop must not return until the peer is fully disconnected and the
	// run loop has exited. We verify this by checking that immediately
	// after Stop returns, PeerCount is 0 (the run loop has processed all
	// delpeer messages).
	srv.Stop()

	if n := srv.PeerCount(); n != 0 {
		t.Fatalf("PeerCount = %d after Stop, want 0", n)
	}

	// The client should see the connection close: either a disconnect
	// message or a read error.
	for {
		msg, err := cli.ReadMsg()
		if err != nil {
			break
		}
		msg.Discard()
	}
}

// TestStopConcurrent calls Stop from multiple goroutines simultaneously.
// All calls must return without panic, and the server must be fully shut
// down afterward.
func TestStopConcurrent(t *testing.T) {
	srv, added := startShutdownTestServer(t, 10)
	fd, _ := dialServer(t, srv)
	defer fd.Close()

	select {
	case <-added:
	case <-time.After(5 * time.Second):
		t.Fatal("peer not admitted")
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			srv.Stop()
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent Stop calls did not return")
	}

	if n := srv.PeerCount(); n != 0 {
		t.Fatalf("PeerCount = %d after concurrent Stop, want 0", n)
	}
}

// TestStopRepeated calls Stop multiple times sequentially. Each call must
// return promptly without panic.
func TestStopRepeated(t *testing.T) {
	srv, _ := startShutdownTestServer(t, 10)

	srv.Stop()
	srv.Stop()
	srv.Stop()
}

// TestCheckpointRaceStopDuringHandshake verifies that Stop racing a
// checkpoint does not leave the run loop blocked on an unbuffered c.cont
// send. The test dials the server but delays the protocol handshake so
// that the server's run loop is blocked sending on c.cont (unbuffered)
// when Stop closes srv.quit. The select on srv.quit in the run loop must
// unblock it.
func TestCheckpointRaceStopDuringHandshake(t *testing.T) {
	srv, _ := startShutdownTestServer(t, 10)

	// Dial and complete the encryption handshake only. The server will
	// send the conn through the posthandshake checkpoint and block on
	// c.cont because the test never reads from the connection (the
	// protocol handshake is never initiated from our side).
	fd, err := net.Dial("tcp", srv.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()

	addr := srv.listener.Addr().(*net.TCPAddr)
	dest := &discover.Node{
		ID:  discover.PubkeyID(&srv.PrivateKey.PublicKey),
		IP:  addr.IP,
		TCP: uint16(addr.Port),
	}
	cliKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	cli := newRLPX(fd).(*rlpx)
	if _, err := cli.doEncHandshake(cliKey, dest); err != nil {
		t.Fatal(err)
	}
	// Deliberately do NOT do the protocol handshake. The server is now
	// blocked in the posthandshake checkpoint send or about to be.

	// Give the server a moment to enter the checkpoint.
	time.Sleep(100 * time.Millisecond)

	// Stop must return promptly even though the run loop may be blocked
	// on a checkpoint reply.
	done := make(chan struct{})
	go func() {
		srv.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return promptly while a checkpoint was in flight")
	}
}

// TestSameIdentityReconnect verifies that within a running server, once a
// peer disconnects and is removed from the peer set, a new connection with
// the same identity is accepted. This exercises the delpeer ordering: the
// old peer's runPeer must have sent on delpeer (and the run loop must have
// processed it) before a new connection with the same ID can be added.
func TestSameIdentityReconnect(t *testing.T) {
	srv, added := startShutdownTestServer(t, 10)

	// Connect peer 1.
	cliKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	nodeID := discover.PubkeyID(&cliKey.PublicKey)

	fd1, err := net.Dial("tcp", srv.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	addr := srv.listener.Addr().(*net.TCPAddr)
	dest := &discover.Node{
		ID:  discover.PubkeyID(&srv.PrivateKey.PublicKey),
		IP:  addr.IP,
		TCP: uint16(addr.Port),
	}
	cli1 := newRLPX(fd1).(*rlpx)
	if _, err := cli1.doEncHandshake(cliKey, dest); err != nil {
		t.Fatal(err)
	}
	if _, err := cli1.doProtoHandshake(&protoHandshake{
		Version: baseProtocolVersion,
		Name:    "reconnect-test-1",
		ID:      nodeID,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-added:
	case <-time.After(5 * time.Second):
		t.Fatal("first peer not admitted")
	}

	// Disconnect peer 1 by closing the connection.
	fd1.Close()

	// Wait for the peer to be removed from the peer set. With the fix,
	// p.wg.Wait() ensures the peer's goroutines finish before delpeer is
	// sent, and the run loop processes delpeer before accepting new
	// connections.
	waitFor(t, "peer removal", func() bool { return srv.PeerCount() == 0 })

	// Connect peer 2 with the same identity.
	fd2, err := net.Dial("tcp", srv.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer fd2.Close()
	cli2 := newRLPX(fd2).(*rlpx)
	if _, err := cli2.doEncHandshake(cliKey, dest); err != nil {
		t.Fatal(err)
	}
	if _, err := cli2.doProtoHandshake(&protoHandshake{
		Version: baseProtocolVersion,
		Name:    "reconnect-test-2",
		ID:      nodeID,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-added:
	case <-time.After(5 * time.Second):
		t.Fatal("second peer with same identity not admitted")
	}

	if n := srv.PeerCount(); n != 1 {
		t.Fatalf("PeerCount = %d, want 1", n)
	}
	srv.Stop()
}

// TestPeerRunJoinsProtocolGoroutines verifies that Peer.run waits for its
// protocol goroutines to finish before returning. The test uses a
// protocol whose Run function signals that it has started and then blocks
// until the peer closes; with p.wg.Wait() restored, run() must not return
// before the protocol goroutine has finished.
func TestPeerRunJoinsProtocolGoroutines(t *testing.T) {
	tr := newHeldTransport()
	protoStarted := make(chan struct{})
	protoDone := make(chan struct{})
	proto := p2p.Protocol{
		Name:    "join-test",
		Version: 1,
		Length:  1,
		Run: func(_ p2p.Peer, rw p2p.MsgReadWriter) error {
			close(protoStarted)
			<-protoDone
			return nil
		},
	}
	p, done := runTestPeer(t, tr, nil, []p2p.Cap{proto.Cap()}, []p2p.Protocol{proto})

	// Wait for the protocol goroutine to start.
	select {
	case <-protoStarted:
	case <-time.After(testTimeout):
		t.Fatal("protocol goroutine did not start")
	}

	// Disconnect the peer. run() should close the transport and signal
	// the protocol to exit via p.closed, but with wg.Wait() restored it
	// must not return until the protocol goroutine actually finishes.
	p.Disconnect(p2p.DiscQuitting)

	// run() should be waiting for the protocol goroutine. Give it a
	// moment to confirm it hasn't returned yet.
	select {
	case r := <-done:
		t.Fatalf("run returned %v before protocol goroutine finished", r)
	case <-time.After(200 * time.Millisecond):
	}

	// Let the protocol goroutine finish.
	close(protoDone)

	// Now run() should return.
	expectReason(t, done, p2p.DiscQuitting)
}

// TestStopReturnsAfterPeerDisconnect verifies the gated-close property:
// Stop must not return until all peers are disconnected and their
// goroutines have finished. This is the primary regression test for the
// missing loopWG.Wait() in Stop.
func TestStopReturnsAfterPeerDisconnect(t *testing.T) {
	srv, added := startShutdownTestServer(t, 10)

	// Connect two peers.
	fd1, _ := dialServer(t, srv)
	defer fd1.Close()
	select {
	case <-added:
	case <-time.After(5 * time.Second):
		t.Fatal("peer 1 not admitted")
	}
	fd2, _ := dialServer(t, srv)
	defer fd2.Close()
	select {
	case <-added:
	case <-time.After(5 * time.Second):
		t.Fatal("peer 2 not admitted")
	}

	if n := srv.PeerCount(); n != 2 {
		t.Fatalf("PeerCount = %d, want 2", n)
	}

	// Stop and verify it returns.
	done := make(chan struct{})
	go func() {
		srv.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return")
	}

	// After Stop returns, the peer set must be empty.
	if n := srv.PeerCount(); n != 0 {
		t.Fatalf("PeerCount = %d after Stop, want 0", n)
	}
}
