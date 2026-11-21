package session

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/transport/anytls/padding"
)

type pipeDialer struct {
	mu    sync.Mutex
	conns []net.Conn
	gate  chan struct{}
	dials chan struct{}
}

func newPipeDialer() *pipeDialer {
	return &pipeDialer{dials: make(chan struct{}, 16)}
}

func (d *pipeDialer) dial(ctx context.Context) (net.Conn, error) {
	d.mu.Lock()
	gate := d.gate
	d.gate = nil
	d.mu.Unlock()
	d.dials <- struct{}{}
	if gate != nil {
		<-gate
	}
	local, remote := net.Pipe()
	go func() { _, _ = io.Copy(io.Discard, remote) }()
	d.mu.Lock()
	d.conns = append(d.conns, local)
	d.mu.Unlock()
	return local, nil
}

func (d *pipeDialer) dialed() []net.Conn {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]net.Conn(nil), d.conns...)
}

func newTestClient(t *testing.T, dialer *pipeDialer) *Client {
	t.Helper()
	var scheme atomic.Pointer[padding.PaddingFactory]
	padding.UpdatePaddingScheme(padding.DefaultPaddingScheme, &scheme)
	client := NewClient(context.Background(), dialer.dial, &scheme, "", 0, 0, 0, false)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func isClosedConn(conn net.Conn) bool {
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	_, err := conn.Read(make([]byte, 1))
	return err == io.ErrClosedPipe
}

func TestResetNetworkRedialsASessionDialledBeforeIt(t *testing.T) {
	dialer := newPipeDialer()
	gate := make(chan struct{})
	dialer.gate = gate
	client := newTestClient(t, dialer)

	opened := make(chan error, 1)
	go func() {
		stream, err := client.CreateStream(context.Background())
		if err == nil {
			_ = stream.Close()
		}
		opened <- err
	}()
	<-dialer.dials
	client.ResetNetwork()
	close(gate)
	if err := <-opened; err != nil {
		t.Fatal(err)
	}

	conns := dialer.dialed()
	if len(conns) != 2 {
		t.Fatalf("dialed %d connections, want the old network's redialled", len(conns))
	}
	if !isClosedConn(conns[0]) {
		t.Fatal("the connection dialled before the reset stayed open")
	}
}

func TestStreamCloseLeavesAClosedSessionOutOfThePool(t *testing.T) {
	client := newTestClient(t, newPipeDialer())
	stream, err := client.CreateStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	session := stream.(*Stream).sess
	t.Cleanup(func() { _ = session.conn.Close() })

	client.idleSessionLock.Lock()
	closed := make(chan struct{})
	go func() {
		_ = stream.Close()
		close(closed)
	}()
	time.Sleep(50 * time.Millisecond)
	// Close as the session would, with its hook already past the pool.
	session.dieOnce.Do(func() { close(session.die) })
	client.idleSessionLock.Unlock()
	<-closed

	client.idleSessionLock.Lock()
	pooled := client.idleSession.Len()
	client.idleSessionLock.Unlock()
	if pooled != 0 {
		t.Fatal("a closed session went back into the idle pool")
	}
}

func TestCreateStreamSkipsAClosedIdleSession(t *testing.T) {
	dialer := newPipeDialer()
	client := newTestClient(t, dialer)
	stream, err := client.CreateStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	session := stream.(*Stream).sess
	t.Cleanup(func() { _ = session.conn.Close() })
	_ = stream.Close()
	session.dieOnce.Do(func() { close(session.die) })

	stream, err = client.CreateStream(context.Background())
	if err != nil {
		t.Fatalf("took the closed session from the pool: %v", err)
	}
	_ = stream.Close()
	if got := len(dialer.dialed()); got != 2 {
		t.Fatalf("dialed %d connections, want a new session", got)
	}
}
