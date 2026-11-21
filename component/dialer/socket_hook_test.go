package dialer

import (
	"context"
	"net"
	"net/netip"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func observeSockets(t *testing.T) *atomic.Int32 {
	t.Helper()
	var seen atomic.Int32
	DefaultSocketObserver = func(network, address string, conn syscall.RawConn) error {
		seen.Add(1)
		return nil
	}
	t.Cleanup(func() { DefaultSocketObserver = nil })
	return &seen
}

func TestSocketObserverSeesDialsAndListens(t *testing.T) {
	seen := observeSockets(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	conn, err := DialContext(context.Background(), "tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	packetConn, err := ListenPacket(context.Background(), "udp", "127.0.0.1:0", netip.AddrPort{})
	if err != nil {
		t.Fatal(err)
	}
	_ = packetConn.Close()

	if got := seen.Load(); got != 2 {
		t.Fatalf("observer saw %d sockets, want 2", got)
	}
}

func TestSocketObserverKeepsInterfaceName(t *testing.T) {
	observeSockets(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := DialContext(ctx, "tcp", "192.0.2.1:9", WithInterface("flclash-missing0"))
	if err == nil {
		_ = conn.Close()
		t.Fatal("dial ignored interface-name")
	}
	if ctx.Err() != nil {
		t.Fatalf("dial ran to its deadline instead of failing to bind: %v", err)
	}
}
