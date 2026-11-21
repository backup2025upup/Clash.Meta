package outbound

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"

	"github.com/metacubex/mihomo/component/dialer"
	C "github.com/metacubex/mihomo/constant"

	"github.com/metacubex/ssh"
)

type countingSshDialer struct {
	C.Dialer
	dials atomic.Int32
}

func (d *countingSshDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	d.dials.Add(1)
	return d.Dialer.DialContext(ctx, network, address)
}

// serveEchoSsh accepts password logins and echoes every direct-tcpip channel.
func serveEchoSsh(t *testing.T) netip.AddrPort {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatal(err)
	}
	config := &ssh.ServerConfig{
		PasswordCallback: func(ssh.ConnMetadata, []byte) (*ssh.Permissions, error) { return nil, nil },
	}
	config.AddHostKey(signer)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				_, channels, requests, err := ssh.NewServerConn(conn, config)
				if err != nil {
					return
				}
				go ssh.DiscardRequests(requests)
				for newChannel := range channels {
					channel, channelRequests, err := newChannel.Accept()
					if err != nil {
						continue
					}
					go ssh.DiscardRequests(channelRequests)
					go func() {
						_, _ = io.Copy(channel, channel)
						_ = channel.Close()
					}()
				}
			}()
		}
	}()
	return netip.MustParseAddrPort(listener.Addr().String())
}

func TestSshResetNetworkDialsAgain(t *testing.T) {
	addrPort := serveEchoSsh(t)
	counting := &countingSshDialer{Dialer: dialer.NewDialer()}
	option := SshOption{
		Name:     "ssh",
		Server:   addrPort.Addr().String(),
		Port:     int(addrPort.Port()),
		UserName: "test",
		Password: "test",
	}
	option.DialerForAPI = counting
	out, err := NewSsh(option)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()

	echo := func() {
		t.Helper()
		conn, err := out.DialContext(context.Background(), &C.Metadata{
			DstIP:   netip.MustParseAddr("192.0.2.1"),
			DstPort: 80,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := conn.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		reply := make([]byte, 4)
		if _, err := io.ReadFull(conn, reply); err != nil || string(reply) != "ping" {
			t.Fatalf("echo = %q, %v", reply, err)
		}
	}

	echo()
	echo()
	if got := counting.dials.Load(); got != 1 {
		t.Fatalf("dialed %d times before the reset, want the client reused", got)
	}
	out.ResetNetwork()
	echo()
	if got := counting.dials.Load(); got != 2 {
		t.Fatalf("dialed %d times after the reset, want a new client", got)
	}
}
