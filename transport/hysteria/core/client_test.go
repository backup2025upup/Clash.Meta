package core

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/transport/hysteria/transport"

	"github.com/metacubex/quic-go"
	"github.com/metacubex/tls"
)

type countingPacketDialer struct {
	listens atomic.Int32
}

func (d *countingPacketDialer) ListenPacket(net.Addr) (net.PacketConn, error) {
	d.listens.Add(1)
	return net.ListenUDP("udp", nil)
}

func (d *countingPacketDialer) Context() context.Context { return context.Background() }

func (d *countingPacketDialer) RemoteAddr(host string) (net.Addr, error) {
	return net.ResolveUDPAddr("udp", host)
}

func selfSignedCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "hysteria"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// serveEchoHysteria accepts every client and echoes each TCP request stream.
func serveEchoHysteria(t *testing.T) string {
	t.Helper()
	listener, err := quic.ListenAddr("127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{selfSignedCertificate(t)},
		NextProtos:   []string{"hysteria"},
	}, &quic.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept(context.Background())
			if err != nil {
				return
			}
			go func() {
				control, err := conn.AcceptStream(context.Background())
				if err != nil {
					return
				}
				if _, err := ReadClientHello(control); err != nil {
					return
				}
				if err := WriteServerHello(control, ServerHello{OK: true}); err != nil {
					return
				}
				for {
					stream, err := conn.AcceptStream(context.Background())
					if err != nil {
						return
					}
					go func() {
						defer stream.Close()
						if _, err := ReadClientRequest(stream); err != nil {
							return
						}
						if err := WriteServerResponse(stream, ServerResponse{OK: true}); err != nil {
							return
						}
						_, _ = io.Copy(stream, stream)
					}()
				}
			}()
		}
	}()
	return listener.Addr().String()
}

func TestResetNetworkConnectsAgain(t *testing.T) {
	server := serveEchoHysteria(t)
	client, err := NewClient(server, "", "udp", []byte("auth"),
		&tls.Config{InsecureSkipVerify: true, NextProtos: []string{"hysteria"}}, &quic.Config{},
		&transport.ClientTransport{}, 1<<20, 1<<20, nil, nil, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	dialer := &countingPacketDialer{}

	echo := func() {
		t.Helper()
		conn, err := client.DialTCP("192.0.2.1", 80, dialer)
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
	if got := dialer.listens.Load(); got != 1 {
		t.Fatalf("opened %d sessions before the reset, want one reused", got)
	}
	client.ResetNetwork()
	echo()
	if got := dialer.listens.Load(); got != 2 {
		t.Fatalf("opened %d sessions after the reset, want a new one", got)
	}
}
