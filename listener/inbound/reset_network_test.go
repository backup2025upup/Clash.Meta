package inbound_test

import (
	"context"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"

	"github.com/metacubex/mihomo/adapter/outbound"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/listener/inbound"

	"github.com/stretchr/testify/assert"
)

type countingDialer struct {
	C.Dialer
	dials atomic.Int64
}

func (d *countingDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	d.dials.Add(1)
	return d.Dialer.DialContext(ctx, network, address)
}

func (d *countingDialer) ListenPacket(ctx context.Context, network, address string, rAddrPort netip.AddrPort) (net.PacketConn, error) {
	d.dials.Add(1)
	return d.Dialer.ListenPacket(ctx, network, address, rAddrPort)
}

type networkResetter interface {
	ResetNetwork()
}

// testResetNetwork checks that a second run reuses the shared connection, and
// that a run after ResetNetwork works over a new one.
func testResetNetwork(t *testing.T, in C.InboundListener, newOutbound func(addrPort netip.AddrPort, dialer C.Dialer, tunnel *TestTunnel) (outbound.ProxyAdapter, error)) {
	tunnel := NewHttpTestTunnel()
	defer tunnel.Close()

	if !assert.NoError(t, in.Listen(tunnel)) {
		return
	}
	defer in.Close()

	addrPort, err := netip.ParseAddrPort(in.Address())
	if !assert.NoError(t, err) {
		return
	}

	dialer := &countingDialer{Dialer: tunnel.NewDialer()}
	out, err := newOutbound(addrPort, dialer, tunnel)
	if !assert.NoError(t, err) {
		return
	}
	defer out.Close()

	resetter, ok := out.(networkResetter)
	if !assert.True(t, ok, "%T has no ResetNetwork", out) {
		return
	}

	tunnel.DoSequentialTest(t, out)
	opened := dialer.dials.Load()
	tunnel.DoSequentialTest(t, out)
	assert.Equal(t, opened, dialer.dials.Load(), "the shared connection was not reused")

	resetter.ResetNetwork()
	tunnel.DoSequentialTest(t, out)
	assert.Greater(t, dialer.dials.Load(), opened, "ResetNetwork kept the shared connection")
}

func newResetTrojanInbound(t *testing.T, grpc bool) C.InboundListener {
	options := inbound.TrojanOption{
		BaseOption:  inbound.BaseOption{NameStr: "trojan_inbound", Listen: "127.0.0.1", Port: "0"},
		Users:       []inbound.TrojanUser{{Username: "test", Password: userUUID}},
		Certificate: tlsCertificate,
		PrivateKey:  tlsPrivateKey,
	}
	if grpc {
		options.GrpcServiceName = "GunService"
	}
	in, err := inbound.NewTrojan(&options)
	if err != nil {
		t.Fatal(err)
	}
	return in
}

func newResetTrojanOutbound(network string) func(netip.AddrPort, C.Dialer, *TestTunnel) (*outbound.Trojan, error) {
	return func(addrPort netip.AddrPort, dialer C.Dialer, tunnel *TestTunnel) (*outbound.Trojan, error) {
		options := outbound.TrojanOption{
			Name:        "trojan_outbound",
			Server:      addrPort.Addr().String(),
			Port:        int(addrPort.Port()),
			Password:    userUUID,
			Fingerprint: tlsFingerprint,
			Network:     network,
		}
		options.DialerForAPI = dialer
		options.TunnelForAPI = tunnel
		if network == "grpc" {
			options.GrpcOpts = outbound.GrpcOptions{GrpcServiceName: "GunService"}
		}
		return outbound.NewTrojan(options)
	}
}

func TestResetNetworkTrojanGrpc(t *testing.T) {
	testResetNetwork(t, newResetTrojanInbound(t, true), func(addrPort netip.AddrPort, dialer C.Dialer, tunnel *TestTunnel) (outbound.ProxyAdapter, error) {
		return newResetTrojanOutbound("grpc")(addrPort, dialer, tunnel)
	})
}

func TestResetNetworkSingMux(t *testing.T) {
	for _, protocol := range singMuxProtocolList {
		t.Run(protocol, func(t *testing.T) {
			testResetNetwork(t, newResetTrojanInbound(t, false), func(addrPort netip.AddrPort, dialer C.Dialer, tunnel *TestTunnel) (outbound.ProxyAdapter, error) {
				out, err := newResetTrojanOutbound("")(addrPort, dialer, tunnel)
				if err != nil {
					return nil, err
				}
				return outbound.NewSingMux(outbound.SingMuxOption{Enabled: true, Protocol: protocol}, out)
			})
		})
	}
}

func TestResetNetworkAnyTLS(t *testing.T) {
	in, err := inbound.NewAnyTLS(&inbound.AnyTLSOption{
		BaseOption:  inbound.BaseOption{NameStr: "anytls_inbound", Listen: "127.0.0.1", Port: "0"},
		Users:       map[string]string{"test": userUUID},
		Certificate: tlsCertificate,
		PrivateKey:  tlsPrivateKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	testResetNetwork(t, in, func(addrPort netip.AddrPort, dialer C.Dialer, tunnel *TestTunnel) (outbound.ProxyAdapter, error) {
		options := outbound.AnyTLSOption{
			Name:        "anytls_outbound",
			Server:      addrPort.Addr().String(),
			Port:        int(addrPort.Port()),
			Password:    userUUID,
			Fingerprint: tlsFingerprint,
		}
		options.DialerForAPI = dialer
		options.TunnelForAPI = tunnel
		return outbound.NewAnyTLS(options)
	})
}

func TestResetNetworkTuic(t *testing.T) {
	in, err := inbound.NewTuic(&inbound.TuicOption{
		BaseOption:            inbound.BaseOption{NameStr: "tuic_inbound", Listen: "127.0.0.1", Port: "0"},
		Users:                 map[string]string{userUUID: userUUID},
		Certificate:           tlsCertificate,
		PrivateKey:            tlsPrivateKey,
		AuthenticationTimeout: 5000,
	})
	if err != nil {
		t.Fatal(err)
	}
	testResetNetwork(t, in, func(addrPort netip.AddrPort, dialer C.Dialer, tunnel *TestTunnel) (outbound.ProxyAdapter, error) {
		options := outbound.TuicOption{
			Name:        "tuic_outbound",
			Server:      addrPort.Addr().String(),
			Port:        int(addrPort.Port()),
			UUID:        userUUID,
			Password:    userUUID,
			Fingerprint: tlsFingerprint,
		}
		options.DialerForAPI = dialer
		options.TunnelForAPI = tunnel
		return outbound.NewTuic(options)
	})
}

func TestResetNetworkHysteria2(t *testing.T) {
	in, err := inbound.NewHysteria2(&inbound.Hysteria2Option{
		BaseOption:  inbound.BaseOption{NameStr: "hysteria2_inbound", Listen: "127.0.0.1", Port: "0"},
		Users:       map[string]string{"test": userUUID},
		Certificate: tlsCertificate,
		PrivateKey:  tlsPrivateKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	testResetNetwork(t, in, func(addrPort netip.AddrPort, dialer C.Dialer, tunnel *TestTunnel) (outbound.ProxyAdapter, error) {
		options := outbound.Hysteria2Option{
			Name:        "hysteria2_outbound",
			Server:      addrPort.Addr().String(),
			Port:        int(addrPort.Port()),
			Password:    userUUID,
			Fingerprint: tlsFingerprint,
		}
		options.DialerForAPI = dialer
		options.TunnelForAPI = tunnel
		return outbound.NewHysteria2(options)
	})
}
