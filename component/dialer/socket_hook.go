package dialer

import (
	"context"
	"net"
	"syscall"
)

// SocketControl
// never change type traits because it's used in CMFA
type SocketControl func(network, address string, conn syscall.RawConn) error

// DefaultSocketHook
// never change type traits because it's used in CMFA
var DefaultSocketHook SocketControl

// DefaultSocketObserver sees each socket opened while DefaultSocketHook is nil,
// after interface-name and routing-mark are applied, and keeps them in force.
var DefaultSocketObserver SocketControl

func socketObserverControl(observer SocketControl) controlFn {
	return func(ctx context.Context, network, address string, c syscall.RawConn) error {
		return observer(network, address, c)
	}
}

func socketHookToToDialer(dialer *net.Dialer) {
	addControlToDialer(dialer, func(ctx context.Context, network, address string, c syscall.RawConn) error {
		return DefaultSocketHook(network, address, c)
	})
}

func socketHookToListenConfig(lc *net.ListenConfig) {
	addControlToListenConfig(lc, func(ctx context.Context, network, address string, c syscall.RawConn) error {
		return DefaultSocketHook(network, address, c)
	})
}
