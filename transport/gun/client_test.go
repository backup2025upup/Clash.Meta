package gun

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
)

func newTestClient(made *int) *Client {
	dial := func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("no network in this test")
	}
	return NewClient(func() *Transport {
		*made++
		return NewTransport(dial, nil, &Config{})
	}, 0, 0, 0)
}

func TestClientStaysUsableAfterClose(t *testing.T) {
	made := 0
	client := newTestClient(&made)
	first := client.getTransport()
	if again := client.getTransport(); again != first || made != 1 {
		t.Fatalf("an idle transport was not reused: made %d", made)
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if first.ctx.Err() == nil {
		t.Fatal("Close left the old transport open")
	}

	if next := client.getTransport(); next == first || made != 2 {
		t.Fatalf("Close left the client without a new transport: made %d", made)
	}
}

func TestClientCloseRacesTransportPicks(t *testing.T) {
	made := 0
	client := newTestClient(&made)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				transport := client.getTransport()
				transport.count.Add(1)
				transport.count.Add(-1)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = client.Close()
			}
		}()
	}
	wg.Wait()
}
