package transport

import (
	"testing"
	"time"
)

func TestPickActiveRelay(t *testing.T) {
	now := time.Now()
	a := &relayConnection{id: "a", state: relayStateOpen}
	b := &relayConnection{id: "b", state: relayStateOpen}
	c := &relayConnection{id: "c", state: relayStateClosed}
	if pickActive([]*relayConnection{a, b}, now) != nil {
		t.Fatal("sem mídia recebida não há relay ativo")
	}
	a.lastRecv.Store(now.Add(-2 * time.Second).UnixNano())
	b.lastRecv.Store(now.Add(-100 * time.Millisecond).UnixNano())
	c.lastRecv.Store(now.UnixNano())
	if got := pickActive([]*relayConnection{a, b, c}, now); got != b {
		t.Fatalf("esperava b (mais recente e aberto), got %v", got)
	}
	b.lastRecv.Store(now.Add(-10 * time.Second).UnixNano())
	a.lastRecv.Store(now.Add(-10 * time.Second).UnixNano())
	if pickActive([]*relayConnection{a, b}, now) != nil {
		t.Fatal("relays velhos demais não são ativos")
	}
}
