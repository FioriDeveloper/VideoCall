package transport

import "testing"

func TestPickActiveRelay(t *testing.T) {
	a := &relayConnection{id: "a", state: relayStateOpen}
	b := &relayConnection{id: "b", state: relayStateOpen}
	c := &relayConnection{id: "c", state: relayStateConnecting}
	if got := pickActiveRelay([]*relayConnection{b, a, c}); got != a {
		t.Fatal("empate: menor id")
	}
	b.lastRx.Store(100)
	if got := pickActiveRelay([]*relayConnection{a, b, c}); got != b {
		t.Fatal("deveria preferir o que recebeu por último")
	}
	c.lastRx.Store(999)
	if got := pickActiveRelay([]*relayConnection{a, b, c}); got != b {
		t.Fatal("relay não aberto não conta")
	}
	if pickActiveRelay([]*relayConnection{c}) != nil {
		t.Fatal("sem relay aberto = nil")
	}
}
