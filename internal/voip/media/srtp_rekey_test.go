package media

import (
	"testing"

	"wacalls/internal/voip/core"
)

// Rotação de chave preserva o estado de ROC/sequência (continuidade do stream).
func TestSrtpContextWithKeyingPreservesROC(t *testing.T) {
	km1 := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	km2 := core.SrtpKeyingMaterial{MasterKey: append([]byte{1}, make([]byte, 15)...), MasterSalt: make([]byte, 14)}
	c, err := NewSrtpContext(km1, 4)
	if err != nil {
		t.Fatal(err)
	}
	c.updateRoc(65000)
	c.updateRoc(10) // wrap -> roc 1
	if c.roc != 1 {
		t.Fatalf("roc = %d, want 1", c.roc)
	}
	n, err := c.WithKeying(km2)
	if err != nil {
		t.Fatal(err)
	}
	if n.roc != 1 || n.lastSeq != 10 || !n.initialized {
		t.Fatalf("state not preserved: roc=%d lastSeq=%d init=%v", n.roc, n.lastSeq, n.initialized)
	}
	if string(n.sessionKey) == string(c.sessionKey) {
		t.Fatal("session key did not change")
	}
}

// Pacotes fora de ordem na virada da sequência não podem corromper o ROC.
func TestUpdateRocReorderAcrossWrap(t *testing.T) {
	c := &SrtpContext{}
	steps := []struct {
		seq uint16
		roc uint32
	}{
		{65533, 0}, {65534, 0}, {0, 1}, {65535, 0}, {1, 1}, {2, 1}, {65535, 0}, {3, 1},
	}
	for i, s := range steps {
		if got := c.updateRoc(s.seq); got != s.roc {
			t.Fatalf("passo %d seq=%d: roc=%d, esperado %d", i, s.seq, got, s.roc)
		}
	}
}

// Dois SSRCs intercalados: o ROC de um não pode mexer no do outro.
func TestRecvRocIsPerSsrc(t *testing.T) {
	km := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	c, err := NewSrtpContext(km, 4)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.recvRocFor(1, 65000); got != 0 {
		t.Fatalf("ssrc 1 início: roc=%d", got)
	}
	if got := c.recvRocFor(2, 100); got != 0 {
		t.Fatalf("ssrc 2 início: roc=%d", got)
	}
	// Com estado único, o 100 do ssrc 2 depois do 65000 do ssrc 1 virava a volta.
	if got := c.recvRocFor(1, 65001); got != 0 {
		t.Fatalf("ssrc 1 seguiu: roc=%d, esperado 0", got)
	}
	if got := c.recvRocFor(1, 5); got != 1 {
		t.Fatalf("ssrc 1 virou a sequência: roc=%d, esperado 1", got)
	}
	if got := c.recvRocFor(2, 101); got != 0 {
		t.Fatalf("ssrc 2 não pode herdar a virada do 1: roc=%d", got)
	}
	n, err := c.WithKeying(km)
	if err != nil {
		t.Fatal(err)
	}
	if got := n.recvRocFor(1, 6); got != 1 {
		t.Fatalf("rechaveamento perdeu o ROC do ssrc 1: %d", got)
	}
}
