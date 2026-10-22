package signaling

import (
	"testing"

	"go.mau.fi/whatsmeow/types"
)

func preacceptCapability(t *testing.T, video bool) []byte {
	n := BuildPreacceptStanzaFor(types.NewJID("1", types.DefaultUserServer), "C", types.NewJID("2", types.DefaultUserServer), video)
	c := findChild(findChild(&n, "preaccept"), "capability")
	b, _ := c.Content.([]byte)
	return b
}

func TestPreacceptCapabilityAB(t *testing.T) {
	if got := preacceptCapability(t, true); got[5] != 0xbb {
		t.Fatalf("desligado: capability de áudio esperada, got %x", got)
	}
	t.Setenv("WACALLS_1A1_SIGNALING_AB", "1")
	if got := preacceptCapability(t, true); got[5] != 0xfa {
		t.Fatalf("ligado + vídeo: capability de vídeo esperada, got %x", got)
	}
	if got := preacceptCapability(t, false); got[5] != 0xbb {
		t.Fatalf("ligado + áudio: capability de áudio esperada, got %x", got)
	}
}

func TestVideoDecForAcceptAB(t *testing.T) {
	if VideoDecForAccept() != "H264,AV1" {
		t.Fatal("padrão deve manter o valor legado")
	}
	t.Setenv("WACALLS_1A1_SIGNALING_AB", "true")
	if VideoDecForAccept() != "H264" {
		t.Fatal("AB deve anunciar só H264")
	}
}
