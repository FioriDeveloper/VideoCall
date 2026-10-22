package signaling

import (
	"bytes"
	"testing"

	"go.mau.fi/whatsmeow/types"
)

func preacceptCap(t *testing.T, video bool) []byte {
	n := BuildPreacceptStanzaFor(types.JID{User: "1", Server: "s.whatsapp.net"}, "C", types.JID{User: "2", Server: "s.whatsapp.net"}, video)
	for _, c := range n.GetChildren()[0].GetChildren() {
		if c.Tag == "capability" {
			return c.Content.([]byte)
		}
	}
	t.Fatal("sem capability")
	return nil
}

func TestPreacceptCapabilityAB(t *testing.T) {
	if !bytes.Equal(preacceptCap(t, true), capabilityPreaccept) {
		t.Fatal("A/B desligado: capability tem de continuar a de áudio")
	}
	t.Setenv("WACALLS_1A1_SIGNALING_AB", "1")
	if !bytes.Equal(preacceptCap(t, true), capabilityVideoPreaccept) {
		t.Fatal("A/B ligado + vídeo: capability de vídeo")
	}
	if !bytes.Equal(preacceptCap(t, false), capabilityPreaccept) {
		t.Fatal("áudio nunca muda")
	}
}

func TestVideoDecForAccept(t *testing.T) {
	if VideoDecForAccept() != "H264,AV1" {
		t.Fatal("padrão preservado")
	}
	t.Setenv("WACALLS_1A1_SIGNALING_AB", "on")
	if VideoDecForAccept() != "H264" {
		t.Fatal("A/B sem AV1")
	}
}
