package media

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestBuildReceiverReportBytes(t *testing.T) {
	blk := RtcpReportBlock{SSRC: 0xAABBCCDD, FractionLost: 0x10, CumulativeLost: 0x000102,
		HighestSeq: 0x00010005, Jitter: 7, LSR: 0, DLSR: 0}
	got := BuildReceiverReport(0x12345678, []RtcpReportBlock{blk}, false)
	want, _ := hex.DecodeString("81c90007" + "12345678" +
		"aabbccdd" + "10000102" + "00010005" + "00000007" + "00000000" + "00000000")
	if !bytes.Equal(got, want) {
		t.Fatalf("rr = %x, want %x", got, want)
	}
	if g := BuildReceiverReport(1, nil, true); !bytes.Equal(g, []byte{0x90, 0xc9, 0, 1, 0, 0, 0, 1}) {
		t.Fatalf("rr vazio = %x", g)
	}
}

func TestPLIBytes(t *testing.T) {
	got := BuildPictureLossIndication(0x11223344, 0x55667788, false)
	want, _ := hex.DecodeString("81ce0002" + "11223344" + "55667788")
	if !bytes.Equal(got[:], want) {
		t.Fatalf("pli = %x", got[:])
	}
}

func TestReceiverStatsLossAndWrap(t *testing.T) {
	s := &RtpReceiverStats{ClockRate: 90000}
	// 65534, 65535, 0, (1 perdido), 2 => esperado 5, recebido 4
	for i, seq := range []uint16{65534, 65535, 0, 2} {
		s.Update(seq, uint32(i)*6000, int64(i)*66)
	}
	b := s.Block(9)
	if b.HighestSeq != 1<<16|2 {
		t.Fatalf("highest = %x", b.HighestSeq)
	}
	if b.CumulativeLost != 1 {
		t.Fatalf("lost = %d", b.CumulativeLost)
	}
	if b.FractionLost != uint8((1<<8)/5) {
		t.Fatalf("frac = %d", b.FractionLost)
	}
	// intervalo seguinte sem perda
	s.Update(3, 24000, 264)
	if b2 := s.Block(9); b2.FractionLost != 0 {
		t.Fatalf("frac2 = %d", b2.FractionLost)
	}
}

func TestSrtcpTagLen4RoundTrip(t *testing.T) {
	epoch := bytes.Repeat([]byte{7}, 32)
	keys, _ := DeriveGroupSrtcpKeys(epoch, "1@s.whatsapp.net:1")
	plain := BuildPictureLossIndication(1, 2, false)
	pkt, err := ProtectSrtcpTag(&keys, 1, 5, plain[:], 4)
	if err != nil || len(pkt) != 12+4+4 {
		t.Fatalf("len=%d err=%v", len(pkt), err)
	}
	if _, _, ok := UnprotectSrtcpTag(&keys, 1, pkt, 10); ok {
		t.Fatal("tag errado não pode autenticar")
	}
	out, idx, ok := UnprotectSrtcpTag(&keys, 1, pkt, 4)
	if !ok || idx != 5 || !bytes.Equal(out, plain[:]) {
		t.Fatalf("roundtrip falhou ok=%v idx=%d", ok, idx)
	}
}
