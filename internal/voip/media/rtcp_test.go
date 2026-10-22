package media

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"
	"time"
)

// Vetores do meowcaller (rtp/testdata/kats.json).
func TestBuildSenderReportMatchesKAT(t *testing.T) {
	stats := RtcpSenderStats{PacketsSent: 5, OctetsSent: 600, RtpTimestamp: 1600}
	got := BuildSenderReport(305419896, &stats, 1718000000000)
	want, _ := hex.DecodeString("80c8000612345678ea11180000000000000006400000000500000258")
	if !bytes.Equal(got[:], want) {
		t.Fatalf("sender report = %x, want %x", got[:], want)
	}
}

func TestSrtcpRoundTripAndAuth(t *testing.T) {
	epoch, _ := hex.DecodeString("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	keys, err := DeriveGroupSrtcpKeys(epoch, "111111111111111:0@lid")
	if err != nil {
		t.Fatal(err)
	}
	// Labels distintos dos da SRTP: a chave de cifra SRTCP não pode coincidir com a
	// chave de cifra SRTP do mesmo participante.
	srtp, _ := DeriveGroupSrtpKeying(epoch, "111111111111111:0@lid")
	ctx, _ := NewSrtpContext(srtp, 4)
	if bytes.Equal(ctx.sessionKey, keys.CipherKey[:]) {
		t.Fatal("SRTCP keys must use distinct KDF labels")
	}
	ssrc := uint32(305419896)
	stats := RtcpSenderStats{PacketsSent: 5, OctetsSent: 600, RtpTimestamp: 1600}
	var cname [WhatsappRtcpCnameLen]byte
	copy(cname[:], "abcde@pj123456.org")
	plain := BuildSenderReportWithSdes(ssrc, &stats, 1718000000000, &cname, true)
	protected, err := ProtectSrtcp(&keys, ssrc, 1, plain)
	if err != nil {
		t.Fatal(err)
	}
	if len(protected) != len(plain)+SrtcpTrailerLen {
		t.Fatalf("protected len %d want %d", len(protected), len(plain)+SrtcpTrailerLen)
	}
	recovered, index, ok := UnprotectSrtcp(&keys, ssrc, protected)
	if !ok || index != 1 || !bytes.Equal(recovered, plain) {
		t.Fatalf("unprotect: ok=%v index=%d equal=%v", ok, index, bytes.Equal(recovered, plain))
	}
	protected[len(protected)-1] ^= 0x01
	if _, _, ok := UnprotectSrtcp(&keys, ssrc, protected); ok {
		t.Fatal("forged SRTCP tag accepted")
	}
}

func TestPliRoundTripDetected(t *testing.T) {
	epoch := make([]byte, 32)
	for i := range epoch {
		epoch[i] = byte(i)
	}
	sender, err := NewSrtcpSender(epoch, "111111111111111:0@lid", 0x11223344, true)
	if err != nil {
		t.Fatal(err)
	}
	pkt, err := sender.PictureLossIndication(0xAABBCCDD)
	if err != nil {
		t.Fatal(err)
	}
	if !IsRtcpPacket(pkt) {
		t.Fatal("PLI not classified as RTCP")
	}
	if s, ok := ParseRtcpSenderSsrc(pkt); !ok || s != 0x11223344 {
		t.Fatalf("sender ssrc = %x ok=%v", s, ok)
	}
	keys := sender.Keying()
	plain, _, ok := UnprotectSrtcp(&keys, 0x11223344, pkt)
	if !ok {
		t.Fatal("unprotect PLI failed")
	}
	if !RtcpRequestsKeyframe(plain, 0xAABBCCDD) {
		t.Fatal("PLI for our video SSRC not detected")
	}
	if RtcpRequestsKeyframe(plain, 0x01020304) {
		t.Fatal("PLI matched wrong SSRC")
	}
	sr, _ := sender.SenderReport(RtcpSenderStats{}, 1718000000000)
	plainSR, _, ok := UnprotectSrtcp(&keys, 0x11223344, sr)
	if !ok || RtcpRequestsKeyframe(plainSR, 0xAABBCCDD) {
		t.Fatal("SR+SDES must unprotect and not look like a keyframe request")
	}
}

func TestSrtcpConfigurableTagLen(t *testing.T) {
	epoch := make([]byte, 32)
	keys, _ := DeriveGroupSrtcpKeys(epoch, "1:0@s.whatsapp.net")
	pli := BuildPictureLossIndication(0x11223344, 0x55667788, false)
	for _, tag := range []int{4, 10} {
		prot, err := ProtectSrtcpTag(&keys, 0x11223344, 7, pli[:], tag)
		if err != nil || len(prot) != len(pli)+4+tag {
			t.Fatalf("tag %d: len=%d err=%v", tag, len(prot), err)
		}
		plain, idx, ok := UnprotectSrtcpTag(&keys, 0x11223344, prot, tag)
		if !ok || idx != 7 || !bytes.Equal(plain, pli[:]) {
			t.Fatalf("tag %d: roundtrip falhou", tag)
		}
		other := 14 - tag
		if _, _, ok := UnprotectSrtcpTag(&keys, 0x11223344, prot, other); ok {
			t.Fatalf("tag %d aceito como %d", tag, other)
		}
	}
}

func TestPliBytes(t *testing.T) {
	got := BuildPictureLossIndication(0x11223344, 0x55667788, false)
	want, _ := hex.DecodeString("81ce000211223344" + "55667788")
	if !bytes.Equal(got[:], want) {
		t.Fatalf("PLI = %x, want %x", got[:], want)
	}
}

func TestBuildReceiverReportBytes(t *testing.T) {
	var block [24]byte
	for i := range block {
		block[i] = byte(i + 1)
	}
	got := BuildReceiverReport(0xaabbccdd, block, false)
	if hex.EncodeToString(got[:8]) != "81c90007aabbccdd" || !bytes.Equal(got[8:], block[:]) {
		t.Fatalf("RR = %x", got[:])
	}
	if p := BuildReceiverReport(1, block, true); p[0] != 0x91 {
		t.Fatalf("bit de perfil: %x", p[0])
	}
}

func TestRtpRxStatsLossJitterDup(t *testing.T) {
	s := NewRtpRxStats(90000)
	t0 := time.Unix(1000, 0)
	// seq 100..109 com 103 e 104 perdidos, 105 duplicado, chegada a cada 20ms certinho
	for i, seq := range []uint16{100, 101, 102, 105, 105, 106, 107, 108, 109} {
		at := t0.Add(time.Duration(i) * 20 * time.Millisecond)
		s.Update(seq, uint32(i)*1800, at)
	}
	if s.Received() != 8 {
		t.Fatalf("received=%d (duplicado deveria ser ignorado)", s.Received())
	}
	b, ok := s.Report(0x1234, t0.Add(time.Second))
	if !ok {
		t.Fatal("sem relatório")
	}
	if binary.BigEndian.Uint32(b[0:4]) != 0x1234 {
		t.Fatal("ssrc")
	}
	if b[5] != 0 || b[6] != 0 || b[7] != 2 {
		t.Fatalf("perdidos acumulados = %x, want 2", b[5:8])
	}
	if b[4] != uint8((2<<8)/10) {
		t.Fatalf("fração = %d", b[4])
	}
	if binary.BigEndian.Uint32(b[8:12]) != 109 {
		t.Fatal("ext highest seq")
	}
	// 2º relatório sem perda nova: fração 0
	s.Update(110, 9*1800, t0.Add(9*20*time.Millisecond))
	b2, _ := s.Report(0x1234, t0.Add(2*time.Second))
	if b2[4] != 0 {
		t.Fatalf("fração do 2º intervalo = %d", b2[4])
	}
}

func TestRtpRxStatsWrapAndLSR(t *testing.T) {
	s := NewRtpRxStats(16000)
	t0 := time.Unix(0, 0)
	s.Update(65534, 0, t0)
	s.Update(65535, 960, t0)
	s.Update(0, 1920, t0)
	s.NoteSenderReport(0xdeadbeef, t0)
	b, _ := s.Report(7, t0.Add(500*time.Millisecond))
	if binary.BigEndian.Uint32(b[8:12]) != 1<<16 {
		t.Fatalf("ext seq = %x", b[8:12])
	}
	if binary.BigEndian.Uint32(b[16:20]) != 0xdeadbeef || binary.BigEndian.Uint32(b[20:24]) != 32768 {
		t.Fatalf("LSR/DLSR = %x", b[16:24])
	}
}

func TestParseSenderReports(t *testing.T) {
	sr := BuildSenderReport(0x01020304, &RtcpSenderStats{}, 1718000000000)
	got := ParseSenderReports(sr[:])
	if len(got) != 1 || got[0].SSRC != 0x01020304 || got[0].NtpMid != binary.BigEndian.Uint32(sr[10:14]) {
		t.Fatalf("got %+v", got)
	}
}
