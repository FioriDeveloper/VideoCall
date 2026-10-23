package media

import "testing"

// Estrutura do teste native_video_sender_report_matches_profile_structure do
// whatsapp-rust: 108 B, SR "91 c8 00 12", bloco sobre o peer em 28..32, extensão em
// 52..76, SDES "91 ca 00 07" em 76.
func TestWhatsappSenderReportStructure(t *testing.T) {
	var cname [WhatsappRtcpCnameLen]byte
	var block [24]byte
	block[0], block[1], block[2], block[3] = 0xaa, 0xbb, 0xcc, 0xdd
	b := BuildWhatsappSenderReport(0x01020304, &RtcpSenderStats{PacketsSent: 5}, 1_000, block,
		WhatsappReceptionExt(758055, 800_000), &cname, true)
	if len(b) != 108 {
		t.Fatalf("len %d", len(b))
	}
	if b[0] != 0x91 || b[1] != 0xc8 || b[2] != 0 || b[3] != 0x12 {
		t.Fatalf("cabeçalho SR %x", b[:4])
	}
	if b[28] != 0xaa || b[31] != 0xdd {
		t.Fatalf("bloco %x", b[28:32])
	}
	if b[76] != 0x91 || b[77] != 0xca || b[79] != 0x07 {
		t.Fatalf("SDES %x", b[76:80])
	}
	rs := ParsePeerReceptions(b)
	if len(rs) != 1 || !rs[0].HasExt || rs[0].Ext[0] != 758055 || rs[0].Ext[5] != 800_000 || rs[0].About != 0xaabbccdd {
		t.Fatalf("parse %+v", rs)
	}
	a := BuildWhatsappSenderReport(1, nil, 1_000, block, WhatsappReceptionExt(0, 0), &cname, false)
	if a[0] != 0x81 || a[76] != 0x81 {
		t.Fatalf("áudio sem o bit de perfil: %x %x", a[0], a[76])
	}
}
