package media

import "encoding/binary"

// SR no formato nativo do WhatsApp 1:1 (fonte: github.com/oxidezap/whatsapp-rust,
// wacore/src/voip/rtcp.rs, encode_whatsapp_reception_report e
// build_whatsapp_sender_report_with_sdes, PR #1761): SR com RC=1 cujo bloco de
// recepção RFC 3550 (24 B) vem seguido de 24 B próprios, palavras u32 big-endian:
// w0 = bytes de payload recebidos no stream, w5 = taxa de recepção em bit/s,
// w1..w4 desconhecidas (0). Lá, com a extensão zerada, um iPhone baixava o vídeo.
// O SR que o celular nos manda tem 76 B (28 + 24 + 24): o mesmo formato.

// WhatsappReceptionExt monta os 24 B da extensão do bloco de recepção.
//
// w1: o celular manda 106000..149491 nos SR dele, o MESMO valor no bloco do áudio e
// no do vídeo (produção, 23/10/2026). SUPOSIÇÃO: é a estimativa de banda de quem
// recebe; o whatsapp-rust manda 0 (ver WhatsappReceptionExtW1).
func WhatsappReceptionExt(bytesReceived, rateBps uint32) [24]byte {
	return WhatsappReceptionExtW1(bytesReceived, 0, rateBps)
}

// WhatsappReceptionExtW1 é WhatsappReceptionExt com a palavra 1 preenchida.
func WhatsappReceptionExtW1(bytesReceived, w1, rateBps uint32) [24]byte {
	var e [24]byte
	binary.BigEndian.PutUint32(e[0:4], bytesReceived)
	binary.BigEndian.PutUint32(e[4:8], w1)
	binary.BigEndian.PutUint32(e[20:24], rateBps)
	return e
}

// BuildWhatsappSenderReport monta SR (RC=1, 76 B) + SDES: o bloco sobre o stream do
// peer e a extensão do WhatsApp. profile liga o bit 0x10 (stream de vídeo).
func BuildWhatsappSenderReport(localSsrc uint32, stats *RtcpSenderStats, nowMs uint64, block [24]byte, ext [24]byte,
	cname *[WhatsappRtcpCnameLen]byte, profile bool) []byte {
	sr := BuildSenderReport(localSsrc, stats, nowMs)
	out := make([]byte, 0, 76+32)
	out = append(out, sr[:]...)
	out[0] = 0x81
	if profile {
		out[0] |= 0x10
	}
	binary.BigEndian.PutUint16(out[2:4], 18) // (18+1)*4 = 76
	out = append(out, block[:]...)
	out = append(out, ext[:]...)
	sdes := BuildSourceDescription(localSsrc, cname, profile)
	return append(out, sdes[:]...)
}

// PeerReception é o bloco de recepção (com a extensão do WhatsApp) de um SR do peer.
type PeerReception struct {
	Sender, About   uint32
	FractionLost    uint8
	CumLost, Jitter uint32
	Ext             [6]uint32
	HasExt          bool
}

// ParsePeerReceptions lê os blocos de recepção dos SR de um composto decifrado.
func ParsePeerReceptions(plain []byte) []PeerReception {
	var out []PeerReception
	for off := 0; off+4 <= len(plain); {
		l := (int(binary.BigEndian.Uint16(plain[off+2:off+4])) + 1) * 4
		if l < 8 || off+l > len(plain) {
			break
		}
		p := plain[off : off+l]
		if p[1] == RtcpPtSr && p[0]&0x0f >= 1 && l >= 52 {
			r := PeerReception{
				Sender:       binary.BigEndian.Uint32(p[4:8]),
				About:        binary.BigEndian.Uint32(p[28:32]),
				FractionLost: p[32],
				CumLost:      uint32(p[33])<<16 | uint32(p[34])<<8 | uint32(p[35]),
				Jitter:       binary.BigEndian.Uint32(p[40:44]),
			}
			if l >= 76 {
				r.HasExt = true
				for i := 0; i < 6; i++ {
					r.Ext[i] = binary.BigEndian.Uint32(p[52+4*i:])
				}
			}
			out = append(out, r)
		}
		off += l
	}
	return out
}
