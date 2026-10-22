package media

import "encoding/binary"

// REMB (Receiver Estimated Maximum Bitrate, draft-alvestrand-rmcat-remb): PSFB
// (PT 206) FMT 15 com o identificador "REMB". O celular do WhatsApp manda REMB
// no 1:1 (visto em produção, 28 bytes = 2 SSRCs): é o retorno de banda que o
// emissor dele espera. Sem REMB nosso, o vídeo do celular desce ao mínimo.

const rembFmt = 15

// BuildRemb monta um REMB de senderSsrc anunciando bitrate (bps) para ssrcs.
func BuildRemb(senderSsrc uint32, bitrate uint64, ssrcs []uint32) []byte {
	if len(ssrcs) > 255 {
		ssrcs = ssrcs[:255]
	}
	exp, mant := uint32(0), bitrate
	for mant > 0x3ffff {
		mant >>= 1
		exp++
	}
	buf := make([]byte, 20+4*len(ssrcs))
	buf[0] = 0x80 | rembFmt
	buf[1] = 206
	binary.BigEndian.PutUint16(buf[2:4], uint16(len(buf)/4-1))
	binary.BigEndian.PutUint32(buf[4:8], senderSsrc)
	// media SSRC = 0 (o REMB vale para os SSRCs da lista)
	copy(buf[12:16], "REMB")
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(ssrcs))<<24|exp<<18|uint32(mant))
	for i, s := range ssrcs {
		binary.BigEndian.PutUint32(buf[20+4*i:], s)
	}
	return buf
}

// Remb é um REMB lido de um composto RTCP.
type Remb struct {
	Sender  uint32
	Bitrate uint64
	Ssrcs   []uint32
}

// ParseRembs devolve os REMBs de um composto RTCP decifrado (o bit 0x10 de perfil
// do WhatsApp no primeiro byte é ignorado).
func ParseRembs(plain []byte) []Remb {
	var out []Remb
	for off := 0; off+4 <= len(plain); {
		l := (int(binary.BigEndian.Uint16(plain[off+2:off+4])) + 1) * 4
		if off+l > len(plain) {
			break
		}
		p := plain[off : off+l]
		if p[1] == 206 && p[0]&0x0f == rembFmt && l >= 20 && string(p[12:16]) == "REMB" {
			w := binary.BigEndian.Uint32(p[16:20])
			n := int(w >> 24)
			r := Remb{Sender: binary.BigEndian.Uint32(p[4:8]), Bitrate: uint64(w&0x3ffff) << ((w >> 18) & 0x3f)}
			for i := 0; i < n && 20+4*i+4 <= l; i++ {
				r.Ssrcs = append(r.Ssrcs, binary.BigEndian.Uint32(p[20+4*i:]))
			}
			out = append(out, r)
		}
		off += l
	}
	return out
}
