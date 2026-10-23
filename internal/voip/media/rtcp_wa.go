package media

import "encoding/binary"

// Feedback RTCP que o celular do WhatsApp manda no 1:1 (visto em produção em
// 22/10/2026, decifrado com as chaves SRTCP da chamada):
//
//   - PSFB (206) FMT 1 com o bit de perfil 0x10: PLI pedindo keyframe do nosso vídeo;
//   - RTPFB (205) FMT 1 com o bit 0x10: NACK genérico (RFC 4585), pedindo pacotes
//     perdidos do nosso vídeo, ex. 91cd0003 56d99331 e64ecb71 b25b000f;
//   - PSFB (206) FMT 15 (sem o bit) com o identificador "PRST" no lugar de "REMB":
//     8fce0006 2ff9b888 00000000 50525354 0008 0a04088080052000 0000, ou seja um
//     tamanho de 16 bits e um protobuf {1: {1: 81920}, 4: 0}. É wa.voip.PeerFeedback
//     (motor VoIP do WhatsApp Web): o campo 1.1 é video_sender_stats.sender_bwe, a
//     estimativa do PRÓPRIO envio do celular (81920/35000 = ele no mínimo), e não
//     uma banda para o nosso envio. Mandar PRST nosso não ajuda.

// ParseNacks devolve as sequências pedidas em NACKs (205/1, com ou sem o bit 0x10)
// para o mediaSSRC.
func ParseNacks(plain []byte, mediaSSRC uint32) []uint16 {
	var out []uint16
	for off := 0; off+4 <= len(plain); {
		l := (int(binary.BigEndian.Uint16(plain[off+2:off+4])) + 1) * 4
		if off+l > len(plain) {
			break
		}
		p := plain[off : off+l]
		if p[1] == 205 && p[0]&0x0f == 1 && l >= 16 && binary.BigEndian.Uint32(p[8:12]) == mediaSSRC {
			for i := 12; i+4 <= l; i += 4 {
				pid := binary.BigEndian.Uint16(p[i : i+2])
				blp := binary.BigEndian.Uint16(p[i+2 : i+4])
				out = append(out, pid)
				for b := uint16(0); b < 16; b++ {
					if blp&(1<<b) != 0 {
						out = append(out, pid+b+1)
					}
				}
			}
		}
		off += l
	}
	return out
}

// ParsePrst devolve o campo 1.1 (bps, suposição) de cada PRST do composto.
func ParsePrst(plain []byte) []uint64 {
	var out []uint64
	for off := 0; off+4 <= len(plain); {
		l := (int(binary.BigEndian.Uint16(plain[off+2:off+4])) + 1) * 4
		if off+l > len(plain) {
			break
		}
		p := plain[off : off+l]
		if p[1] == 206 && p[0]&0x0f == 15 && l >= 18 && string(p[12:16]) == "PRST" {
			n := int(binary.BigEndian.Uint16(p[16:18]))
			if 18+n <= l {
				if v, ok := prstBitrate(p[18 : 18+n]); ok {
					out = append(out, v)
				}
			}
		}
		off += l
	}
	return out
}

// BuildPrst monta um PRST de senderSsrc com o mesmo formato do celular, trocando a
// banda por bps.
func BuildPrst(senderSsrc uint32, bps uint64) []byte {
	inner := append([]byte{0x08}, uvarint(bps)...)
	pb := append([]byte{0x0a, byte(len(inner))}, inner...)
	pb = append(pb, 0x20, 0x00)
	body := append([]byte{byte(len(pb) >> 8), byte(len(pb))}, pb...)
	n := 16 + len(body)
	for n%4 != 0 {
		n++
	}
	buf := make([]byte, n)
	buf[0] = 0x80 | 15
	buf[1] = 206
	binary.BigEndian.PutUint16(buf[2:4], uint16(n/4-1))
	binary.BigEndian.PutUint32(buf[4:8], senderSsrc)
	copy(buf[12:16], "PRST")
	copy(buf[16:], body)
	return buf
}

func uvarint(v uint64) []byte {
	var b []byte
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

func readUvarint(b []byte) (uint64, int) {
	var v uint64
	for i := 0; i < len(b) && i < 10; i++ {
		v |= uint64(b[i]&0x7f) << (7 * i)
		if b[i] < 0x80 {
			return v, i + 1
		}
	}
	return 0, 0
}

// prstBitrate lê o campo 1 (mensagem) -> campo 1 (varint) do protobuf do PRST.
func prstBitrate(pb []byte) (uint64, bool) {
	for i := 0; i < len(pb); {
		key, n := readUvarint(pb[i:])
		if n == 0 {
			return 0, false
		}
		i += n
		field, wire := key>>3, key&7
		switch wire {
		case 0:
			_, n = readUvarint(pb[i:])
			if n == 0 {
				return 0, false
			}
			i += n
		case 2:
			ln, n := readUvarint(pb[i:])
			if n == 0 || i+n+int(ln) > len(pb) {
				return 0, false
			}
			i += n
			if field == 1 {
				sub := pb[i : i+int(ln)]
				if len(sub) > 0 && sub[0] == 0x08 {
					if v, m := readUvarint(sub[1:]); m > 0 {
						return v, true
					}
				}
			}
			i += int(ln)
		default:
			return 0, false
		}
	}
	return 0, false
}
