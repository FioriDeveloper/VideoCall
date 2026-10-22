package media

import "encoding/binary"

// Receiver Report (RFC 3550 §6.4.2, PT 201) e o contador de recepção que o alimenta
// (RFC 3550 A.1/A.3: perda cumulativa, fração de perda e jitter de chegada).

const RtcpPtRr uint8 = 201

// RtcpReportBlock é um bloco de relatório (24 bytes) sobre um SSRC recebido.
type RtcpReportBlock struct {
	SSRC           uint32
	FractionLost   uint8
	CumulativeLost uint32 // 24 bits
	HighestSeq     uint32 // ciclos<<16 | seq máximo
	Jitter         uint32 // unidades do clock RTP
	LSR            uint32 // meio do NTP do último SR recebido (0 = nenhum)
	DLSR           uint32 // atraso desde o último SR, em 1/65536 s
}

// BuildReceiverReport monta um RR (PT 201) com até 31 blocos.
func BuildReceiverReport(localSsrc uint32, blocks []RtcpReportBlock, profileExtension bool) []byte {
	if len(blocks) > 31 {
		blocks = blocks[:31]
	}
	out := make([]byte, 8+24*len(blocks))
	out[0] = 0x80 | byte(len(blocks))
	if profileExtension {
		out[0] |= 0x10
	}
	out[1] = RtcpPtRr
	binary.BigEndian.PutUint16(out[2:4], uint16(1+6*len(blocks)))
	binary.BigEndian.PutUint32(out[4:8], localSsrc)
	for i, b := range blocks {
		o := out[8+24*i:]
		binary.BigEndian.PutUint32(o[0:4], b.SSRC)
		o[4] = b.FractionLost
		o[5] = byte(b.CumulativeLost >> 16)
		o[6] = byte(b.CumulativeLost >> 8)
		o[7] = byte(b.CumulativeLost)
		binary.BigEndian.PutUint32(o[8:12], b.HighestSeq)
		binary.BigEndian.PutUint32(o[12:16], b.Jitter)
		binary.BigEndian.PutUint32(o[16:20], b.LSR)
		binary.BigEndian.PutUint32(o[20:24], b.DLSR)
	}
	return out
}

// RtpReceiverStats acumula estatísticas de recepção de UM SSRC. Não é thread-safe
// (o chamador serializa).
type RtpReceiverStats struct {
	ClockRate uint32

	init        bool
	baseSeq     uint16
	maxSeq      uint16
	cycles      uint32
	received    uint32
	expectedPri uint32
	receivedPri uint32

	transit    int64
	jitter     float64
	hasTransit bool
}

// Update registra a chegada de um pacote (seq, timestamp RTP, chegada em ms).
// Chame uma vez por pacote ÚNICO (depois do descarte de duplicados).
func (s *RtpReceiverStats) Update(seq uint16, rtpTs uint32, arrivalMs int64) {
	if !s.init {
		s.init = true
		s.baseSeq, s.maxSeq = seq, seq
	} else if d := seq - s.maxSeq; d < 0x8000 && d != 0 { // avançou (com wrap)
		if seq < s.maxSeq {
			s.cycles += 1 << 16
		}
		s.maxSeq = seq
	}
	s.received++
	if s.ClockRate == 0 {
		return
	}
	arrival := arrivalMs * int64(s.ClockRate) / 1000
	transit := arrival - int64(rtpTs)
	if s.hasTransit {
		d := transit - s.transit
		if d < 0 {
			d = -d
		}
		s.jitter += (float64(d) - s.jitter) / 16
	}
	s.transit, s.hasTransit = transit, true
}

// Received devolve quantos pacotes únicos foram vistos.
func (s *RtpReceiverStats) Received() uint32 { return s.received }

// Block monta o bloco de relatório do período desde a chamada anterior (a fração de
// perda é por intervalo, como no RFC 3550 A.3).
func (s *RtpReceiverStats) Block(ssrc uint32) RtcpReportBlock {
	ext := s.cycles + uint32(s.maxSeq)
	expected := ext - uint32(s.baseSeq) + 1
	var lost uint32
	if expected > s.received {
		lost = expected - s.received
	}
	if lost > 0xffffff {
		lost = 0xffffff
	}
	expInt := expected - s.expectedPri
	recInt := s.received - s.receivedPri
	s.expectedPri, s.receivedPri = expected, s.received
	var frac uint8
	if expInt > 0 && expInt > recInt {
		frac = uint8((uint64(expInt-recInt) << 8) / uint64(expInt))
	}
	return RtcpReportBlock{
		SSRC: ssrc, FractionLost: frac, CumulativeLost: lost,
		HighestSeq: ext, Jitter: uint32(s.jitter),
	}
}
