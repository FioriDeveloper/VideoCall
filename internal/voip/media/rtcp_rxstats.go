package media

import (
	"encoding/binary"
	"sync"
	"time"
)

// RtpRxStats acumula perda/jitter de UM stream RTP recebido (RFC 3550 A.1/A.8) para
// montar o bloco de relatório do Receiver Report. Duplicados (2 relays abertos) são
// ignorados. Thread-safe.
type RtpRxStats struct {
	mu        sync.Mutex
	clockRate uint32

	init     bool
	baseSeq  uint16
	maxSeq   uint16
	cycles   uint32 // ciclos de seq já completos (<<16)
	received uint32

	expectedPrior uint32
	receivedPrior uint32

	jitter      float64
	transit     int64
	haveTransit bool

	seen [1024]int32 // seq+1 vistos por (seq%1024), p/ descartar duplicados

	lastSR   uint32
	lastSRAt time.Time
}

// NewRtpRxStats cria o acumulador para um stream com a taxa de relógio dada (Hz).
func NewRtpRxStats(clockRate uint32) *RtpRxStats { return &RtpRxStats{clockRate: clockRate} }

// Update registra a chegada de um pacote (não duplicado).
func (s *RtpRxStats) Update(seq uint16, ts uint32, arrival time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen[seq%1024] == int32(seq)+1 {
		return
	}
	s.seen[seq%1024] = int32(seq) + 1
	if !s.init {
		s.init, s.baseSeq, s.maxSeq = true, seq, seq
	} else if d := int16(seq - s.maxSeq); d > 0 {
		if seq < s.maxSeq {
			s.cycles += 1 << 16
		}
		s.maxSeq = seq
	}
	s.received++

	arr := int64(float64(arrival.UnixNano()) / 1e9 * float64(s.clockRate))
	transit := arr - int64(ts)
	if s.haveTransit {
		d := transit - s.transit
		if d < 0 {
			d = -d
		}
		s.jitter += (float64(d) - s.jitter) / 16
	}
	s.transit, s.haveTransit = transit, true
}

// NoteSenderReport guarda o LSR do último SR do peer (p/ LSR/DLSR do próximo RR).
func (s *RtpRxStats) NoteSenderReport(ntpMid uint32, at time.Time) {
	s.mu.Lock()
	s.lastSR, s.lastSRAt = ntpMid, at
	s.mu.Unlock()
}

// Received devolve quantos pacotes únicos já chegaram.
func (s *RtpRxStats) Received() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.received
}

// Report monta o bloco de relatório de 24 bytes sobre o SSRC de origem.
func (s *RtpRxStats) Report(sourceSsrc uint32, now time.Time) ([24]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var b [24]byte
	if !s.init {
		return b, false
	}
	extMax := s.cycles | uint32(s.maxSeq)
	expected := extMax - uint32(s.baseSeq) + 1
	lost := int64(expected) - int64(s.received)
	expInt := expected - s.expectedPrior
	recInt := s.received - s.receivedPrior
	s.expectedPrior, s.receivedPrior = expected, s.received
	var fraction uint8
	if lostInt := int64(expInt) - int64(recInt); expInt != 0 && lostInt > 0 {
		fraction = uint8((lostInt << 8) / int64(expInt))
	}
	if lost > 0x7fffff {
		lost = 0x7fffff
	} else if lost < -0x800000 {
		lost = -0x800000
	}
	binary.BigEndian.PutUint32(b[0:4], sourceSsrc)
	b[4] = fraction
	b[5], b[6], b[7] = byte(lost>>16), byte(lost>>8), byte(lost)
	binary.BigEndian.PutUint32(b[8:12], extMax)
	binary.BigEndian.PutUint32(b[12:16], uint32(s.jitter))
	if !s.lastSRAt.IsZero() {
		binary.BigEndian.PutUint32(b[16:20], s.lastSR)
		binary.BigEndian.PutUint32(b[20:24], uint32(now.Sub(s.lastSRAt).Seconds()*65536))
	}
	return b, true
}
