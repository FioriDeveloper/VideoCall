package video

import (
	"sync"
	"sync/atomic"
	"time"

	"wacalls/internal/voip/media"
	"wacalls/internal/voip/transport"
)

const (
	rxMaxHeld        = 128 // pacotes retidos no máximo na reordenação
	pliMinInterval   = 300 * time.Millisecond
	pliRepeat        = time.Second     // repete o PLI enquanto esperamos o IDR
	idrFailOpenAfter = 3 * time.Second // sem IDR por tanto tempo: volta a entregar (melhor artefato que congelar)
)

// RxStats são os contadores da recepção 1:1 (diagnóstico / logs).
type RxStats struct {
	Packets, Dups, Foreign, LostPackets uint64
	FramesDelivered, FramesDropped      uint64
	PLIsSent                            uint64
}

// rxState é a recepção robusta do vídeo 1:1: filtro de SSRC, reordenação, descarte de
// duplicados, detecção de lacuna e espera por IDR. Um rxState por Setup; Reset troca
// o ponteiro (atomic) — nenhum lock do pipeline é tomado pelo Setup/Reset.
type rxState struct {
	mu   sync.Mutex // serializa toda a recepção (srtp unprotect, reordenação, depack)
	cbMu sync.Mutex // mantém a ORDEM das callbacks (tomado antes de soltar mu)

	expectedSsrc uint32 // SSRC de vídeo derivado p/ o peer (slot 2)
	peerSsrc     uint32 // SSRC travado (o esperado, ou o 1º autenticado fora do esperado)

	buf   *reorderBuffer[*media.RtpPacket]
	depk  *transport.H264Depacketizer
	timer *time.Timer
	stats media.RtpReceiverStats

	frameBuf     []byte
	frameCorrupt bool // lacuna dentro do quadro corrente: descartar no marker
	frameHasIDR  bool
	waitIDR      bool // só entrega a partir de um IDR
	waitSince    time.Time
	lastPLI      time.Time
	lastOrient   int
	c            RxStats
	stopped      atomic.Bool
}

type rxOutput struct {
	frames  [][]byte
	orients []int
}

func newRxState(expectedSsrc uint32) *rxState {
	return &rxState{
		expectedSsrc: expectedSsrc,
		buf:          newReorderBuffer[*media.RtpPacket](reorderWindow(), rxMaxHeld),
		depk:         &transport.H264Depacketizer{},
		stats:        media.RtpReceiverStats{ClockRate: 90000},
		waitIDR:      true,
		lastOrient:   -1,
	}
}

// stopRx encerra um rxState descartado (handlers e timer em voo viram no-op).
func (p *Pipeline) stopRx(rx *rxState) {
	if rx == nil {
		return
	}
	// Sem tomar rx.mu: o Setup roda com o lock do CallManager e uma callback pode estar
	// esperando por ele com cbMu em mãos. O timer em voo vira no-op (p.rx != rx).
	rx.stopped.Store(true)
}

// RxStats devolve os contadores da recepção robusta (zero se desligada).
func (p *Pipeline) RxStats() RxStats {
	rx := p.rx.Load()
	if rx == nil {
		return RxStats{}
	}
	rx.mu.Lock()
	defer rx.mu.Unlock()
	c := rx.c
	c.Dups = rx.buf.Dups
	return c
}

// handleRobust processa um pacote de vídeo do relay no caminho 1:1 robusto.
func (p *Pipeline) handleRobust(rx *rxState, data []byte) {
	p.mu.Lock()
	srtp, selfSsrc := p.srtp, p.selfSsrc
	p.mu.Unlock()
	if srtp == nil {
		return
	}
	ssrc := media.RTPSsrc(data)
	if ssrc == selfSsrc {
		return
	}

	rx.mu.Lock()
	if rx.stopped.Load() {
		rx.mu.Unlock()
		return
	}
	if rx.peerSsrc != 0 && ssrc != rx.peerSsrc {
		rx.c.Foreign++
		rx.mu.Unlock()
		return
	}
	pkt, err := srtp.Unprotect(data)
	if err != nil {
		rx.mu.Unlock()
		p.log.Debug("video srtp unprotect error", "err", err)
		return
	}
	if rx.peerSsrc == 0 {
		rx.peerSsrc = ssrc
		if ssrc != rx.expectedSsrc {
			p.log.Warn("video 1:1: SSRC do peer diferente do derivado; travando no observado",
				"observed", ssrc, "expected", rx.expectedSsrc)
		} else {
			p.log.Debug("video 1:1: SSRC do peer travado", "ssrc", ssrc)
		}
	}
	if len(pkt.Payload) == 0 {
		rx.mu.Unlock()
		return
	}
	now := time.Now()
	seq := pkt.Header.SequenceNumber
	if rx.buf.seen(seq) {
		rx.buf.Dups++
		rx.mu.Unlock()
		return
	}
	rx.c.Packets++
	rx.stats.Update(seq, pkt.Header.Timestamp, now.UnixMilli())
	var out rxOutput
	items, lost := rx.buf.push(seq, pkt, now)
	p.rxDeliver(rx, items, lost, now, &out)
	p.armRxTimer(rx)
	p.rxFinish(rx, &out) // solta rx.mu
}

// rxFinish solta rx.mu entregando as callbacks fora dele mas na ordem (cbMu).
func (p *Pipeline) rxFinish(rx *rxState, out *rxOutput) {
	if len(out.frames) == 0 && len(out.orients) == 0 {
		rx.mu.Unlock()
		return
	}
	rx.cbMu.Lock()
	rx.mu.Unlock()
	defer rx.cbMu.Unlock()
	if cb := p.OnOrientation; cb != nil {
		for _, o := range out.orients {
			cb(o)
		}
	}
	if cb := p.OnFrame; cb != nil {
		for _, f := range out.frames {
			cb(f)
		}
	}
}

// rxDeliver processa os pacotes já em ordem. Com rx.mu travado.
func (p *Pipeline) rxDeliver(rx *rxState, items []*media.RtpPacket, lost int, now time.Time, out *rxOutput) {
	if lost > 0 {
		rx.c.LostPackets += uint64(lost)
		p.rxMarkLoss(rx, now)
	}
	for _, pkt := range items {
		if o, ok := parseVideoOrientation(pkt.Header); ok && o != rx.lastOrient {
			rx.lastOrient = o
			out.orients = append(out.orients, o)
		}
		for _, nalu := range rx.depk.Depacketize(pkt.Payload) {
			if len(nalu) == 0 {
				continue
			}
			if nalu[0]&0x1f == 5 {
				rx.frameHasIDR = true
			}
			rx.frameBuf = append(rx.frameBuf, annexBStartCode...)
			rx.frameBuf = append(rx.frameBuf, nalu...)
		}
		if pkt.Header.Marker {
			p.rxEndFrame(rx, now, out)
		}
	}
	// Enquanto esperamos o IDR, repete o PLI a cada 1s (os celulares só mandam IDR
	// sob pedido).
	if rx.waitIDR && len(items) > 0 && now.Sub(rx.lastPLI) >= pliRepeat {
		p.rxSendPLI(rx, now)
	}
}

// rxMarkLoss: houve lacuna. O quadro corrente está corrompido, o decoder do peer
// perdeu referência: descarta até o próximo IDR e pede PLI.
func (p *Pipeline) rxMarkLoss(rx *rxState, now time.Time) {
	rx.frameCorrupt = true
	rx.frameBuf = nil
	rx.frameHasIDR = false
	rx.depk = &transport.H264Depacketizer{}
	if !rx.waitIDR {
		rx.waitIDR = true
		rx.waitSince = now
	}
	if now.Sub(rx.lastPLI) >= pliMinInterval {
		p.rxSendPLI(rx, now)
	}
}

// rxEndFrame fecha o quadro no marker: entrega ou descarta.
func (p *Pipeline) rxEndFrame(rx *rxState, now time.Time, out *rxOutput) {
	frame, corrupt, hasIDR := rx.frameBuf, rx.frameCorrupt, rx.frameHasIDR
	rx.frameBuf, rx.frameCorrupt, rx.frameHasIDR = nil, false, false
	if len(frame) == 0 || corrupt {
		rx.c.FramesDropped++
		return
	}
	if rx.waitIDR {
		switch {
		case hasIDR:
			rx.waitIDR = false
		case !rx.waitSince.IsZero() && now.Sub(rx.waitSince) > idrFailOpenAfter:
			// O peer não mandou IDR apesar dos PLIs: volta a entregar.
			rx.waitIDR = false
			p.log.Warn("video 1:1: sem IDR após perda; voltando a entregar sem esperar")
		default:
			rx.c.FramesDropped++
			return
		}
	}
	rx.c.FramesDelivered++
	out.frames = append(out.frames, frame)
}

// armRxTimer agenda a expiração da lacuna mais antiga (sem novos pacotes ninguém
// mais empurraria a fila). Com rx.mu travado.
func (p *Pipeline) armRxTimer(rx *rxState) {
	dl := rx.buf.nextDeadline()
	if dl.IsZero() {
		if rx.timer != nil {
			rx.timer.Stop()
		}
		return
	}
	d := time.Until(dl)
	if d < time.Millisecond {
		d = time.Millisecond
	}
	if rx.timer == nil {
		rx.timer = time.AfterFunc(d, func() { p.rxExpire(rx) })
		return
	}
	rx.timer.Reset(d)
}

func (p *Pipeline) rxExpire(rx *rxState) {
	rx.mu.Lock()
	if rx.stopped.Load() || p.rx.Load() != rx {
		rx.mu.Unlock()
		return
	}
	now := time.Now()
	var out rxOutput
	items, lost := rx.buf.expire(now)
	p.rxDeliver(rx, items, lost, now, &out)
	p.armRxTimer(rx)
	p.rxFinish(rx, &out)
}

// rxSendPLI pede um keyframe ao SSRC de vídeo do peer. Com rx.mu travado.
func (p *Pipeline) rxSendPLI(rx *rxState, now time.Time) {
	rx.lastPLI = now
	ssrc := rx.peerSsrc
	if ssrc == 0 {
		ssrc = rx.expectedSsrc
	}
	if p.sendPLI(ssrc) {
		rx.c.PLIsSent++
		if n := rx.c.PLIsSent; n <= 3 || n%20 == 0 {
			p.log.Info("video 1:1: PLI enviado ao peer (pedido de keyframe)", "media_ssrc", ssrc, "total", n)
		}
	}
}

// RequestKeyframe pede um keyframe ao peer agora (ex.: o decoder do navegador pediu).
func (p *Pipeline) RequestKeyframe() {
	rx := p.rx.Load()
	if rx == nil {
		return
	}
	rx.mu.Lock()
	defer rx.mu.Unlock()
	now := time.Now()
	if now.Sub(rx.lastPLI) >= pliMinInterval {
		p.rxSendPLI(rx, now)
	}
}

// rxReceiverBlock monta o bloco de relatório da recepção (nil = nada recebido).
func (p *Pipeline) rxReceiverBlock() *media.RtcpReportBlock {
	rx := p.rx.Load()
	if rx == nil {
		return nil
	}
	rx.mu.Lock()
	defer rx.mu.Unlock()
	if rx.peerSsrc == 0 || rx.stats.Received() == 0 {
		return nil
	}
	b := rx.stats.Block(rx.peerSsrc)
	return &b
}
