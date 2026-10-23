package video

import (
	"os"
	"time"

	"wacalls/internal/voip/captura"
	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
	"wacalls/internal/voip/transport"
)

// Recepção robusta do vídeo do peer no 1:1 (WACALLS_1A1_RX, padrão ligado):
//   - trava no SSRC de vídeo do peer (descarta outros);
//   - descarta duplicados (2 relays abertos) e reordena numa janela curta;
//   - numa lacuna descarta o quadro em curso e tudo até o próximo IDR, em vez de
//     entregar um quadro corrompido ao navegador, e pede keyframe (PLI).
//
// WACALLS_1A1_PLI (padrão ligado) controla só o pedido de PLI.
// WACALLS_1A1_REORDER_MS (padrão 40) ajusta a janela de reordenação.

const (
	defaultReorderWindow = 120 * time.Millisecond
	pliInterval          = time.Second
	pliMinGap            = 300 * time.Millisecond
	rxDiagInterval       = 5 * time.Second
)

type rxState struct {
	enabled bool
	pli     bool

	derivedSsrc uint32 // SSRC de vídeo do peer derivado (slot 2); só referência
	ssrc        uint32 // SSRC travado (fluxo que decifra como H.264; ver rx_ssrc.go)
	cands       map[uint32]*ssrcCand

	reorder *transport.RtpReorderer[*media.RtpPacket]
	stats   *media.RtpRxStats // perda/jitter do vídeo do peer (p/ Receiver Report)
	timer   *time.Timer
	window  time.Duration

	gotIDR       bool // já entregamos ao menos um IDR
	waitIDR      bool // após lacuna: descartar até o próximo IDR
	waitSince    time.Time
	idrWait      time.Duration // espera máxima pelo IDR depois de uma lacuna
	skipToMarker bool          // após lacuna: descartar até o fim do AU em curso

	lastPLI time.Time

	lastLocked time.Time // último pacote do SSRC travado
	lastDiag   time.Time

	// contadores (diagnóstico, logados a cada rxDiagInterval)
	Pkts    uint64
	Foreign uint64 // pacotes de outro SSRC descartados
	Relocks uint64
	Frames  uint64
	IDRs    uint64
	GaveUp  uint64 // esperas de IDR abandonadas (o celular não mandou a tempo)

	// contadores (diagnóstico)
	Dropped uint64 // quadros descartados por lacuna/espera de IDR
	Gaps    uint64
}

func (p *Pipeline) setupRxLocked(derivedPeerSsrc uint32) {
	p.resetRxLocked()
	if !core.EnvFlag("WACALLS_1A1_RX", true) {
		p.rx.pli = core.EnvFlag("WACALLS_1A1_PLI", true)
		return
	}
	w := defaultReorderWindow
	if ms := envInt("WACALLS_1A1_REORDER_MS"); ms > 0 {
		w = time.Duration(ms) * time.Millisecond
	}
	idrWait := defaultIDRWait
	if ms := envInt("WACALLS_1A1_IDR_WAIT_MS"); ms > 0 {
		idrWait = time.Duration(ms) * time.Millisecond
	}
	p.rx = rxState{
		idrWait:     idrWait,
		cands:       map[uint32]*ssrcCand{},
		enabled:     true,
		pli:         core.EnvFlag("WACALLS_1A1_PLI", true),
		derivedSsrc: derivedPeerSsrc,
		window:      w,
		reorder:     transport.NewRtpReorderer[*media.RtpPacket](w, 512),
		stats:       media.NewRtpRxStats(90000),
	}
}

func (p *Pipeline) resetRxLocked() {
	if p.rx.timer != nil {
		p.rx.timer.Stop()
	}
	p.rx = rxState{}
}

// handleRobustLocked: filtra por SSRC, decifra, reordena e consome em ordem.
func (p *Pipeline) handleRobustLocked(data []byte, now time.Time) (d deliveries) {
	ssrc := media.RTPSsrc(data)
	p.diagLocked(now)
	pkt, err := p.srtp.Unprotect(data)
	if err != nil {
		p.log.Debug("video srtp unprotect error", "err", err)
		return
	}
	if len(pkt.Payload) == 0 {
		return
	}
	if captura.Ligada() {
		captura.Grava('v', pkt.Header.HeaderBytes(), pkt.Payload)
	}
	// Sem autenticação na recepção, um fluxo com outra chave (outro aparelho do
	// contato) decifra lixo sem erro: quem decide é a cara de H.264 do payload.
	c := p.noteCandLocked(ssrc, pkt.Payload, now)
	if ssrc != p.rx.ssrc {
		if !p.shouldLockLocked(c, now) {
			if p.rx.ssrc != 0 {
				p.rx.Foreign++
			}
			c.keep(pkt)
			return
		}
		p.lockLocked(ssrc, c, now)
		// Os pacotes guardados enquanto o fluxo ganhava nota (o 1º IDR da chamada
		// costuma estar neles) entram antes deste.
		for _, q := range c.pending {
			p.pushLocked(q, now, &d)
		}
		c.pending = nil
	}
	p.rx.lastLocked = now
	p.pushLocked(pkt, now, &d)
	p.armFlushLocked(now)
	p.maybePliLocked(now, false, &d)
	return d
}

func (p *Pipeline) pushLocked(pkt *media.RtpPacket, now time.Time, d *deliveries) {
	p.rx.Pkts++
	p.noteReceptionLocked(pkt, now)
	for _, o := range p.rx.reorder.Push(pkt.Header.SequenceNumber, pkt, now) {
		p.consumeOrderedLocked(o, d)
	}
}

func (p *Pipeline) consumeOrderedLocked(o transport.Ordered[*media.RtpPacket], d *deliveries) {
	if o.LostBefore > 0 {
		p.rx.Gaps++
		p.depack.Reset()
		if len(p.frameBuf) > 0 {
			p.frameBuf = nil
			p.rx.Dropped++
		}
		if !p.rx.waitIDR {
			p.rx.waitIDR = true
			p.rx.waitSince = time.Now()
		}
		p.rx.skipToMarker = true
		p.maybePliLocked(time.Now(), true, d)
	}
	if p.rx.skipToMarker {
		if o.Item.Header.Marker {
			p.rx.skipToMarker = false
			p.rx.Dropped++
		}
		return
	}
	p.consumeLocked(o.Item, d)
}

// frameDoneLocked decide se um access unit completo vai ao navegador.
func (p *Pipeline) frameDoneLocked(frame []byte, d *deliveries) {
	if p.rx.enabled {
		idr := annexBHasIDR(frame)
		if p.rx.waitIDR && !idr {
			// O celular pode ignorar o PLI e só mandar IDR de tempos em tempos:
			// congelar até lá é pior que uns quadros borrados. Desiste da espera.
			if time.Since(p.rx.waitSince) < p.rx.idrWait {
				p.rx.Dropped++
				return
			}
			p.rx.waitIDR = false
			p.rx.GaveUp++
		}
		if idr {
			p.rx.IDRs++
			if !p.rx.gotIDR || p.rx.waitIDR {
				p.log.Info("video 1:1: IDR recebido", "ssrc", p.rx.ssrc,
					"ms_desde_pli", sinceMs(p.rx.lastPLI), "esperando_desde_ms", sinceMs(p.rx.waitSince))
			}
			p.rx.waitIDR = false
			p.rx.gotIDR = true
		}
	}
	p.rx.Frames++
	d.frames = append(d.frames, frame)
}

func annexBHasIDR(au []byte) bool {
	for _, n := range transport.SplitAnnexB(au) {
		if len(n) > 0 && n[0]&0x1f == 5 {
			return true
		}
	}
	return false
}

// maybePliLocked agenda um PLI: a cada 1 s até o 1º IDR / enquanto esperamos IDR
// após lacuna; force (lacuna) respeita só o intervalo mínimo de 300 ms.
func (p *Pipeline) maybePliLocked(now time.Time, force bool, d *deliveries) {
	if !p.rx.pli || p.rx.ssrc == 0 || d.pliNeeded {
		return
	}
	wanting := !p.rx.gotIDR || p.rx.waitIDR
	if !wanting {
		return
	}
	gap := pliInterval
	if force {
		gap = pliMinGap
	}
	if now.Sub(p.rx.lastPLI) < gap {
		return
	}
	p.rx.lastPLI = now
	d.pliNeeded, d.pliSsrc = true, p.rx.ssrc
}

// RequestKeyframe é o pedido explícito (ex.: decoder do navegador com erro):
// manda PLI ao peer respeitando o intervalo mínimo.
func (p *Pipeline) RequestKeyframe() {
	p.mu.Lock()
	ssrc := p.rx.ssrc
	if ssrc == 0 {
		ssrc = p.rx.derivedSsrc
	}
	now := time.Now()
	ok := p.rx.enabled && p.rx.pli && ssrc != 0 && now.Sub(p.rx.lastPLI) >= pliMinGap
	if ok {
		p.rx.lastPLI = now
	}
	req := p.OnKeyframeRequest
	p.mu.Unlock()
	if ok && req != nil {
		req(ssrc)
	}
}

// armFlushLocked garante um timer para soltar pacotes retidos além da janela.
func (p *Pipeline) armFlushLocked(now time.Time) {
	if p.rx.reorder.Pending() == 0 || p.rx.timer != nil {
		return
	}
	wait := p.rx.reorder.NextDeadline().Sub(now)
	if wait < time.Millisecond {
		wait = time.Millisecond
	}
	reorder := p.rx.reorder
	p.rx.timer = time.AfterFunc(wait, func() { p.flushReorder(reorder) })
}

func (p *Pipeline) flushReorder(r *transport.RtpReorderer[*media.RtpPacket]) {
	p.mu.Lock()
	if p.rx.reorder != r { // pipeline foi reiniciado
		p.mu.Unlock()
		return
	}
	p.rx.timer = nil
	now := time.Now()
	var d deliveries
	for _, o := range r.Flush(now) {
		p.consumeOrderedLocked(o, &d)
	}
	p.armFlushLocked(now)
	p.maybePliLocked(now, false, &d)
	p.mu.Unlock()
	p.dispatch(d)
}

func (p *Pipeline) noteReceptionLocked(pkt *media.RtpPacket, now time.Time) {
	if p.rx.stats != nil {
		p.rx.stats.UpdateWithPayload(pkt.Header.SequenceNumber, pkt.Header.Timestamp, now, len(pkt.Payload))
	}
}

// RxReport devolve o SSRC do vídeo do peer e o bloco de relatório (perda/jitter) p/ o
// Receiver Report; ok=false se ainda não chegou nada.
func (p *Pipeline) RxReport(now time.Time) (ssrc uint32, block [24]byte, ok bool) {
	p.mu.Lock()
	st, ssrc := p.rx.stats, p.rx.ssrc
	p.mu.Unlock()
	if st == nil || ssrc == 0 {
		return 0, block, false
	}
	block, ok = st.Report(ssrc, now)
	return ssrc, block, ok
}

// NoteSenderReport registra um SR do peer (LSR/DLSR dos próximos RR).
func (p *Pipeline) NoteSenderReport(ssrc, ntpMid uint32, at time.Time) {
	p.mu.Lock()
	st, mine := p.rx.stats, p.rx.ssrc
	p.mu.Unlock()
	if st != nil && ssrc == mine {
		st.NoteSenderReport(ntpMid, at)
	}
}

func envInt(name string) int {
	n := 0
	for _, c := range os.Getenv(name) {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
}
