package video

import (
	"sync"
	"sync/atomic"
	"time"

	"wacalls/internal/voip/media"
)

const rtcpInterval = 1500 * time.Millisecond

// rtcpState é o SRTCP do 1:1. As chaves usam a MESMA derivação do SRTP 1:1 (HKDF da
// chave da chamada com info = JID do dispositivo, rótulos SRTCP 3/4/5) — suposição
// herdada do grupo; o tamanho do tag no 1:1 NÃO é conhecido, então por padrão
// emitimos com tag de 10 e de 4 bytes e na recepção tentamos os dois.
type rtcpState struct {
	senders  []*media.SrtcpSender // um por tamanho de tag (vídeo, perfil ligado)
	tags     []int
	recvKeys media.SrtcpKeys
	periodic bool // SR+SDES periódico e leitura (WACALLS_1A1_SRTCP)
	rr       bool // Receiver Report (WACALLS_1A1_RR)
	stop     chan struct{}
	stopOnce sync.Once

	seenAuthed atomic.Uint64
	seenFailed atomic.Uint64
}

// SetupRtcp liga o SRTCP do 1:1 (chamar depois do Setup). callKey é a chave da
// chamada (32 bytes); os JIDs são os mesmos passados ao Setup. O PLI (padrão ligado)
// precisa só dos emissores; o SR periódico e a leitura do RTCP do peer ficam atrás de
// WACALLS_1A1_SRTCP (padrão desligado até validar o tag numa chamada real).
func (p *Pipeline) SetupRtcp(callKey []byte, ourDeviceJid, peerDeviceJid string) error {
	pli := envBool(envPLI, true)
	periodic := envBool(envSRTCP, false)
	rr := envBool(envRR, false)
	if !pli && !periodic && !rr {
		return nil
	}
	p.mu.Lock()
	ssrc := p.selfSsrc
	p.mu.Unlock()
	rs := &rtcpState{periodic: periodic, rr: rr, tags: srtcpTagLens(), stop: make(chan struct{})}
	for _, tl := range rs.tags {
		s, err := media.NewSrtcpSender(callKey, ourDeviceJid, ssrc, true)
		if err != nil {
			return err
		}
		s.SetAuthTagLen(tl)
		rs.senders = append(rs.senders, s)
	}
	keys, err := media.DeriveGroupSrtcpKeys(callKey, peerDeviceJid)
	if err != nil {
		return err
	}
	rs.recvKeys = keys
	if old := p.rtcp.Swap(rs); old != nil {
		old.stopOnce.Do(func() { close(old.stop) })
	}
	if periodic || rr {
		go p.rtcpLoop(rs)
	}
	p.log.Debug("video 1:1 SRTCP configurado", "pli", pli, "sr", periodic, "rr", rr, "tags", rs.tags)
	return nil
}

// sendRtcp protege com cada emissor (um por tamanho de tag) e manda no relay.
func (p *Pipeline) sendRtcp(rs *rtcpState, build func(s *media.SrtcpSender) ([]byte, error)) bool {
	ok := false
	for _, s := range rs.senders {
		if pkt, err := build(s); err == nil {
			p.relay.Broadcast(pkt)
			ok = true
		}
	}
	return ok
}

// sendPLI manda um PLI pedindo keyframe ao mediaSSRC. False se o SRTCP não está ligado.
func (p *Pipeline) sendPLI(mediaSSRC uint32) bool {
	rs := p.rtcp.Load()
	if rs == nil || len(rs.senders) == 0 || !p.relay.HasConnection() {
		return false
	}
	return p.sendRtcp(rs, func(s *media.SrtcpSender) ([]byte, error) {
		return s.PictureLossIndication(mediaSSRC)
	})
}

// rtcpLoop manda o SR+SDES periódico do nosso vídeo (como no grupo) e, se ligado, o RR.
func (p *Pipeline) rtcpLoop(rs *rtcpState) {
	t := time.NewTicker(rtcpInterval)
	defer t.Stop()
	for {
		select {
		case <-rs.stop:
			return
		case now := <-t.C:
			if !p.relay.HasConnection() {
				continue
			}
			nowMs := uint64(now.UnixMilli())
			if rs.periodic {
				if st := p.SenderStats(); st.PacketsSent > 0 {
					p.sendRtcp(rs, func(s *media.SrtcpSender) ([]byte, error) { return s.SenderReport(st, nowMs) })
				}
			}
			if rs.rr {
				if blk := p.rxReceiverBlock(); blk != nil {
					p.mu.Lock()
					self := p.selfSsrc
					p.mu.Unlock()
					plain := media.BuildReceiverReport(self, []media.RtcpReportBlock{*blk}, true)
					p.sendRtcp(rs, func(s *media.SrtcpSender) ([]byte, error) { return s.Protect(plain) })
				}
			}
		}
	}
}

// HandleRelayRtcp trata um RTCP do peer (PT 200-206 no relay): autentica com as chaves
// SRTCP dele e, se for PLI/FIR pro nosso vídeo, avisa OnKeyframeRequest. Só age com
// WACALLS_1A1_SRTCP ligado; sem isso o RTCP continua descartado como antes.
func (p *Pipeline) HandleRelayRtcp(data []byte) {
	rs := p.rtcp.Load()
	if rs == nil || !rs.periodic {
		return
	}
	ssrc, ok := media.ParseRtcpSenderSsrc(data)
	if !ok {
		return
	}
	var plain []byte
	var gotTag int
	for _, tl := range rs.tags {
		if pl, _, ok := media.UnprotectSrtcpTag(&rs.recvKeys, ssrc, data, tl); ok {
			plain, gotTag = pl, tl
			break
		}
	}
	if plain == nil {
		// Só roda com a chave ligada: loga as primeiras falhas p/ validar a derivação/tag.
		if n := rs.seenFailed.Add(1); n <= 3 {
			p.log.Info("video 1:1: RTCP do peer não autenticou (chave ou tag diferentes do suposto)",
				"pt", data[1], "len", len(data), "sender_ssrc", ssrc, "n", n)
		}
		return
	}
	if n := rs.seenAuthed.Add(1); n == 1 {
		p.log.Info("video 1:1: 1º RTCP do peer autenticado — chave SRTCP e tag confirmados",
			"tag_bytes", gotTag, "pt", plain[1], "len", len(data), "sender_ssrc", ssrc)
	}
	p.mu.Lock()
	self, cb := p.selfSsrc, p.OnKeyframeRequest
	p.mu.Unlock()
	if self != 0 && media.RtcpRequestsKeyframe(plain, self) {
		p.log.Info("video 1:1: peer pediu keyframe do nosso vídeo (PLI/FIR)")
		if cb != nil {
			cb()
		}
	}
}
