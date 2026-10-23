package call

import (
	"encoding/hex"
	"os"
	"strconv"
	"sync"
	"time"

	"wacalls/internal/voip/captura"
	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
)

// SRTCP no 1:1 (item 1 e 3 do plano de qualidade de vídeo; ver QUALIDADE-VIDEO-1A1.md).
//
//	WACALLS_1A1_PLI    (padrão ligado)    pede keyframe (PLI) ao vídeo do celular;
//	WACALLS_1A1_SRTCP  (padrão DESLIGADO) leitura do RTCP do celular (PLI/FIR ->
//	                                      OnPeerKeyframeRequest; REMB dele no log);
//	WACALLS_1A1_SR_SEND (padrão DESLIGADO) SR+SDES periódico nosso. Separado da leitura:
//	                                      com ele ligado o contato deixou de ver a
//	                                      câmera do atendente (teste de 22/10/2026);
//	WACALLS_1A1_REMB   (padrão DESLIGADO) REMB (PT 206 FMT 15) a cada 1,5 s dizendo ao
//	                                      celular quanto ele pode mandar (WACALLS_1A1_REMB_BPS,
//	                                      padrão 500000). O celular manda REMB; sem o nosso,
//	                                      o vídeo dele desce a ~25 kbps e 4 quadros/s.
//	                                      Visto depois: o 206/15 do celular é "PRST", não REMB;
//	WACALLS_1A1_PRST   (padrão DESLIGADO) PRST (formato do celular, ver media/rtcp_wa.go) a
//	                                      cada 1,5 s com WACALLS_1A1_PRST_BPS (padrão 300000);
//	WACALLS_1A1_PRST_SEGUIR (padrão DESLIGADO) repassa o PRST do celular ao navegador
//	                                      ({"type":"bitrate","bps":N} no canal h264);
//	WACALLS_1A1_SR_NATIVO (padrão DESLIGADO) SR no formato nativo do WhatsApp a cada 1,5 s,
//	                                      no áudio e no vídeo: bloco de recepção + 24 B com
//	                                      bytes recebidos e taxa em bit/s (whatsapp-rust,
//	                                      PR #1761). Pede WACALLS_1A1_RX para o vídeo;
//	WACALLS_1A1_SR_W1  (padrão 0)         palavra 1 da extensão do SR nativo (o celular manda
//	                                      ~106000..149491 nela: estimativa de banda?);
//	WACALLS_1A1_NACK   (padrão ligado, com a leitura) reenvia os pacotes do nosso vídeo
//	                                      que o celular pede por NACK (205/1);
//	WACALLS_1A1_RR     (padrão DESLIGADO) Receiver Report (PT 201) com perda/jitter do áudio e
//	                                      do vídeo do celular (a cada 1,5 s);
//	WACALLS_1A1_SRTCP_TAG                 tag SRTCP em bytes (4|10, padrão 10);
//	WACALLS_1A1_RTCP_PROFILE_BIT          bit de perfil (0x10) nos RTCP de vídeo (padrão ligado).
//
// Fato: chaves SRTCP derivam da chave da chamada com a mesma fórmula do SRTP 1:1 e rótulos
// 3/4/5 (mesma do grupo). Suposição NÃO validada: tag de 10 bytes e bit de perfil no vídeo
// (valores do grupo/meowcaller). Na recepção testamos 10 e 4 e adotamos o que autenticar.

const rtcpInterval = 1500 * time.Millisecond

type rtcp1a1 struct {
	mu       sync.Mutex
	audio    *media.SrtcpSender
	video    *media.SrtcpSender
	recvKeys media.SrtcpKeys
	tagLen   int
	detected bool
	sr       bool   // WACALLS_1A1_SRTCP (leitura)
	srSend   bool   // WACALLS_1A1_SR_SEND
	rr       bool   // WACALLS_1A1_RR
	remb     uint64 // WACALLS_1A1_REMB: bps anunciados (0 = desligado)
	prst     uint64 // WACALLS_1A1_PRST: bps anunciados (0 = desligado)
	nack     bool   // WACALLS_1A1_NACK
	seguir   bool   // WACALLS_1A1_PRST_SEGUIR
	nativo   bool   // WACALLS_1A1_SR_NATIVO
	w1       uint32 // WACALLS_1A1_SR_W1: palavra 1 da extensão (estimativa de banda?)
	peerLog  time.Time
	dica     uint64 // último bps repassado ao navegador
	prstLog  time.Time
	prstSeen uint64
	rembLog  time.Time
	rembSeen uint64
	stop     chan struct{}
	lastReq  time.Time
	seen     map[[2]uint8]bool // (PT, FMT) já logados
	rxN      int
}

func envU32(name string) uint32 {
	n, err := strconv.ParseUint(os.Getenv(name), 10, 32)
	if err != nil {
		return 0
	}
	return uint32(n)
}

func rtcpTagLen() int {
	if n, err := strconv.Atoi(os.Getenv("WACALLS_1A1_SRTCP_TAG")); err == nil && (n == 4 || n == 10) {
		return n
	}
	return media.SrtcpAuthTagLen
}

// setupRtcpLocked prepara o SRTCP da chamada (m.mu travado).
func (m *CallManager) setupRtcpLocked(callKey []byte, ourDeviceJid, peerDeviceJid string) {
	pli := core.EnvFlag("WACALLS_1A1_PLI", true)
	sr := core.EnvFlag("WACALLS_1A1_SRTCP", false)
	rr := core.EnvFlag("WACALLS_1A1_RR", false)
	srSend := core.EnvFlag("WACALLS_1A1_SR_SEND", false)
	var remb uint64
	if core.EnvFlag("WACALLS_1A1_REMB", false) {
		remb = 500_000
		if n, err := strconv.ParseUint(os.Getenv("WACALLS_1A1_REMB_BPS"), 10, 64); err == nil && n >= 30_000 {
			remb = n
		}
	}
	var prst uint64
	if core.EnvFlag("WACALLS_1A1_PRST", false) {
		prst = 300_000
		if n, err := strconv.ParseUint(os.Getenv("WACALLS_1A1_PRST_BPS"), 10, 64); err == nil && n >= 30_000 {
			prst = n
		}
	}
	m.stopRtcpLocked()
	m.audioRx = media.NewRtpRxStats(16000)
	nativo := core.EnvFlag("WACALLS_1A1_SR_NATIVO", false)
	if !pli && !sr && !rr && !srSend && remb == 0 && prst == 0 && !nativo {
		return
	}
	r := &rtcp1a1{tagLen: rtcpTagLen(), sr: sr, srSend: srSend, rr: rr, remb: remb, prst: prst,
		nack: core.EnvFlag("WACALLS_1A1_NACK", true), seguir: core.EnvFlag("WACALLS_1A1_PRST_SEGUIR", false), nativo: nativo, w1: envU32("WACALLS_1A1_SR_W1"),
		seen: map[[2]uint8]bool{}}
	profile := core.EnvFlag("WACALLS_1A1_RTCP_PROFILE_BIT", true)
	var err error
	if m.selfSsrc != 0 {
		if r.audio, err = media.NewSrtcpSender(callKey, ourDeviceJid, m.selfSsrc, false); err != nil {
			m.log.Warn("srtcp 1:1: emissor de áudio falhou", "err", err)
			return
		}
		r.audio.SetTagLen(r.tagLen)
	}
	if vs := m.video.SelfSsrc(); vs != 0 {
		if r.video, err = media.NewSrtcpSender(callKey, ourDeviceJid, vs, profile); err != nil {
			m.log.Warn("srtcp 1:1: emissor de vídeo falhou", "err", err)
			return
		}
		r.video.SetTagLen(r.tagLen)
	}
	if r.recvKeys, err = media.DeriveGroupSrtcpKeys(callKey, peerDeviceJid); err != nil {
		m.log.Warn("srtcp 1:1: chaves de recepção falharam", "err", err)
		return
	}
	m.rtcp = r
	m.log.Info("srtcp 1:1 pronto", "pli", pli, "leitura", sr, "sr", srSend, "rr", rr, "remb_bps", remb, "prst_bps", prst, "nack", r.nack, "sr_nativo", nativo, "sr_w1", r.w1, "tag", r.tagLen, "profile_bit", profile)
}

func (m *CallManager) rekeyRtcpRecvLocked(peerKey []byte, peerJid string) {
	if m.rtcp == nil {
		return
	}
	if k, err := media.DeriveGroupSrtcpKeys(peerKey, ensureDeviceJid(peerJid)); err == nil {
		m.rtcp.mu.Lock()
		m.rtcp.recvKeys = k
		m.rtcp.mu.Unlock()
	}
}

func (m *CallManager) stopRtcpLocked() {
	if m.rtcp != nil && m.rtcp.stop != nil {
		close(m.rtcp.stop)
		m.rtcp.stop = nil
	}
	m.rtcp = nil
}

// sendPeerPLI manda um PLI SRTCP pedindo keyframe ao vídeo do celular (ssrc dele).
func (m *CallManager) sendPeerPLI(mediaSSRC uint32) {
	m.mu.Lock()
	r := m.rtcp
	m.mu.Unlock()
	if r == nil || r.video == nil {
		return
	}
	pkt, err := r.video.PictureLossIndication(mediaSSRC)
	if err != nil {
		return
	}
	m.relay.Broadcast(pkt)
	m.log.Info("video 1:1: PLI enviado ao celular", "media_ssrc", mediaSSRC)
}

// RequestPeerKeyframe pede keyframe ao celular a pedido do navegador ({"type":"pli"}).
func (m *CallManager) RequestPeerKeyframe() { m.video.RequestKeyframe() }

// startRtcpLoopLocked inicia o SR+SDES periódico (só com WACALLS_1A1_SRTCP).
func (m *CallManager) startRtcpLoopLocked() {
	r := m.rtcp
	if r == nil || !(r.srSend || r.rr || r.remb > 0 || r.prst > 0 || r.nativo) || r.stop != nil {
		return
	}
	stop := make(chan struct{})
	r.stop = stop
	go func() {
		t := time.NewTicker(rtcpInterval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case now := <-t.C:
				m.sendSenderReports(r, uint64(now.UnixMilli()))
			}
		}
	}()
}

func (m *CallManager) sendSenderReports(r *rtcp1a1, nowMs uint64) {
	m.mu.Lock()
	astats := m.audioStats
	arx := m.audioRx
	peerAudio := m.subscribedSsrcForLog()
	m.mu.Unlock()
	if r.rr {
		m.sendReceiverReports(r, arx, peerAudio, time.UnixMilli(int64(nowMs)))
	}
	if r.nativo {
		m.sendNativeSenderReports(r, astats, arx, peerAudio, nowMs)
	}
	if r.remb > 0 && r.video != nil {
		var ssrcs []uint32
		if vs := m.video.PeerSsrc(); vs != 0 {
			ssrcs = append(ssrcs, vs)
		}
		if peerAudio != 0 {
			ssrcs = append(ssrcs, peerAudio)
		}
		if len(ssrcs) > 0 {
			if pkt, err := r.video.Remb(r.remb, ssrcs); err == nil {
				m.relay.Broadcast(pkt)
			}
		}
	}
	if r.prst > 0 {
		// Sem o bit de perfil, como o do celular: sai pelo emissor do áudio.
		s := r.audio
		if s == nil {
			s = r.video
		}
		if s != nil {
			if pkt, err := s.Prst(r.prst); err == nil {
				m.relay.Broadcast(pkt)
			}
		}
	}
	if !r.srSend {
		return
	}
	if r.audio != nil && astats.PacketsSent > 0 {
		if pkt, err := r.audio.SenderReport(astats, nowMs); err == nil {
			m.relay.Broadcast(pkt)
		}
	}
	if r.video != nil {
		if vs := m.video.SenderStats(); vs.PacketsSent > 0 {
			if pkt, err := r.video.SenderReport(vs, nowMs); err == nil {
				m.relay.Broadcast(pkt)
			}
		}
	}
}

// sendNativeSenderReports manda o SR nativo do WhatsApp no áudio e no vídeo, cada um
// com o bloco de recepção (e a extensão de 24 B) do stream correspondente do celular.
func (m *CallManager) sendNativeSenderReports(r *rtcp1a1, astats media.RtcpSenderStats, arx *media.RtpRxStats, peerAudio uint32, nowMs uint64) {
	now := time.UnixMilli(int64(nowMs))
	if r.audio != nil && arx != nil && peerAudio != 0 {
		if block, ok := arx.Report(peerAudio, now); ok {
			b, rate := arx.Native(now)
			if pkt, err := r.audio.NativeSenderReport(astats, nowMs, block, media.WhatsappReceptionExtW1(b, r.w1, rate)); err == nil {
				m.relay.Broadcast(pkt)
			}
		}
	}
	if r.video != nil {
		if _, block, ok := m.video.RxReport(now); ok {
			if b, rate, ok := m.video.RxNative(now); ok {
				if pkt, err := r.video.NativeSenderReport(m.video.SenderStats(), nowMs, block, media.WhatsappReceptionExtW1(b, r.w1, rate)); err == nil {
					m.relay.Broadcast(pkt)
				}
			}
		}
	}
}

// sendReceiverReports manda um RR por stream do celular (áudio e vídeo), cada um com o
// SSRC do nosso stream correspondente como remetente.
func (m *CallManager) sendReceiverReports(r *rtcp1a1, arx *media.RtpRxStats, peerAudio uint32, now time.Time) {
	if r.audio != nil && arx != nil && peerAudio != 0 {
		if b, ok := arx.Report(peerAudio, now); ok {
			if pkt, err := r.audio.ReceiverReport(b); err == nil {
				m.relay.Broadcast(pkt)
			}
		}
	}
	if r.video != nil {
		if _, b, ok := m.video.RxReport(now); ok {
			if pkt, err := r.video.ReceiverReport(b); err == nil {
				m.relay.Broadcast(pkt)
			}
		}
	}
}

// onRelayRtcp autentica o RTCP do celular e trata PLI/FIR do nosso vídeo.
func (m *CallManager) onRelayRtcp(data []byte) {
	m.mu.Lock()
	r := m.rtcp
	m.mu.Unlock()
	if r == nil || !r.sr {
		return
	}
	ssrc, ok := media.ParseRtcpSenderSsrc(data)
	if !ok {
		return
	}
	r.mu.Lock()
	keys, tag, detected := r.recvKeys, r.tagLen, r.detected
	r.mu.Unlock()
	cur, other := tag, 14-tag
	plain, _, ok := media.UnprotectSrtcpTag(&keys, ssrc, data, tag)
	if !ok {
		if plain, _, ok = media.UnprotectSrtcpTag(&keys, ssrc, data, other); ok {
			tag = other
		}
	}
	if !ok {
		r.mu.Lock()
		r.rxN++
		n := r.rxN
		r.mu.Unlock()
		if n == 1 || n%200 == 0 {
			m.log.Info("srtcp 1:1: RTCP do celular não autenticou (tag/chaves/SSRC?)", "ssrc", ssrc, "bytes", len(data), "n", n)
		}
		return
	}
	if !detected || tag != cur {
		r.mu.Lock()
		r.detected, r.tagLen = true, tag
		r.mu.Unlock()
		for _, s := range []*media.SrtcpSender{r.audio, r.video} {
			if s != nil {
				s.SetTagLen(tag)
			}
		}
		m.log.Info("srtcp 1:1: RTCP do celular autenticado", "tag", tag, "ssrc", ssrc)
	}
	if captura.Ligada() {
		captura.Grava('r', nil, plain)
	}
	m.logRtcpKinds(r, plain)
	if recs := media.ParsePeerReceptions(plain); len(recs) > 0 {
		r.mu.Lock()
		logIt := time.Since(r.peerLog) >= 5*time.Second
		if logIt {
			r.peerLog = time.Now()
		}
		r.mu.Unlock()
		if logIt {
			vs := m.video.SenderStats()
			m.mu.Lock()
			as, selfAudio := m.audioStats, m.selfSsrc
			m.mu.Unlock()
			for _, pr := range recs {
				m.log.Info("srtcp 1:1: o celular diz o que recebe de nós (SR dele)", "sobre_ssrc", pr.About,
					"perda_frac", pr.FractionLost, "perda_total", pr.CumLost, "jitter", pr.Jitter, "ext", pr.HasExt,
					"w0_bytes", pr.Ext[0], "w1", pr.Ext[1], "w2", pr.Ext[2], "w3", pr.Ext[3], "w4", pr.Ext[4], "w5_bps", pr.Ext[5],
					"nossos_bytes_video", vs.OctetsSent, "nossos_bytes_audio", as.OctetsSent,
					"nosso_ssrc_video", m.video.SelfSsrc(), "nosso_ssrc_audio", selfAudio)
			}
		}
	}
	for _, bps := range media.ParsePrst(plain) {
		r.mu.Lock()
		repassar := r.seguir && bps != r.dica
		if repassar {
			r.dica = bps
		}
		cb := m.OnPeerBitrateHint
		prev := r.prstSeen
		logIt := time.Since(r.prstLog) >= 10*time.Second || prev == 0 || bps*4 < prev*3 || bps*3 > prev*4
		if logIt {
			r.prstLog, r.prstSeen = time.Now(), bps
		}
		r.mu.Unlock()
		if logIt {
			m.log.Info("srtcp 1:1: PRST do celular (banda estimada para o nosso envio, suposição)", "bps", bps)
		}
		if repassar && cb != nil {
			cb(bps)
		}
	}
	if vs := m.video.SelfSsrc(); vs != 0 && r.nack {
		if seqs := media.ParseNacks(plain, vs); len(seqs) > 0 {
			m.video.Retransmit(seqs)
		}
	}
	for _, rb := range media.ParseRembs(plain) {
		r.mu.Lock()
		prev := r.rembSeen
		logIt := time.Since(r.rembLog) >= 10*time.Second || prev == 0 ||
			rb.Bitrate*4 < prev*3 || rb.Bitrate*3 > prev*4
		if logIt {
			r.rembLog, r.rembSeen = time.Now(), rb.Bitrate
		}
		r.mu.Unlock()
		if logIt {
			m.log.Info("srtcp 1:1: REMB do celular (banda que ele estima para o nosso envio)",
				"bps", rb.Bitrate, "ssrcs", rb.Ssrcs, "sender", rb.Sender)
		}
	}
	for _, sr := range media.ParseSenderReports(plain) {
		now := time.Now()
		m.video.NoteSenderReport(sr.SSRC, sr.NtpMid, now)
		m.mu.Lock()
		arx, pa := m.audioRx, m.subscribedSsrcForLog()
		m.mu.Unlock()
		if arx != nil && sr.SSRC == pa {
			arx.NoteSenderReport(sr.NtpMid, now)
		}
	}
	if vs := m.video.SelfSsrc(); vs != 0 && media.RtcpRequestsKeyframe(plain, vs) {
		r.mu.Lock()
		// O celular repete o PLI a cada ~425 ms enquanto não decodifica: repassar
		// todos virava ~2,3 IDR/s num caminho de 50 kbps e enchia a fila do relay.
		fire := time.Since(r.lastReq) >= time.Second
		if fire {
			r.lastReq = time.Now()
		}
		r.mu.Unlock()
		if fire && m.OnPeerKeyframeRequest != nil {
			m.log.Info("video 1:1: celular pediu keyframe do nosso vídeo (PLI/FIR)")
			m.OnPeerKeyframeRequest()
		}
	}
}

// logRtcpKinds loga (uma vez) cada combinação (PT, FMT) recebida do celular: é a
// evidência de que feedback de banda o WhatsApp usa de fato (RR, REMB, transport-cc...).
func (m *CallManager) logRtcpKinds(r *rtcp1a1, plain []byte) {
	for off := 0; off+4 <= len(plain); {
		l := (int(plain[off+2])<<8 | int(plain[off+3]) + 1) * 4
		if l < 4 || off+l > len(plain) {
			return
		}
		key := [2]uint8{plain[off+1], plain[off] & 0x1f}
		r.mu.Lock()
		first := !r.seen[key]
		r.seen[key] = true
		r.mu.Unlock()
		if first {
			// Os bytes (decifrados: SSRCs e números do protocolo, nada de chave) do
			// feedback que ainda não entendemos, para decifrar o formato.
			var bytesHex string
			if key[0] == 205 || key[0] == 206 || key[0] == 204 {
				bytesHex = hex.EncodeToString(plain[off : off+l])
			}
			m.log.Info("srtcp 1:1: tipo de RTCP do celular", "pt", key[0], "fmt_ou_rc", key[1], "bytes", l, "hex", bytesHex)
		}
		off += l
	}
}
