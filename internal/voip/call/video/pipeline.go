package video

import (
	"log/slog"
	"sync"
	"time"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
	"wacalls/internal/voip/transport"
)

const (
	rtpStepSamples      = 90000 / 15
	congestionDropBytes = 48 * 1024
	slotWord            = 2
)

var (
	callSlots       = []uint32{0, 1, 4, 2, 3, 5}
	annexBStartCode = []byte{0, 0, 0, 1}
)

type Relay interface {
	Broadcast(data []byte)
	BufferedAmount() uint64
	HasConnection() bool
	SetStreamSsrcs(selfSsrcs, peerSsrcs []uint32)
}

type Pipeline struct {
	log   *slog.Logger
	relay Relay

	mu       sync.Mutex
	rtp      *media.RtpSession
	srtp     *media.SrtpSession
	selfSsrc uint32
	clock    *media.MediaClock // relógio de mídia da call (semeia o ts do vídeo no 1º frame)
	depack   *transport.H264Depacketizer
	frameBuf []byte
	lastAUAt time.Time

	// Estado da extensão de vídeo do WhatsApp (perfil 0xDEBE) no envio.
	frameNumber      uint16 // nº do frame (por access unit), começa em 1
	transportSeq     uint16 // sequência transport (por pacote)
	keyframeRequired bool   // o primeiro frame enviado precisa ser IDR

	stats media.RtcpSenderStats // contadores de envio (p/ Sender Report SRTCP)

	lastOrientation int // última orientação (0..3) vista na extensão RTP do peer; -1 = nenhuma

	// Recepção robusta do vídeo do peer no 1:1 (WACALLS_1A1_RX; ver rx.go).
	rx rxState

	OnFrame func(au []byte)
	// OnKeyframeRequest é chamado (fora do mutex) quando o pipeline quer um keyframe
	// do peer; recebe o SSRC de vídeo dele. Quem implementa envia o PLI via SRTCP.
	OnKeyframeRequest func(mediaSSRC uint32)
	// OnOrientation é chamado quando a ORIENTAÇÃO da câmera do peer muda (quartos de
	// volta horários, 0..3), lida da extensão de vídeo do WhatsApp (MediaFrameInfo,
	// bits baixos = CVO). Permite o painel girar o vídeo sozinho.
	OnOrientation func(orientation int)
}

// parseVideoOrientation lê a orientação (CVO, 2 bits baixos do MediaFrameInfo, id 3)
// da extensão 0xDEBE de um pacote de vídeo recebido.
func parseVideoOrientation(h *media.RtpHeader) (int, bool) {
	if h == nil || !h.Extension || h.ExtensionProfile != videoExtProfile {
		return 0, false
	}
	ext := h.ExtensionData
	for off := 0; off < len(ext); {
		hdr := ext[off]
		off++
		if hdr == 0 {
			continue // padding
		}
		id := hdr >> 4
		l := int(hdr&0x0f) + 1
		if id == 15 || off+l > len(ext) {
			return 0, false
		}
		if id == 3 {
			return int(ext[off] & 0x03), true
		}
		off += l
	}
	return 0, false
}

func New(log *slog.Logger, relay Relay) *Pipeline {
	if log == nil {
		log = slog.Default()
	}
	return &Pipeline{log: log, relay: relay, lastOrientation: -1}
}

// SetClock liga o relógio de mídia da call ao pipeline: o 1º frame de vídeo é semeado
// com o timestamp real (elapsed×90000) a partir dele, ancorando o vídeo no mesmo eixo
// de tempo do áudio (evita o congelamento por overflow do int32 no receptor).
func (p *Pipeline) SetClock(c *media.MediaClock) {
	p.mu.Lock()
	p.clock = c
	p.mu.Unlock()
}

func (p *Pipeline) Setup(callID, ourDeviceJid, peerDeviceJid string, sendKM, recvKM core.SrtpKeyingMaterial) error {
	srtp, err := media.NewSrtpSession(sendKM, recvKM, core.SRTPSendAuthTagLen, core.SRTPRecvAuthTagLen)
	if err != nil {
		return err
	}
	selfSsrc := media.GenerateSecureSsrc(callID, ourDeviceJid, slotWord)

	selfSsrcs := make([]uint32, len(callSlots))
	peerSsrcs := make([]uint32, len(callSlots))
	for i, slot := range callSlots {
		selfSsrcs[i] = media.GenerateSecureSsrc(callID, ourDeviceJid, slot)
		peerSsrcs[i] = media.GenerateSecureSsrc(callID, peerDeviceJid, slot)
	}

	p.mu.Lock()
	p.srtp = srtp
	p.selfSsrc = selfSsrc
	p.rtp = media.NewH264Session(selfSsrc)
	if p.depack == nil {
		p.depack = &transport.H264Depacketizer{}
	}
	p.frameNumber = 1
	p.transportSeq = 0
	p.keyframeRequired = true
	p.lastAUAt = time.Time{}
	p.setupRxLocked(peerSsrcs[2])
	p.mu.Unlock()

	p.relay.SetStreamSsrcs(selfSsrcs, peerSsrcs)
	p.log.Debug("video media set up", "self_video_ssrc", selfSsrc,
		"stream_ssrcs", len(selfSsrcs)+len(peerSsrcs))
	return nil
}

// SetupGroup configura o pipeline p/ chamada em GRUPO com SSRCs de vídeo EXPLÍCITOS
// (derivados no esquema de grupo) e material SRTP já derivado do epoch. Não chama
// SetStreamSsrcs (o relay de grupo gerencia a assinatura via Allocate). selfVideoSSRC
// pode ser 0 em pipelines só de recepção; peerVideoSSRC é usado p/ filtrar o stream.
func (p *Pipeline) SetupGroup(selfVideoSSRC uint32, sendKM, recvKM core.SrtpKeyingMaterial) error {
	srtp, err := media.NewSrtpSession(sendKM, recvKM, core.SRTPSendAuthTagLen, core.SRTPRecvAuthTagLen)
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.srtp = srtp
	p.selfSsrc = selfVideoSSRC
	p.rtp = media.NewH264Session(selfVideoSSRC)
	if p.depack == nil {
		p.depack = &transport.H264Depacketizer{}
	}
	p.frameNumber = 1
	p.transportSeq = 0
	p.keyframeRequired = true
	p.lastAUAt = time.Time{}
	p.mu.Unlock()
	return nil
}

// RekeyGroup troca as chaves SRTP (envio e recepção) pro epoch novo do grupo,
// preservando ROC/sequência e o resto do estado do pipeline.
func (p *Pipeline) RekeyGroup(sendKM, recvKM core.SrtpKeyingMaterial) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.srtp == nil {
		return nil
	}
	n, err := p.srtp.WithKeying(sendKM, recvKM)
	if err != nil {
		return err
	}
	p.srtp = n
	return nil
}

// videoExtProfile é o perfil da RTP header extension do vídeo do WhatsApp (0xDEBE,
// NÃO 0xBEDE). O conteúdo (one-byte headers) traz MediaFrameInfo, InitialBandwidth,
// ShortOffset e TransportSequence — não abs-send-time. Formato confirmado contra o
// vídeo real do WhatsApp e o meowcaller/whatsapp-rust.
const videoExtProfile = 0xDEBE

const (
	videoMediaFrameIDR   = 0x08 // frame é IDR/keyframe
	videoMediaFrameDelta = 0x20 // frame delta
)

// buildWhatsappVideoExt monta o bloco da extensão 0xDEBE (padded a múltiplo de 4).
// frameNumber só entra no PRIMEIRO pacote de cada access unit (ptr não-nil).
func buildWhatsappVideoExt(mediaFrameInfo uint8, frameNumber *uint16, transportSeq uint16) []byte {
	ext := make([]byte, 0, 16)
	if frameNumber != nil {
		ext = append(ext, 0x32, mediaFrameInfo, byte(*frameNumber>>8), byte(*frameNumber)) // id3, len3
	} else {
		ext = append(ext, 0x30, mediaFrameInfo) // id3, len1
	}
	ext = append(ext, 0x51, 0x00, 0x00)                                // id5 InitialBandwidth=0
	ext = append(ext, 0x61, 0x00, 0x00)                                // id6 ShortOffset=0
	ext = append(ext, 0x91, byte(transportSeq>>8), byte(transportSeq)) // id9 TransportSequence
	for len(ext)%4 != 0 {
		ext = append(ext, 0x00) // padding até fronteira de 4 bytes
	}
	return ext
}

func (p *Pipeline) FeedCaptured(au []byte) {
	p.mu.Lock()
	rtp, srtp := p.rtp, p.srtp
	keyframeRequired := p.keyframeRequired
	frameNum := p.frameNumber
	tseq := p.transportSeq
	first := p.lastAUAt.IsZero()
	clock := p.clock
	p.mu.Unlock()
	if rtp == nil || srtp == nil || !p.relay.HasConnection() || len(au) == 0 {
		return
	}
	nalus := transport.SplitAnnexB(au)
	if len(nalus) == 0 {
		return
	}
	idr := false
	for _, n := range nalus {
		if len(n) > 0 && n[0]&0x1f == 5 { // NAL tipo 5 = IDR
			idr = true
			break
		}
	}
	// O primeiro frame enviado precisa ser keyframe, senão o peer não decodifica.
	if keyframeRequired && !idr {
		return
	}
	// Junta TODOS os NALUs (menos AUD tipo 9) num único access unit Annex-B e fragmenta
	// como UM NAL — é assim que o WhatsApp manda o vídeo (não NALU a NALU).
	var packed []byte
	for _, n := range nalus {
		if len(n) == 0 || n[0]&0x1f == 9 {
			continue
		}
		if len(packed) > 0 {
			packed = append(packed, 0, 0, 0, 1)
		}
		packed = append(packed, n...)
	}
	if len(packed) == 0 {
		return
	}
	if p.relay.BufferedAmount() > congestionDropBytes {
		return
	}
	if first {
		// Ancora o 1º frame de vídeo no relógio de mídia da call (elapsed×90000), pro
		// vídeo ficar no mesmo eixo de tempo do áudio mesmo num upgrade tardio.
		if clock != nil {
			rtp.SetTimestamp(clock.TimestampFor(90000))
		}
	} else {
		rtp.AdvanceTimestamp(rtpStepSamples)
	}
	mediaFrameInfo := uint8(videoMediaFrameDelta)
	if idr {
		mediaFrameInfo = videoMediaFrameIDR
	}
	payloads := transport.PackageH264NALU(packed)
	for i, payload := range payloads {
		last := i == len(payloads)-1
		var fn *uint16
		if i == 0 {
			v := frameNum
			fn = &v // FrameNumber só no primeiro pacote do access unit
		}
		pkt := rtp.CreatePacketWithDuration(payload, 0, last)
		pkt.Header.Extension = true
		pkt.Header.ExtensionProfile = videoExtProfile
		pkt.Header.ExtensionData = buildWhatsappVideoExt(mediaFrameInfo, fn, tseq)
		tseq++
		if protected, err := srtp.Protect(pkt); err == nil {
			p.relay.Broadcast(protected)
		}
	}

	p.mu.Lock()
	p.lastAUAt = time.Now()
	p.transportSeq = tseq
	p.frameNumber = frameNum + 1
	if idr {
		p.keyframeRequired = false
	}
	p.stats.PacketsSent += uint32(len(payloads))
	p.stats.OctetsSent += uint32(len(packed))
	p.stats.RtpTimestamp = rtp.Timestamp()
	p.mu.Unlock()
}

// SenderStats devolve os contadores de envio (pacotes/octetos/último timestamp RTP).
func (p *Pipeline) SenderStats() media.RtcpSenderStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

func (p *Pipeline) HandleRelayData(data []byte) {
	p.mu.Lock()
	if p.srtp == nil || p.depack == nil {
		p.mu.Unlock()
		return
	}
	if media.RTPSsrc(data) == p.selfSsrc {
		p.mu.Unlock()
		return
	}
	if p.rx.enabled {
		d := p.handleRobustLocked(data, time.Now())
		p.mu.Unlock()
		p.dispatch(d)
		return
	}

	// Caminho legado (grupo): sem reordenação; depack/srtp sob o mutex.
	pkt, err := p.srtp.Unprotect(data)
	if err != nil {
		p.mu.Unlock()
		p.log.Debug("video srtp unprotect error", "err", err)
		return
	}
	if len(pkt.Payload) == 0 {
		p.mu.Unlock()
		return
	}
	var d deliveries
	p.consumeLocked(pkt, &d)
	p.mu.Unlock()
	p.dispatch(d)
}

// deliveries são os efeitos colaterais colhidos sob o mutex e executados fora dele.
type deliveries struct {
	frames    [][]byte
	orient    int
	orientCb  func(int)
	pliSsrc   uint32
	pliNeeded bool
}

func (p *Pipeline) dispatch(d deliveries) {
	if d.orientCb != nil {
		d.orientCb(d.orient)
	}
	p.mu.Lock()
	cb, req := p.OnFrame, p.OnKeyframeRequest
	p.mu.Unlock()
	if cb != nil {
		for _, f := range d.frames {
			cb(f)
		}
	}
	if d.pliNeeded && req != nil {
		req(d.pliSsrc)
	}
}

// consumeLocked despacota um pacote já decifrado e, no marker, fecha o access unit.
func (p *Pipeline) consumeLocked(pkt *media.RtpPacket, d *deliveries) {
	nalus := p.depack.Depacketize(pkt.Payload)
	if orient, ok := parseVideoOrientation(pkt.Header); ok && orient != p.lastOrientation {
		p.lastOrientation = orient
		d.orient, d.orientCb = orient, p.OnOrientation
	}
	for _, nalu := range nalus {
		p.frameBuf = append(p.frameBuf, annexBStartCode...)
		p.frameBuf = append(p.frameBuf, nalu...)
	}
	if pkt.Header.Marker && len(p.frameBuf) > 0 {
		frame := p.frameBuf
		p.frameBuf = nil
		p.frameDoneLocked(frame, d)
	}
}

func (p *Pipeline) Reset() {
	p.mu.Lock()
	p.rtp = nil
	p.srtp = nil
	p.selfSsrc = 0
	p.depack = nil
	p.frameBuf = nil
	p.lastAUAt = time.Time{}
	p.frameNumber = 1
	p.transportSeq = 0
	p.keyframeRequired = true
	p.stats = media.RtcpSenderStats{}
	p.lastOrientation = -1
	p.resetRxLocked()
	p.mu.Unlock()
}
