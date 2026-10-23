package video

import (
	"fmt"
	"math/bits"
	"sort"
	"strings"
	"time"

	"wacalls/internal/voip/media"
	"wacalls/internal/voip/transport"
)

// Escolha do fluxo de vídeo do peer no 1:1.
//
// A chamada toca em vários aparelhos do contato e chegam pacotes de vídeo de mais
// de um SSRC (aparelho que fez preaccept e não atendeu, troca de fluxo ao atender).
// A recepção não autentica o SRTP, então um fluxo com outra chave decifra lixo sem
// erro, e o SSRC derivado nem sempre bate com o que chega. Por isso cada SSRC
// ganha uma nota pelos últimos payloads que têm cara de H.264, e a recepção:
//   - trava no 1º pacote com cara de H.264;
//   - troca para outro de nota boa se o travado ficar mudo (ssrcSilenceRelock) ou
//     passar a decifrar lixo.

// ssrcSilenceRelock: o SSRC travado ficou mudo por este tempo enquanto outro
// chega, então o celular trocou de fluxo. Com poucos quadros/s, pausas de ~300 ms
// são normais. Variável para os testes.
var ssrcSilenceRelock = time.Second

// defaultIDRWait: quanto segurar os quadros esperando um IDR depois de uma lacuna
// (WACALLS_1A1_IDR_WAIT_MS). Passado isso, volta a entregar mesmo sem IDR.
const defaultIDRWait = 300 * time.Millisecond

const (
	maxCands   = 32
	maxPending = 32 // pacotes guardados por fluxo ainda não travado
)

type ssrcCand struct {
	hist    uint8 // 1 = payload com cara de H.264, nos últimos 8 pacotes
	n       uint64
	valid   uint64
	last    time.Time
	pending []*media.RtpPacket
}

// keep guarda o pacote de um fluxo não travado (se ele for travado, entra).
func (c *ssrcCand) keep(pkt *media.RtpPacket) {
	if len(c.pending) >= maxPending {
		c.pending = c.pending[1:]
	}
	c.pending = append(c.pending, pkt)
}

func (c *ssrcCand) good() bool { return c.n >= 5 && bits.OnesCount8(c.hist&0x1f) >= 4 }
func (c *ssrcCand) bad() bool  { return c.n >= 8 && bits.OnesCount8(c.hist) <= 2 }

// plausibleH264 diz se o payload RTP decifrado parece H.264 (RFC 6184): bit F
// zerado e tipo de NAL que o celular manda (fatia, IDR, SEI, SPS, PPS, AUD, STAP-A,
// FU-A de fatia/IDR). Um payload decifrado com a chave errada passa em ~11%.
func plausibleH264(pl []byte) bool {
	if len(pl) < 2 || pl[0]&0x80 != 0 {
		return false
	}
	switch pl[0] & 0x1f {
	case 1, 5, 6, 7, 8, 9, 24:
		return true
	case 28:
		fu := pl[1]
		if fu&0xc0 == 0xc0 { // início e fim no mesmo fragmento: inválido
			return false
		}
		t := fu & 0x1f
		return t == 1 || t == 5
	}
	return false
}

func (p *Pipeline) noteCandLocked(ssrc uint32, payload []byte, now time.Time) *ssrcCand {
	c := p.rx.cands[ssrc]
	if c == nil {
		if len(p.rx.cands) >= maxCands {
			for k := range p.rx.cands {
				if k != p.rx.ssrc {
					delete(p.rx.cands, k)
				}
			}
		}
		c = &ssrcCand{}
		p.rx.cands[ssrc] = c
	}
	ok := plausibleH264(payload)
	c.hist <<= 1
	if ok {
		c.hist |= 1
		c.valid++
	}
	c.n++
	c.last = now
	return c
}

// shouldLockLocked: o pacote é de um SSRC diferente do travado; trava nele?
func (p *Pipeline) shouldLockLocked(c *ssrcCand, now time.Time) bool {
	if p.rx.ssrc == 0 {
		// 1º travamento: o 1º pacote com cara de H.264. Se for lixo que passou
		// (~11%), a nota dele cai em poucos pacotes e o fluxo bom assume.
		return c.hist&1 == 1
	}
	if !c.good() {
		return false
	}
	if now.Sub(p.rx.lastLocked) >= ssrcSilenceRelock {
		return true
	}
	cur := p.rx.cands[p.rx.ssrc]
	return cur != nil && cur.bad()
}

// lockLocked trava (ou retrava) no SSRC: descarta o estado do fluxo anterior e
// espera um IDR do novo (o PLI sai para ele).
func (p *Pipeline) lockLocked(ssrc uint32, c *ssrcCand, now time.Time) {
	if p.rx.ssrc == 0 {
		p.log.Info("video 1:1: SSRC do peer travado", "ssrc", ssrc, "derivado", p.rx.derivedSsrc,
			"bate", ssrc == p.rx.derivedSsrc, "pacotes_vistos", c.n)
	} else {
		p.rx.Relocks++
		old := p.rx.cands[p.rx.ssrc]
		mudo, ruim := now.Sub(p.rx.lastLocked).Milliseconds(), old != nil && old.bad()
		p.log.Info("video 1:1: SSRC do peer mudou; retravando", "de", p.rx.ssrc, "para", ssrc,
			"mudo_ms", mudo, "antigo_lixo", ruim)
		p.rx.waitIDR = true
		p.rx.waitSince = now
	}
	if p.rx.timer != nil {
		p.rx.timer.Stop()
		p.rx.timer = nil
	}
	p.rx.reorder = transport.NewRtpReorderer[*media.RtpPacket](p.rx.window, 512)
	p.rx.stats = media.NewRtpRxStats(90000)
	p.depack.Reset()
	p.frameBuf = nil
	p.rx.skipToMarker = false
	p.rx.lastPLI = time.Time{}
	p.rx.ssrc = ssrc
}

// diagLocked registra os contadores da recepção a cada rxDiagInterval.
func (p *Pipeline) diagLocked(now time.Time) {
	if p.rx.lastDiag.IsZero() {
		p.rx.lastDiag = now
		return
	}
	if now.Sub(p.rx.lastDiag) < rxDiagInterval {
		return
	}
	p.rx.lastDiag = now
	p.log.Info("video 1:1: recepção", "ssrc", p.rx.ssrc, "pacotes", p.rx.Pkts, "quadros", p.rx.Frames,
		"idrs", p.rx.IDRs, "lacunas", p.rx.Gaps, "descartados", p.rx.Dropped, "desistiu_idr", p.rx.GaveUp,
		"outro_ssrc", p.rx.Foreign, "retravas", p.rx.Relocks, "esperando_idr", p.rx.waitIDR,
		"fluxos", p.candSummaryLocked())
}

// candSummaryLocked: "ssrc:pacotes/%h264" dos fluxos mais ativos.
func (p *Pipeline) candSummaryLocked() string {
	type kv struct {
		ssrc uint32
		c    *ssrcCand
	}
	all := make([]kv, 0, len(p.rx.cands))
	for k, c := range p.rx.cands {
		all = append(all, kv{k, c})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].c.n > all[j].c.n })
	var b strings.Builder
	for i, e := range all {
		if i == 4 {
			break
		}
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%d:%d/%d%%", e.ssrc, e.c.n, e.c.valid*100/e.c.n)
	}
	return b.String()
}

// RxNative devolve bytes de payload recebidos e a taxa (bit/s) do vídeo do peer,
// para a extensão do SR nativo (ok=false sem recepção robusta ou sem dados).
func (p *Pipeline) RxNative(now time.Time) (bytesTotal, rateBps uint32, ok bool) {
	p.mu.Lock()
	st := p.rx.stats
	p.mu.Unlock()
	if st == nil {
		return 0, 0, false
	}
	b, r := st.Native(now)
	return b, r, true
}

// PeerSsrc devolve o SSRC de vídeo do peer travado (0 = nenhum ainda).
func (p *Pipeline) PeerSsrc() uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rx.ssrc
}

func sinceMs(t time.Time) int64 {
	if t.IsZero() {
		return -1
	}
	return time.Since(t).Milliseconds()
}
