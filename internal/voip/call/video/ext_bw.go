package video

import (
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"wacalls/internal/voip/media"
)

// Campo id5 da extensão 0xDEBE do vídeo ("InitialBandwidth").
//
// A config de VoIP que o WhatsApp manda em cada chamada traz disable_rtcp_remb=1,
// enable_fast_remb=1, fast_remb_in_video_rtp=1 e fast_remb_send_interval_ms=1000:
// o retorno de banda vai DENTRO do RTP de vídeo, não em RTCP. SUPOSIÇÃO: é o id5,
// que mandávamos sempre 0 (o celular ficava no mínimo, minbwe=35000).
//
// WACALLS_1A1_ID5: vazio/0 = 0 (legado); um número = esse valor bruto (16 bits);
// "espelho" = o maior id5 que o celular mandou nos últimos ~5 s (unidade igual à
// dele, seja qual for).

// Fast RR no elemento id13 da extensão 0xDEBE do nosso vídeo: o retorno de banda
// que o celular usa no 1:1 (fast_remb_in_video_rtp=1, REMB por RTCP desligado).
//
// Formato igual ao que o celular manda no vídeo dele (produção, 23/10/2026:
// 09 0247f3 00019e10 03) e ao modo rbe_and_pp do motor VoIP do WhatsApp Web:
// flags 0x09 (0x01 rbe, 0x08 link_cap_est) + rbe u24 BE (bps) + link_cap_est u32
// BE (bps) + estado u8 (3), elemento de 9 B (cabeçalho 0xD8). Vai em UM pacote
// por segundo (fast_rr_send_interval_ms), no 1º pacote do quadro: um teste com
// 4 B em todo pacote fez o celular rejeitar o nosso vídeo.
//
// WACALLS_1A1_FAST_RR_BPS (rbe; 0 = desligado), WACALLS_1A1_FAST_RR_LCE
// (link_cap_est; padrão o maior entre rbe e 2000000).
const fastRRInterval = time.Second

var fastRRBps = func() func() uint32 {
	n, err := strconv.ParseUint(strings.TrimSpace(os.Getenv("WACALLS_1A1_FAST_RR_BPS")), 10, 32)
	if err != nil || n > 0xffffff {
		return func() uint32 { return 0 }
	}
	v := uint32(n)
	return func() uint32 { return v }
}()

var fastRRLce = func() uint32 {
	n, err := strconv.ParseUint(strings.TrimSpace(os.Getenv("WACALLS_1A1_FAST_RR_LCE")), 10, 32)
	if err != nil {
		return 0
	}
	return uint32(n)
}()

func fastRRLinkCap(rbe uint32) uint32 {
	if fastRRLce > 0 {
		return fastRRLce
	}
	if rbe > 2_000_000 {
		return rbe
	}
	return 2_000_000
}

// fastRRElement devolve o elemento id13 completo (cabeçalho + 9 B).
func fastRRElement(rbe, linkCap uint32) []byte {
	return []byte{0xD8, 0x09, byte(rbe >> 16), byte(rbe >> 8), byte(rbe),
		byte(linkCap >> 24), byte(linkCap >> 16), byte(linkCap >> 8), byte(linkCap), 0x03}
}

var (
	id5Mirror  atomic.Uint32 // maior id5 do celular na janela (espelho)
	id5ModeVal = func() string { return strings.TrimSpace(os.Getenv("WACALLS_1A1_ID5")) }()
)

func id5Value() uint16 {
	switch id5ModeVal {
	case "", "0":
		return 0
	case "espelho":
		return uint16(id5Mirror.Load())
	}
	if n, err := strconv.ParseUint(id5ModeVal, 10, 16); err == nil {
		return uint16(n)
	}
	return 0
}

// rxExtStats registra os elementos da extensão do vídeo do celular (diagnóstico).
type rxExtStats struct {
	mu     sync.Mutex
	last   map[byte]string
	cnt    map[byte]uint64 // em quantos pacotes cada elemento apareceu
	min    map[byte]uint32
	max    map[byte]uint32
	n      uint64
	since  time.Time
	winMax uint32
}

func (p *Pipeline) noteRxExt(h *media.RtpHeader) {
	if h == nil || !h.Extension || h.ExtensionProfile != videoExtProfile {
		return
	}
	st := &p.rxExt
	st.mu.Lock()
	if st.last == nil {
		st.last, st.min, st.max = map[byte]string{}, map[byte]uint32{}, map[byte]uint32{}
		st.cnt = map[byte]uint64{}
		st.since = time.Now()
	}
	ext := h.ExtensionData
	for off := 0; off < len(ext); {
		hdr := ext[off]
		off++
		if hdr == 0 {
			continue
		}
		id, l := hdr>>4, int(hdr&0x0f)+1
		if id == 15 || off+l > len(ext) {
			break
		}
		v := ext[off : off+l]
		if id != 3 { // 3: quadro/orientação (o resto, inclusive o 9, vai para o log)
			st.cnt[id]++
			st.last[id] = hex.EncodeToString(v)
			var num uint32
			for _, b := range v {
				num = num<<8 | uint32(b)
			}
			if mn, ok := st.min[id]; !ok || num < mn {
				st.min[id] = num
			}
			if num > st.max[id] {
				st.max[id] = num
			}
			if id == 5 && num > st.winMax {
				st.winMax = num
			}
		}
		off += l
	}
	st.n++
	logIt := time.Since(st.since) >= 5*time.Second
	var line string
	if logIt {
		ids := make([]int, 0, len(st.last))
		for id := range st.last {
			ids = append(ids, int(id))
		}
		sort.Ints(ids)
		var b strings.Builder
		for _, i := range ids {
			id := byte(i)
			fmt.Fprintf(&b, "id%d=%s(min %d max %d, em %d pacotes) ", id, st.last[id], st.min[id], st.max[id], st.cnt[id])
		}
		line = b.String()
		id5Mirror.Store(st.winMax)
		st.winMax = 0
		st.last, st.min, st.max = map[byte]string{}, map[byte]uint32{}, map[byte]uint32{}
		st.cnt = map[byte]uint64{}
		st.since = time.Now()
	}
	n := st.n
	st.mu.Unlock()
	if logIt {
		p.log.Info("video 1:1: extensão do vídeo do celular (últimos 5 s)", "pacotes", n, "campos", line,
			"nosso_id5", id5Value(), "modo_id5", id5ModeVal)
	}
}
