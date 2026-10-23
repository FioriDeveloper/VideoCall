// analisar-captura lê uma captura de chamada 1:1 (internal/voip/captura) e mostra
// o que o celular mandou e o que o nosso servidor faria com isso: duplicados, fora
// de ordem, supressão de silêncio x perda, silêncio inserido conforme a janela de
// reordenação do áudio, quadros por segundo e congelamentos do vídeo (se nascem no
// celular ou no caminho), e o RTCP do celular ao longo do tempo.
//
//	go run ./cmd/analisar-captura arquivo.cap
package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"wacalls/internal/voip/media"
	"wacalls/internal/voip/transport"
)

type rec struct {
	at   time.Time
	tipo byte
	pkt  *media.RtpPacket
	raw  []byte
}

func ler(nome string) ([]rec, error) {
	b, err := os.ReadFile(nome)
	if err != nil {
		return nil, err
	}
	var out []rec
	for off := 0; off+13 <= len(b); {
		at := time.Unix(0, int64(binary.BigEndian.Uint64(b[off:off+8])))
		tipo := b[off+8]
		n := int(binary.BigEndian.Uint32(b[off+9 : off+13]))
		off += 13
		if off+n > len(b) {
			return out, io.ErrUnexpectedEOF
		}
		data := b[off : off+n]
		off += n
		r := rec{at: at, tipo: tipo, raw: data}
		if tipo == 'a' || tipo == 'v' {
			h, err := media.DecodeRtpHeader(data)
			if err != nil {
				continue
			}
			r.pkt = &media.RtpPacket{Header: h, Payload: data[h.Size():]}
		}
		out = append(out, r)
	}
	return out, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "uso: analisar-captura arquivo.cap")
		os.Exit(2)
	}
	rs, err := ler(os.Args[1])
	if err != nil && len(rs) == 0 {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(rs) == 0 {
		fmt.Println("captura vazia")
		return
	}
	t0 := rs[0].at
	fmt.Printf("captura: %d registros, %.1f s\n\n", len(rs), rs[len(rs)-1].at.Sub(t0).Seconds())
	audio(rs)
	video(rs, t0)
	rtcp(rs, t0)
}

// unicos devolve a primeira cópia de cada (ssrc, seq), na ordem de chegada, e conta
// duplicados (os 2 relays) e pacotes fora de ordem.
func unicos(rs []rec, tipo byte) (first []rec, dups, fora int, ssrcs map[uint32]int) {
	seen := map[uint64]bool{}
	ssrcs = map[uint32]int{}
	maxSeq := map[uint32]uint16{}
	have := map[uint32]bool{}
	for _, r := range rs {
		if r.tipo != tipo || r.pkt == nil {
			continue
		}
		h := r.pkt.Header
		ssrcs[h.Ssrc]++
		k := uint64(h.Ssrc)<<16 | uint64(h.SequenceNumber)
		if seen[k] {
			dups++
			continue
		}
		seen[k] = true
		if have[h.Ssrc] && int16(h.SequenceNumber-maxSeq[h.Ssrc]) < 0 {
			fora++
		} else {
			maxSeq[h.Ssrc], have[h.Ssrc] = h.SequenceNumber, true
		}
		first = append(first, r)
	}
	return
}

func dominante(m map[uint32]int) uint32 {
	var best uint32
	n := -1
	for k, v := range m {
		if v > n {
			best, n = k, v
		}
	}
	return best
}

func audio(rs []rec) {
	first, dups, fora, ssrcs := unicos(rs, 'a')
	if len(first) == 0 {
		fmt.Println("ÁUDIO: nenhum pacote")
		return
	}
	ss := dominante(ssrcs)
	var pk []rec
	for _, r := range first {
		if r.pkt.Header.Ssrc == ss {
			pk = append(pk, r)
		}
	}
	fmt.Printf("ÁUDIO (ssrc %d, %d ssrcs vistos): %d únicos, %d duplicados, %d fora de ordem\n", ss, len(ssrcs), len(pk), dups, fora)
	ord := append([]rec(nil), pk...)
	sort.SliceStable(ord, func(i, j int) bool {
		return int16(ord[i].pkt.Header.SequenceNumber-ord[j].pkt.Header.SequenceNumber) < 0
	})
	sub := map[int]int{}
	var perdidos, dtxMs, durMs int
	for i, r := range ord {
		nf := media.MLowSubFrames(r.pkt.Payload)
		sub[nf]++
		durMs += nf * 60
		if i == 0 {
			continue
		}
		p := ord[i-1]
		dseq := int(uint16(r.pkt.Header.SequenceNumber - p.pkt.Header.SequenceNumber))
		dts := int(r.pkt.Header.Timestamp-p.pkt.Header.Timestamp) / 16 // ms a 16 kHz
		esperado := media.MLowSubFrames(p.pkt.Payload) * 60
		if dseq > 1 {
			perdidos += dseq - 1
		} else if dts > esperado {
			dtxMs += dts - esperado // pausa sem perda: supressão de silêncio do celular
		}
	}
	fmt.Printf("  frames por pacote: %v; áudio útil %.1f s; perdidos %d; pausa sem perda (supressão de silêncio do celular) %.1f s\n",
		sub, float64(durMs)/1000, perdidos, float64(dtxMs)/1000)
	var gaps []float64
	for i := 1; i < len(pk); i++ {
		gaps = append(gaps, pk[i].at.Sub(pk[i-1].at).Seconds()*1000)
	}
	fmt.Printf("  intervalo entre chegadas (ms): %s\n", resumo(gaps))
	for _, w := range []int{0, 60, 120, 200, 300} {
		sil, late := simulaAudio(pk, time.Duration(w)*time.Millisecond)
		fmt.Printf("  janela %3d ms: atrasados descartados %d, silêncio inserido %.1f s (fora a supressão do celular)\n",
			w, late, float64(sil)/1000-float64(dtxMs)/1000)
	}
	fmt.Println()
}

// simulaAudio reproduz o caminho do servidor (dedup já feito, reordenação com janela
// w, descarte de atrasado, silêncio no buraco) e devolve ms de silêncio e atrasados.
func simulaAudio(pk []rec, w time.Duration) (silMs, late int) {
	reo := transport.NewRtpReorderer[*media.RtpPacket](w, 256)
	var base uint32
	var played int64
	set := false
	tocar := func(p *media.RtpPacket) {
		n := int64(media.MLowSubFrames(p.Payload) * 960)
		if !set {
			set, base, played = true, p.Header.Timestamp, n
			return
		}
		d := int64(int32(p.Header.Timestamp-base)) - played
		if d < 0 && d > -8000 {
			late++
			return
		}
		if d < 0 || d > 8000 {
			base, played = p.Header.Timestamp, n
			return
		}
		silMs += int(d / 16)
		played += d + n
	}
	for _, r := range pk {
		if w == 0 {
			tocar(r.pkt)
			continue
		}
		for _, o := range reo.Flush(r.at) {
			tocar(o.Item)
		}
		for _, o := range reo.Push(r.pkt.Header.SequenceNumber, r.pkt, r.at) {
			tocar(o.Item)
		}
	}
	if w > 0 && len(pk) > 0 {
		for _, o := range reo.Flush(pk[len(pk)-1].at.Add(time.Hour)) {
			tocar(o.Item)
		}
	}
	return
}

type quadro struct {
	chegou time.Time
	ts     uint32
	bytes  int
	idr    bool
}

func video(rs []rec, t0 time.Time) {
	first, dups, fora, ssrcs := unicos(rs, 'v')
	if len(first) == 0 {
		fmt.Println("VÍDEO: nenhum pacote")
		return
	}
	ss := dominante(ssrcs)
	var qs []quadro
	cur := quadro{}
	for _, r := range first {
		h := r.pkt.Header
		if h.Ssrc != ss {
			continue
		}
		cur.bytes += len(r.pkt.Payload)
		cur.ts = h.Timestamp
		if pl := r.pkt.Payload; len(pl) > 1 {
			t := pl[0] & 0x1f
			if t == 5 || t == 7 || (t == 28 && pl[1]&0x1f == 5) {
				cur.idr = true
			}
		}
		if h.Marker {
			cur.chegou = r.at
			qs = append(qs, cur)
			cur = quadro{}
		}
	}
	fmt.Printf("VÍDEO (ssrc %d, %d ssrcs vistos): %d pacotes únicos, %d duplicados, %d fora de ordem, %d quadros\n",
		ss, len(ssrcs), len(first), dups, fora, len(qs))
	if len(qs) < 2 {
		return
	}
	dur := qs[len(qs)-1].chegou.Sub(qs[0].chegou).Seconds()
	var bytes, idr int
	for _, q := range qs {
		bytes += q.bytes
		if q.idr {
			idr++
		}
	}
	fmt.Printf("  %.1f quadros/s, %.0f kbps, %d IDR (1 a cada %.1f s)\n",
		float64(len(qs))/dur, float64(bytes)*8/dur/1000, idr, dur/float64(max(idr, 1)))
	var capt, cheg []float64
	congCel, congCam := 0, 0
	for i := 1; i < len(qs); i++ {
		c := float64(qs[i].ts-qs[i-1].ts) / 90 // ms entre capturas no celular
		a := qs[i].chegou.Sub(qs[i-1].chegou).Seconds() * 1000
		capt = append(capt, c)
		cheg = append(cheg, a)
		if a > 400 {
			if c > 300 {
				congCel++ // o próprio celular ficou sem capturar/mandar quadro
			} else {
				congCam++ // capturado em ritmo, mas chegou atrasado: caminho
			}
		}
	}
	fmt.Printf("  intervalo entre capturas no celular (ms): %s\n", resumo(capt))
	fmt.Printf("  intervalo entre chegadas aqui (ms):       %s\n", resumo(cheg))
	fmt.Printf("  congelamentos > 400 ms: %d nascem no celular, %d no caminho\n", congCel, congCam)
	porS := map[int]int{}
	for _, q := range qs {
		porS[int(q.chegou.Sub(t0).Seconds())]++
	}
	var ks []int
	for k := range porS {
		ks = append(ks, k)
	}
	sort.Ints(ks)
	fmt.Print("  quadros por segundo ao longo da chamada:")
	for _, k := range ks {
		fmt.Printf(" %d", porS[k])
	}
	fmt.Println()
	fmt.Println()
}

func rtcp(rs []rec, t0 time.Time) {
	tipos := map[[2]byte]int{}
	fmt.Println("RTCP do celular:")
	for _, r := range rs {
		if r.tipo != 'r' {
			continue
		}
		p := r.raw
		for off := 0; off+4 <= len(p); {
			l := (int(binary.BigEndian.Uint16(p[off+2:off+4])) + 1) * 4
			if l < 4 || off+l > len(p) {
				break
			}
			tipos[[2]byte{p[off+1], p[off] & 0x1f}]++
			off += l
		}
		for _, v := range media.ParsePrst(p) {
			fmt.Printf("  %5.1f s  PRST %d\n", r.at.Sub(t0).Seconds(), v)
		}
		for _, pr := range media.ParsePeerReceptions(p) {
			if pr.HasExt {
				fmt.Printf("  %5.1f s  SR sobre %d: perda %d/256, jitter %d, w0 %d w1 %d w2 %d w3 %d w4 %d w5 %d\n",
					r.at.Sub(t0).Seconds(), pr.About, pr.FractionLost, pr.Jitter, pr.Ext[0], pr.Ext[1], pr.Ext[2], pr.Ext[3], pr.Ext[4], pr.Ext[5])
			}
		}
	}
	fmt.Printf("  tipos (pt, fmt|rc): %v\n", tipos)
}

func resumo(v []float64) string {
	if len(v) == 0 {
		return "-"
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	q := func(p float64) float64 { return s[int(p*float64(len(s)-1))] }
	return fmt.Sprintf("mediana %.0f, p90 %.0f, p99 %.0f, máx %.0f (n=%d)", q(0.5), q(0.9), q(0.99), s[len(s)-1], len(s))
}
