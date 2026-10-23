package video

import (
	"bytes"
	"math/rand"
	"sync"
	"testing"
	"time"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
	"wacalls/internal/voip/transport"
)

type rxHarness struct {
	t      *testing.T
	p      *Pipeline
	tx     *media.SrtpSession
	rtp    *media.RtpSession
	mu     sync.Mutex
	frames [][]byte
	plis   []uint32
}

func newRxHarness(t *testing.T, ssrc uint32) *rxHarness {
	km := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	tx, err := media.NewSrtpSession(km, km, 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	rx, _ := media.NewSrtpSession(km, km, 4, 4)
	h := &rxHarness{t: t, tx: tx, rtp: media.NewH264Session(ssrc)}
	p := New(nil, &fakeRelay{})
	p.srtp = rx
	p.depack = &transport.H264Depacketizer{}
	p.selfSsrc = 1
	p.rx = rxState{
		enabled: true, pli: true, window: 5 * time.Millisecond,
		reorder: transport.NewRtpReorderer[*media.RtpPacket](5*time.Millisecond, 512),
		cands:   map[uint32]*ssrcCand{}, idrWait: defaultIDRWait,
	}
	p.OnFrame = func(au []byte) { h.mu.Lock(); h.frames = append(h.frames, au); h.mu.Unlock() }
	p.OnKeyframeRequest = func(s uint32) { h.mu.Lock(); h.plis = append(h.plis, s); h.mu.Unlock() }
	h.p = p
	return h
}

// au gera os pacotes SRTP de um access unit (1 NAL de tipo nalType, size bytes).
func (h *rxHarness) au(nalType byte, size int) [][]byte {
	nalu := make([]byte, size)
	nalu[0] = 0x60 | nalType
	for i := 1; i < size; i++ {
		nalu[i] = byte(i)
	}
	pays := transport.PackageH264NALU(nalu)
	var out [][]byte
	for i, pl := range pays {
		pkt := h.rtp.CreatePacketWithDuration(pl, 0, i == len(pays)-1)
		b, err := h.tx.Protect(pkt)
		if err != nil {
			h.t.Fatal(err)
		}
		out = append(out, b)
	}
	return out
}

func (h *rxHarness) feed(pkts ...[]byte) {
	for _, b := range pkts {
		h.p.HandleRelayData(b)
	}
}

func (h *rxHarness) nFrames() int { h.mu.Lock(); defer h.mu.Unlock(); return len(h.frames) }
func (h *rxHarness) nPli() int    { h.mu.Lock(); defer h.mu.Unlock(); return len(h.plis) }

func TestRxReordersFragmentsAndDropsDuplicates(t *testing.T) {
	h := newRxHarness(t, 77)
	idr := h.au(5, 2500)
	if len(idr) != 4 {
		t.Fatalf("esperava 4 fragmentos, got %d", len(idr))
	}
	// ordem trocada + duplicados
	h.feed(idr[0], idr[2], idr[1], idr[1], idr[0], idr[3], idr[2], idr[3])
	if h.nFrames() != 1 {
		t.Fatalf("frames=%d, want 1", h.nFrames())
	}
	want := append([]byte{0, 0, 0, 1, 0x65}, func() []byte {
		b := make([]byte, 2499)
		for i := range b {
			b[i] = byte(i + 1)
		}
		return b
	}()...)
	if !bytes.Equal(h.frames[0], want) {
		t.Fatal("quadro reordenado difere do original")
	}
}

func TestRxGapDropsFrameUntilIDRAndRequestsPLI(t *testing.T) {
	h := newRxHarness(t, 77)
	h.feed(h.au(5, 2500)...)
	if h.nFrames() != 1 {
		t.Fatalf("IDR inicial não entregue")
	}
	h.p.mu.Lock()
	h.p.rx.lastPLI = time.Now().Add(-time.Second)
	h.p.mu.Unlock()
	base := h.nPli()

	p1 := h.au(1, 2500) // quadro P com um fragmento perdido
	h.feed(p1[0], p1[2], p1[3])
	time.Sleep(15 * time.Millisecond) // estoura a janela -> timer libera com lacuna
	if h.nFrames() != 1 {
		t.Fatalf("quadro corrompido foi entregue (frames=%d)", h.nFrames())
	}
	if h.nPli() <= base {
		t.Fatal("lacuna deveria disparar PLI")
	}

	h.feed(h.au(1, 800)...) // P seguinte: descartado até o IDR
	if h.nFrames() != 1 {
		t.Fatal("P antes do IDR não deveria ser entregue")
	}
	h.feed(h.au(5, 800)...)
	if h.nFrames() != 2 {
		t.Fatalf("IDR após lacuna deveria ser entregue (frames=%d)", h.nFrames())
	}
	h.feed(h.au(1, 800)...)
	if h.nFrames() != 3 {
		t.Fatal("P após o IDR deveria voltar a ser entregue")
	}
}

func TestRxFiltersForeignSSRCAndPLIBeforeFirstIDR(t *testing.T) {
	h := newRxHarness(t, 77)
	// P antes de IDR (em 2 pacotes: o 1º travamento pede 2 seguidos com cara de
	// H.264): entregue (comportamento legado), mas pede PLI.
	h.feed(h.au(1, 1600)...)
	if h.nPli() != 1 || h.plis[0] != 77 {
		t.Fatalf("PLI esperado ao SSRC 77, got %v", h.plis)
	}
	other := newRxHarness(t, 99)
	n := h.nFrames()
	h.feed(other.au(5, 800)...)
	if h.nFrames() != n {
		t.Fatal("SSRC estranho deveria ser descartado")
	}
}

func TestRxRelocksWhenLockedSsrcGoesSilent(t *testing.T) {
	old := ssrcSilenceRelock
	ssrcSilenceRelock = 20 * time.Millisecond
	defer func() { ssrcSilenceRelock = old }()

	h := newRxHarness(t, 77)
	h.feed(h.au(5, 1600)...) // 2+ pacotes: o 1º travamento pede 2 seguidos
	if h.nFrames() != 1 {
		t.Fatalf("1º IDR do SSRC 77: frames=%d", h.nFrames())
	}

	// Outro fluxo começa enquanto o 77 ainda fala: descartado (mas guardado).
	// Cada fluxo tem o seu cifrador (como cada aparelho): com um só, a sequência
	// aleatória do 2º fluxo podia virar o ROC de envio e o teste falhava às vezes.
	km := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	otx, _ := media.NewSrtpSession(km, km, 4, 4)
	other := &rxHarness{t: t, tx: otx, rtp: media.NewH264Session(88)}
	h.feed(other.au(5, 300)...)
	if h.nFrames() != 1 || h.p.rx.Foreign == 0 {
		t.Fatalf("fluxo 88 com 77 ativo devia ser descartado: frames=%d foreign=%d", h.nFrames(), h.p.rx.Foreign)
	}

	// O 77 fica mudo: quando o 88 ganha nota (5 pacotes), assume; os guardados
	// (o IDR dele inclusive) entram em ordem.
	time.Sleep(30 * time.Millisecond)
	for i := 0; i < 4; i++ {
		h.feed(other.au(1, 200)...)
	}
	if h.p.rx.ssrc != 88 || h.p.rx.Relocks != 1 {
		t.Fatalf("devia retravar no 88: ssrc=%d relocks=%d", h.p.rx.ssrc, h.p.rx.Relocks)
	}
	if h.nFrames() != 6 {
		t.Fatalf("IDR guardado + 4 P do 88 deviam ser entregues: frames=%d", h.nFrames())
	}
}

func TestRxGarbageStreamLockedFirstIsReplaced(t *testing.T) {
	h := newRxHarness(t, 77)
	// Fluxo de outro aparelho: mesma sessão RTP, chave diferente -> decifra lixo.
	km := core.SrtpKeyingMaterial{MasterKey: bytes.Repeat([]byte{7}, 16), MasterSalt: bytes.Repeat([]byte{3}, 14)}
	gtx, _ := media.NewSrtpSession(km, km, 4, 4)
	g := &rxHarness{t: t, tx: gtx, rtp: media.NewH264Session(55)}
	// Força o pior caso: o lixo trava primeiro.
	h.p.mu.Lock()
	h.p.rx.ssrc = 55
	h.p.rx.cands[55] = &ssrcCand{n: 1, hist: 1}
	h.p.rx.lastLocked = time.Now()
	h.p.mu.Unlock()
	// O lixo passa como H.264 em ~11% dos pacotes: com poucos pacotes a nota dele
	// às vezes não caía (o teste falhava às vezes). 30 deixa isso desprezível.
	for i := 0; i < 30; i++ {
		h.feed(g.au(1, 200)...)
		h.feed(h.au(1, 200)...)
	}
	h.feed(h.au(5, 300)...)
	if h.p.rx.ssrc != 77 {
		t.Fatalf("o fluxo bom devia assumir: ssrc=%d fluxos=%s", h.p.rx.ssrc, h.p.candSummaryLocked())
	}
	if good, _ := h.countFrames(); good == 0 {
		t.Fatal("nenhum quadro do fluxo bom entregue")
	}
}

func TestRxGivesUpWaitingForIDR(t *testing.T) {
	h := newRxHarness(t, 77)
	h.p.rx.idrWait = 30 * time.Millisecond
	h.feed(h.au(5, 300)...)
	p1 := h.au(1, 2500)
	h.feed(p1[0], p1[2], p1[3]) // lacuna
	time.Sleep(15 * time.Millisecond)
	h.feed(h.au(1, 200)...)
	n := h.nFrames()
	if n != 1 {
		t.Fatalf("logo após a lacuna os P deviam esperar o IDR: frames=%d", n)
	}
	time.Sleep(40 * time.Millisecond)
	h.feed(h.au(1, 200)...)
	if h.nFrames() != 2 || h.p.rx.GaveUp != 1 {
		t.Fatalf("passada a espera, o P devia ser entregue: frames=%d desistiu=%d", h.nFrames(), h.p.rx.GaveUp)
	}
}

// Chamada simulada: 5 quadros/s, IDR a cada 6, cada pacote chega pelos 2 relays
// com atraso aleatório (até 90 ms) e 3% de perda por cópia, mais um fluxo de lixo
// de outro aparelho no começo. Entregas concorrentes (rode com -race).
func TestRxSimulatedCall(t *testing.T) {
	h := newRxHarness(t, 77)
	h.p.mu.Lock()
	h.p.rx.window = defaultReorderWindow
	h.p.rx.reorder = transport.NewRtpReorderer[*media.RtpPacket](defaultReorderWindow, 512)
	h.p.mu.Unlock()
	km := core.SrtpKeyingMaterial{MasterKey: bytes.Repeat([]byte{9}, 16), MasterSalt: bytes.Repeat([]byte{1}, 14)}
	gtx, _ := media.NewSrtpSession(km, km, 4, 4)
	g := &rxHarness{t: t, tx: gtx, rtp: media.NewH264Session(55)}

	rng := rand.New(rand.NewSource(42))
	var wg sync.WaitGroup
	send := func(b []byte, at time.Duration) {
		wg.Add(1)
		time.AfterFunc(at, func() { defer wg.Done(); h.p.HandleRelayData(b) })
	}
	const nFrames = 12
	for i := 0; i < 5; i++ { // lixo antes e durante o começo
		for _, b := range g.au(1, 300) {
			send(b, time.Duration(i*40)*time.Millisecond)
		}
	}
	for f := 0; f < nFrames; f++ {
		typ := byte(1)
		if f%6 == 0 {
			typ = 5
		}
		base := time.Duration(f*200) * time.Millisecond
		for _, b := range h.au(typ, 1500+rng.Intn(2500)) {
			for copy := 0; copy < 2; copy++ {
				if rng.Float64() < 0.03 {
					continue
				}
				send(b, base+time.Duration(rng.Intn(90))*time.Millisecond)
			}
		}
	}
	wg.Wait()
	time.Sleep(100 * time.Millisecond) // timers de flush
	h.p.mu.Lock()
	ssrc, sum, gaps := h.p.rx.ssrc, h.p.candSummaryLocked(), h.p.rx.Gaps
	h.p.mu.Unlock()
	if ssrc != 77 {
		t.Fatalf("travou no fluxo errado: %d (%s)", ssrc, sum)
	}
	good, bad := h.countFrames()
	t.Logf("quadros bons %d/%d, lixo %d, lacunas %d, fluxos %s", good, nFrames, bad, gaps, sum)
	if good < nFrames-3 {
		t.Fatalf("entregou poucos quadros: %d de %d", good, nFrames)
	}
	if bad > 2 {
		t.Fatalf("entregou %d quadros de lixo", bad)
	}
}

// countFrames separa os quadros do fluxo bom (conteúdo 1, 2, 3... gerado por au)
// dos de lixo.
func (h *rxHarness) countFrames() (good, bad int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, f := range h.frames {
		if len(f) > 6 && f[5] == 1 && f[6] == 2 {
			good++
		} else {
			bad++
		}
	}
	return
}

func TestId5Modes(t *testing.T) {
	old := id5ModeVal
	defer func() { id5ModeVal = old }()
	id5ModeVal = ""
	if id5Value() != 0 {
		t.Fatal("padrão deve ser 0")
	}
	id5ModeVal = "1234"
	if id5Value() != 1234 {
		t.Fatal("valor fixo")
	}
	id5ModeVal = "espelho"
	id5Mirror.Store(777)
	if id5Value() != 777 {
		t.Fatal("espelho")
	}
	ext := buildWhatsappVideoExt(videoMediaFrameDelta, nil, 1)
	if ext[2] != 0x51 || ext[3] != 0x03 || ext[4] != 0x09 {
		t.Fatalf("id5 na extensão: %x", ext)
	}
}

// parseOneByteExt lê a extensão como o parser do celular (one-byte headers:
// id = alto, tamanho = baixo+1; byte 0 é padding; elemento além do fim = erro).
func parseOneByteExt(t *testing.T, ext []byte) (ids []byte, vals [][]byte) {
	t.Helper()
	for off := 0; off < len(ext); {
		h := ext[off]
		off++
		if h == 0 {
			continue
		}
		id, l := h>>4, int(h&0x0f)+1
		if off+l > len(ext) {
			t.Fatalf("elemento id%d de %d B passa do fim (bad length): %x", id, l, ext)
		}
		ids = append(ids, id)
		vals = append(vals, ext[off:off+l])
		off += l
	}
	return
}

func TestFastRRElementLikePhone(t *testing.T) {
	// Os bytes que o celular manda: 09 0247f3 00019e10 03.
	if got := fastRRElement(149491, 106000); !bytes.Equal(got, []byte{0xD8, 0x09, 0x02, 0x47, 0xf3, 0x00, 0x01, 0x9e, 0x10, 0x03}) {
		t.Fatalf("elemento %x", got)
	}
	for _, fn := range []*uint16{nil, new(uint16)} {
		ext := buildWhatsappVideoExtFR(videoMediaFrameIDR, fn, 7, fastRRElement(1_500_000, 2_000_000))
		if len(ext)%4 != 0 {
			t.Fatalf("extensão sem alinhamento: %x", ext)
		}
		ids, vals := parseOneByteExt(t, ext)
		if ids[len(ids)-1] != 13 || len(vals[len(vals)-1]) != 9 || vals[len(vals)-1][0] != 0x09 {
			t.Fatalf("id13 errado: ids %v vals %x", ids, vals)
		}
		// o pacote inteiro: a extensão volta igual depois de codificar o cabeçalho
		pkt := media.NewH264Session(1).CreatePacketWithDuration([]byte{0x65, 1}, 0, true)
		pkt.Header.Extension, pkt.Header.ExtensionProfile, pkt.Header.ExtensionData = true, videoExtProfile, ext
		hb := pkt.Header.HeaderBytes()
		h2, err := media.DecodeRtpHeader(append(hb, pkt.Payload...))
		if err != nil || !bytes.Equal(h2.ExtensionData, ext) || h2.ExtensionProfile != videoExtProfile {
			t.Fatalf("cabeçalho não volta igual: %v", err)
		}
	}
	if bytes.Contains(buildWhatsappVideoExt(videoMediaFrameDelta, nil, 1), []byte{0xD8}) {
		t.Fatal("sem fast RR não pode ter id13")
	}
}

// O id13 sai em no máximo 1 pacote por segundo, no 1º pacote do quadro.
func TestFastRROncePerSecond(t *testing.T) {
	old := fastRRBps
	defer func() { fastRRBps = old }()
	fastRRBps = func() uint32 { return 1_500_000 }
	rel := &capRelay{connected: true}
	p := New(nil, rel)
	km := core.SrtpKeyingMaterial{MasterKey: make([]byte, 16), MasterSalt: make([]byte, 14)}
	if err := p.Setup("c", "a@s.whatsapp.net", "b@s.whatsapp.net", km, km); err != nil {
		t.Fatal(err)
	}
	au := func(typ byte, n int) []byte {
		return append([]byte{0, 0, 0, 1, 0x60 | typ}, make([]byte, n)...)
	}
	p.FeedCaptured(au(5, 2000)) // IDR em 3 pacotes
	for i := 0; i < 5; i++ {
		p.FeedCaptured(au(1, 300))
	}
	rx, _ := media.NewSrtpSession(km, km, 4, 4)
	com := 0
	for i, b := range rel.sent {
		pkt, err := rx.Unprotect(b)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(pkt.Header.ExtensionData, []byte{0xD8, 0x09}) {
			com++
			if i != 0 {
				t.Fatalf("id13 fora do 1º pacote do 1º quadro (pacote %d)", i)
			}
		}
	}
	if com != 1 {
		t.Fatalf("id13 em %d pacotes, esperado 1 (1 por segundo); pacotes %d", com, len(rel.sent))
	}
}

type capRelay struct {
	connected bool
	sent      [][]byte
}

func (r *capRelay) Broadcast(b []byte)                 { r.sent = append(r.sent, append([]byte(nil), b...)) }
func (r *capRelay) BufferedAmount() uint64             { return 0 }
func (r *capRelay) HasConnection() bool                { return r.connected }
func (r *capRelay) SetStreamSsrcs(self, peer []uint32) {}
