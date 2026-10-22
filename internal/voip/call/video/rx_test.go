package video

import (
	"bytes"
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
	h.feed(h.au(1, 800)...) // P antes de IDR: entregue (comportamento legado) mas pede PLI
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
	h.feed(h.au(5, 300)...)
	time.Sleep(15 * time.Millisecond)
	if h.nFrames() != 1 {
		t.Fatalf("1º IDR do SSRC 77: frames=%d", h.nFrames())
	}

	// Outro fluxo começa enquanto o 77 ainda fala: descartado.
	other := &rxHarness{t: t, tx: h.tx, rtp: media.NewH264Session(88)}
	h.feed(other.au(5, 300)...)
	time.Sleep(15 * time.Millisecond)
	if h.nFrames() != 1 || h.p.rx.Foreign == 0 {
		t.Fatalf("fluxo 88 com 77 ativo devia ser descartado: frames=%d foreign=%d", h.nFrames(), h.p.rx.Foreign)
	}

	// O 77 fica mudo: o 88 assume (espera IDR e então entrega).
	time.Sleep(30 * time.Millisecond)
	h.feed(other.au(1, 200)...) // delta: retrava, mas não entrega (espera IDR)
	h.feed(other.au(5, 300)...)
	time.Sleep(15 * time.Millisecond)
	if h.p.rx.ssrc != 88 || h.p.rx.Relocks != 1 {
		t.Fatalf("devia retravar no 88: ssrc=%d relocks=%d", h.p.rx.ssrc, h.p.rx.Relocks)
	}
	if h.nFrames() != 2 {
		t.Fatalf("IDR do 88 devia ser entregue (e o delta antes dele não): frames=%d", h.nFrames())
	}
}
