package video

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
)

type rxRelay struct {
	mu   sync.Mutex
	sent [][]byte
}

func (f *rxRelay) Broadcast(d []byte) {
	f.mu.Lock()
	f.sent = append(f.sent, append([]byte(nil), d...))
	f.mu.Unlock()
}
func (f *rxRelay) BufferedAmount() uint64             { return 0 }
func (f *rxRelay) HasConnection() bool                { return true }
func (f *rxRelay) SetStreamSsrcs(self, peer []uint32) {}
func (f *rxRelay) count() int                         { f.mu.Lock(); defer f.mu.Unlock(); return len(f.sent) }
func (f *rxRelay) last() []byte                       { f.mu.Lock(); defer f.mu.Unlock(); return f.sent[len(f.sent)-1] }

const (
	tCall = "CALL1"
	tOur  = "111:1@s.whatsapp.net"
	tPeer = "222:2@s.whatsapp.net"
)

type rxHarness struct {
	t      *testing.T
	p      *Pipeline
	relay  *rxRelay
	peer   *media.RtpSession
	peerSP *media.SrtpSession
	mu     sync.Mutex
	frames [][]byte
	key    []byte
}

func newRxHarness(t *testing.T) *rxHarness {
	t.Setenv(envReorder, "5")
	h := &rxHarness{t: t, relay: &rxRelay{}, key: bytes.Repeat([]byte{9}, 32)}
	sendKM, _ := media.DerivePerJidSrtpKey(h.key, tOur)
	recvKM, _ := media.DerivePerJidSrtpKey(h.key, tPeer)
	h.p = New(nil, h.relay)
	if err := h.p.Setup(tCall, tOur, tPeer, sendKM, recvKM); err != nil {
		t.Fatal(err)
	}
	if err := h.p.SetupRtcp(h.key, tOur, tPeer); err != nil {
		t.Fatal(err)
	}
	h.p.OnFrame = func(au []byte) { h.mu.Lock(); h.frames = append(h.frames, au); h.mu.Unlock() }
	peerSSRC := media.GenerateSecureSsrc(tCall, tPeer, slotWord)
	h.peer = media.NewH264Session(peerSSRC)
	h.peerSP, _ = media.NewSrtpSession(recvKM, sendKM, core.SRTPSendAuthTagLen, core.SRTPRecvAuthTagLen)
	return h
}

// pkt gera o próximo pacote RTP protegido do peer.
func (h *rxHarness) pkt(payload []byte, marker bool) []byte {
	p := h.peer.CreatePacketWithDuration(payload, 0, marker)
	out, err := h.peerSP.Protect(p)
	if err != nil {
		h.t.Fatal(err)
	}
	return out
}

func (h *rxHarness) nFrames() int { h.mu.Lock(); defer h.mu.Unlock(); return len(h.frames) }

func (h *rxHarness) waitFrames(n int) {
	h.t.Helper()
	for i := 0; i < 200; i++ {
		if h.nFrames() >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.t.Fatalf("frames=%d, esperado %d", h.nFrames(), n)
}

var (
	idrNal = []byte{0x65, 1, 2, 3}
	pNal   = []byte{0x41, 4, 5, 6}
)

func fua(start, end bool, body ...byte) []byte {
	hdr := byte(5)
	if start {
		hdr |= 0x80
	}
	if end {
		hdr |= 0x40
	}
	return append([]byte{0x7c, hdr}, body...)
}

func TestRxInOrderDedupAndReorder(t *testing.T) {
	h := newRxHarness(t)
	a := h.pkt(idrNal, true)
	b := h.pkt(pNal, true)
	c := h.pkt(pNal, true)
	for _, d := range [][]byte{a, a, c, b, b} { // a duplicado, c antes de b, b duplicado
		h.p.HandleRelayData(d)
	}
	h.waitFrames(3)
	time.Sleep(30 * time.Millisecond)
	if h.nFrames() != 3 {
		t.Fatalf("frames=%d, esperado 3 (sem duplicados)", h.nFrames())
	}
	want := append([]byte{0, 0, 0, 1}, idrNal...)
	if !bytes.Equal(h.frames[0], want) {
		t.Fatalf("1º quadro = %x", h.frames[0])
	}
	if !bytes.Equal(h.frames[1][4:], pNal) || !bytes.Equal(h.frames[2][4:], pNal) {
		t.Fatal("ordem dos quadros errada")
	}
	if st := h.p.RxStats(); st.Dups != 2 || st.LostPackets != 0 {
		t.Fatalf("stats=%+v", st)
	}
}

func TestRxDropsForeignSsrc(t *testing.T) {
	h := newRxHarness(t)
	h.p.HandleRelayData(h.pkt(idrNal, true)) // trava o SSRC do peer
	other := media.NewH264Session(media.GenerateSecureSsrc(tCall, tPeer, 5))
	sendKM, _ := media.DerivePerJidSrtpKey(h.key, tOur)
	recvKM, _ := media.DerivePerJidSrtpKey(h.key, tPeer)
	sp, _ := media.NewSrtpSession(recvKM, sendKM, 4, 4)
	op, _ := sp.Protect(other.CreatePacketWithDuration(pNal, 0, true))
	h.p.HandleRelayData(op)
	time.Sleep(30 * time.Millisecond)
	if h.nFrames() != 1 || h.p.RxStats().Foreign != 1 {
		t.Fatalf("frames=%d stats=%+v", h.nFrames(), h.p.RxStats())
	}
}

func TestRxGapDropsFramesUntilIDRAndSendsPLI(t *testing.T) {
	h := newRxHarness(t)
	h.p.HandleRelayData(h.pkt(idrNal, true)) // quadro 1 (IDR) entregue
	h.waitFrames(1)
	basePLI := h.relay.count() // PLI inicial (esperando 1º IDR) pode ter saído
	// quadro 2 em 3 fragmentos FU-A; o do meio se perde
	f1 := h.pkt(fua(true, false, 1, 2), false)
	_ = h.pkt(fua(false, false, 3, 4), false) // perdido
	f3 := h.pkt(fua(false, true, 5, 6), true)
	h.p.HandleRelayData(f1)
	h.p.HandleRelayData(f3)
	// quadro 3: P inteiro, mas ainda sem IDR => descartado
	h.p.HandleRelayData(h.pkt(pNal, true))
	time.Sleep(60 * time.Millisecond) // a janela expira e a lacuna vira perda
	if h.nFrames() != 1 {
		t.Fatalf("quadro corrompido/P sem IDR foi entregue: %d", h.nFrames())
	}
	if h.relay.count() <= basePLI {
		t.Fatal("nenhum PLI enviado após a lacuna")
	}
	// o PLI sai protegido: decifra com as chaves SRTCP do nosso JID (tag 4 ou 10)
	keys, _ := media.DeriveGroupSrtcpKeys(h.key, tOur)
	pkt := h.relay.last()
	var plain []byte
	for _, tl := range []int{10, 4} {
		if pl, _, ok := media.UnprotectSrtcpTag(&keys, h.p.selfSsrc, pkt, tl); ok {
			plain = pl
		}
	}
	peerVideo := media.GenerateSecureSsrc(tCall, tPeer, slotWord)
	wantPLI := media.BuildPictureLossIndication(h.p.selfSsrc, peerVideo, true)
	if !bytes.Equal(plain, wantPLI[:]) {
		t.Fatalf("PLI = %x, esperado %x", plain, wantPLI[:])
	}
	// novo IDR volta a entregar
	h.p.HandleRelayData(h.pkt(idrNal, true))
	h.p.HandleRelayData(h.pkt(pNal, true))
	h.waitFrames(3)
	if st := h.p.RxStats(); st.LostPackets != 1 || st.FramesDropped < 2 {
		t.Fatalf("stats=%+v", st)
	}
}

func TestRxDisabledUsesLegacyPath(t *testing.T) {
	t.Setenv(envRx, "0")
	h := newRxHarness(t)
	if h.p.rx.Load() != nil {
		t.Fatal("rx deveria estar nil com WACALLS_1A1_RX=0")
	}
	h.p.HandleRelayData(h.pkt(idrNal, true))
	if h.nFrames() != 1 {
		t.Fatalf("legado não entregou: %d", h.nFrames())
	}
}

func TestRtcpIncomingPLIRequestsKeyframe(t *testing.T) {
	t.Setenv(envSRTCP, "1")
	t.Setenv(envTag, "4")
	h := newRxHarness(t)
	got := make(chan struct{}, 1)
	h.p.OnKeyframeRequest = func() { got <- struct{}{} }
	// o peer assina com as chaves SRTCP dele (JID do peer) e tag de 4 bytes
	keys, _ := media.DeriveGroupSrtcpKeys(h.key, tPeer)
	pli := media.BuildPictureLossIndication(777, h.p.selfSsrc, true)
	pkt, _ := media.ProtectSrtcpTag(&keys, 777, 1, pli[:], 4)
	h.p.HandleRelayRtcp(pkt)
	select {
	case <-got:
	case <-time.After(time.Second):
		t.Fatal("PLI do peer não virou pedido de keyframe")
	}
	// chave errada: não autentica, não dispara
	bad, _ := media.DeriveGroupSrtcpKeys(h.key, "x")
	pkt2, _ := media.ProtectSrtcpTag(&bad, 777, 2, pli[:], 4)
	h.p.HandleRelayRtcp(pkt2)
	select {
	case <-got:
		t.Fatal("RTCP não autenticado disparou keyframe")
	case <-time.After(50 * time.Millisecond):
	}
}
