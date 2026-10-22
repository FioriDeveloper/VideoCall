package call

import (
	"bytes"
	"encoding/binary"
	"log/slog"
	"sync"
	"testing"
	"time"

	callvideo "wacalls/internal/voip/call/video"
	"wacalls/internal/voip/core"
	"wacalls/internal/voip/media"
)

type recRelay struct {
	fakeRelay
	mu   sync.Mutex
	sent [][]byte
}

func (r *recRelay) Broadcast(d []byte) {
	r.mu.Lock()
	r.sent = append(r.sent, append([]byte(nil), d...))
	r.mu.Unlock()
}
func (r *recRelay) BufferedAmount() uint64 { return 0 }
func (r *recRelay) HasConnection() bool    { return true }

func newRtcpTestManager(t *testing.T) (*CallManager, *recRelay, []byte) {
	t.Setenv("WACALLS_1A1_SRTCP", "1")
	rr := &recRelay{}
	m := &CallManager{log: slog.Default(), relay: rr, selfSsrc: 0x1111}
	m.video = callvideo.New(nil, rr)
	m.video.OnKeyframeRequest = m.sendPeerPLI
	callKey := make([]byte, 32)
	for i := range callKey {
		callKey[i] = byte(i)
	}
	km, _ := media.DerivePerJidSrtpKey(callKey, "a:1@s.whatsapp.net")
	if err := m.video.Setup("CALL", "a:1@s.whatsapp.net", "b:1@s.whatsapp.net", km, km); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.setupRtcpLocked(callKey, "a:1@s.whatsapp.net", "b:1@s.whatsapp.net")
	m.mu.Unlock()
	if m.rtcp == nil {
		t.Fatal("rtcp não inicializado")
	}
	return m, rr, callKey
}

func TestSendPeerPLIIsAuthenticSrtcp(t *testing.T) {
	m, rr, callKey := newRtcpTestManager(t)
	m.sendPeerPLI(0xabcdef01)
	if len(rr.sent) != 1 {
		t.Fatalf("sent=%d", len(rr.sent))
	}
	ourKeys, _ := media.DeriveGroupSrtcpKeys(callKey, "a:1@s.whatsapp.net")
	vs := m.video.SelfSsrc()
	plain, _, ok := media.UnprotectSrtcpTag(&ourKeys, vs, rr.sent[0], 10)
	want := media.BuildPictureLossIndication(vs, 0xabcdef01, true)
	if !ok || !bytes.Equal(plain, want[:]) {
		t.Fatalf("PLI inválido: ok=%v %x", ok, plain)
	}
}

func TestOnRelayRtcpHandlesPeerPLIAndAdaptsTag(t *testing.T) {
	m, rr, callKey := newRtcpTestManager(t)
	got := 0
	m.OnPeerKeyframeRequest = func() { got++ }
	peerKeys, _ := media.DeriveGroupSrtcpKeys(callKey, "b:1@s.whatsapp.net")
	pli := media.BuildPictureLossIndication(0x9999, m.video.SelfSsrc(), false)
	pkt, _ := media.ProtectSrtcpTag(&peerKeys, 0x9999, 1, pli[:], 4)
	m.onRelayData(pkt)
	if got != 1 {
		t.Fatalf("OnPeerKeyframeRequest chamado %d vezes", got)
	}
	if m.rtcp.tagLen != 4 {
		t.Fatalf("tag deveria adaptar para 4, é %d", m.rtcp.tagLen)
	}
	// forjado (chave errada) é ignorado
	bad := append([]byte(nil), pkt...)
	bad[len(bad)-1] ^= 1
	m.onRelayData(bad)
	if got != 1 {
		t.Fatal("RTCP forjado não pode disparar keyframe")
	}
	// depois de adaptar, o PLI que mandamos usa tag 4
	m.sendPeerPLI(1)
	ourKeys, _ := media.DeriveGroupSrtcpKeys(callKey, "a:1@s.whatsapp.net")
	if _, _, ok := media.UnprotectSrtcpTag(&ourKeys, m.video.SelfSsrc(), rr.sent[len(rr.sent)-1], 4); !ok {
		t.Fatal("PLI deveria sair com tag de 4 bytes")
	}
	_ = core.PayloadTypeWhatsAppH264
}

func TestSendReceiverReportsAudio(t *testing.T) {
	t.Setenv("WACALLS_1A1_RR", "1")
	m, rr, callKey := newRtcpTestManager(t)
	for i := 0; i < 5; i++ {
		m.audioRx.Update(uint16(10+i), uint32(i)*960, time.Unix(0, 0).Add(time.Duration(i)*60*time.Millisecond))
	}
	m.sendReceiverReports(m.rtcp, m.audioRx, 0xfeed, time.Unix(1, 0))
	if len(rr.sent) != 1 {
		t.Fatalf("sent=%d", len(rr.sent))
	}
	ourKeys, _ := media.DeriveGroupSrtcpKeys(callKey, "a:1@s.whatsapp.net")
	plain, _, ok := media.UnprotectSrtcpTag(&ourKeys, m.selfSsrc, rr.sent[0], 10)
	if !ok || plain[1] != media.RtcpPtRr || len(plain) != 32 {
		t.Fatalf("RR inválido: ok=%v %x", ok, plain)
	}
	if binary.BigEndian.Uint32(plain[4:8]) != m.selfSsrc || binary.BigEndian.Uint32(plain[8:12]) != 0xfeed {
		t.Fatalf("SSRCs errados: %x", plain[:12])
	}
}
