package call

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	"wacalls/internal/voip/media"
)

// fakeCodec "decodifica" cada pacote em 960 amostras com o 1º byte do payload,
// para o teste saber a ordem em que os pacotes viraram áudio.
type fakeCodec struct{}

func (fakeCodec) Encode(pcm []float32) ([]byte, error) { return nil, nil }
func (fakeCodec) Decode(frame []byte) ([]float32, error) {
	pcm := make([]float32, 960)
	for i := range pcm {
		pcm[i] = float32(frame[0])
	}
	return pcm, nil
}
func (fakeCodec) FrameSize() int  { return 960 }
func (fakeCodec) SampleRate() int { return 16000 }
func (fakeCodec) Close()          {}

// Pacotes do contato pelos 2 relays (duplicados) e fora de ordem: cada um vira
// áudio uma vez, em ordem, sem silêncio no lugar do que só estava atrasado.
func TestPeerAudioDedupAndReorder(t *testing.T) {
	m := &CallManager{log: slog.Default(), audioRx: media.NewRtpRxStats(16000)}
	var mu sync.Mutex
	var got []float32
	var silence int
	m.OnPeerAudio = func(pcm []float32) {
		mu.Lock()
		defer mu.Unlock()
		for i := 0; i < len(pcm); i += 960 {
			if pcm[i] == 0 {
				silence++
			} else {
				got = append(got, pcm[i])
			}
		}
	}
	pkt := func(n uint16) *media.RtpPacket {
		return &media.RtpPacket{Header: &media.RtpHeader{SequenceNumber: 100 + n, Timestamp: 5000 + uint32(n)*960}, Payload: []byte{byte(n + 1)}}
	}
	feed := func(n uint16) {
		p := pkt(n)
		if !m.audioRx.UpdateWithPayload(p.Header.SequenceNumber, p.Header.Timestamp, time.Now(), 1) {
			return // duplicado: handleAudioRelayData descarta antes daqui
		}
		m.queuePeerAudio(p, fakeCodec{}, false)
	}
	for _, n := range []uint16{0, 2, 1, 1, 3, 2, 5, 4, 4, 5} {
		feed(n)
	}
	time.Sleep(audioReorderWindow + 30*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 6 || silence != 0 {
		t.Fatalf("got %v, silêncio %d quadros", got, silence)
	}
	for i, v := range got {
		if v != float32(i+1) {
			t.Fatalf("fora de ordem: %v", got)
		}
	}
}
