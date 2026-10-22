package main

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/pion/webrtc/v4"
)

func makeBrowserOffer(t *testing.T) string {
	t.Helper()
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	mic, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "browser-mic",
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pc.AddTrack(mic); err != nil {
		t.Fatal(err)
	}
	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		t.Fatal(err)
	}

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gather := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	<-gather
	return pc.LocalDescription().SDP
}

func TestNewBridgeNegotiatesBrowserOffer(t *testing.T) {
	offer := makeBrowserOffer(t)

	br, answer, err := NewBridge(offer, slog.Default())
	if err != nil {
		t.Fatalf("NewBridge failed: %v", err)
	}
	defer br.Close()

	if answer == "" || !strings.Contains(answer, "opus") {
		t.Fatalf("answer missing or has no opus codec:\n%s", answer)
	}

	if strings.Contains(answer, "m=audio 0 ") {
		t.Fatalf("answer rejected an audio m-line:\n%s", answer)
	}
	t.Logf("bridge negotiated OK; answer has %d m-lines", strings.Count(answer, "m=audio"))
}

func TestIsBrowserPliMessage(t *testing.T) {
	for in, want := range map[string]bool{
		`{"type":"pli"}`:       true,
		`{"type":"other"}`:     false,
		`not json`:             false,
		`{"type":"pli","x":1}`: true,
		``:                     false,
	} {
		if got := isBrowserPliMessage([]byte(in)); got != want {
			t.Errorf("%q: got %v want %v", in, got, want)
		}
	}
}
