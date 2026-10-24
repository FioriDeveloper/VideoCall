package signaling

import (
	"bytes"
	"context"
	"testing"

	"wacalls/internal/voip/wanode"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

func preacceptCapability(t *testing.T, video bool) []byte {
	n := BuildPreacceptStanzaFor(types.NewJID("1", types.DefaultUserServer), "C", types.NewJID("2", types.DefaultUserServer), video)
	c := findChild(findChild(&n, "preaccept"), "capability")
	b, _ := c.Content.([]byte)
	return b
}

func TestPreacceptCapabilityAB(t *testing.T) {
	if got := preacceptCapability(t, true); got[5] != 0xbb {
		t.Fatalf("desligado: capability de áudio esperada, got %x", got)
	}
	t.Setenv("WACALLS_1A1_SIGNALING_AB", "1")
	if got := preacceptCapability(t, true); got[5] != 0xfa {
		t.Fatalf("ligado + vídeo: capability de vídeo esperada, got %x", got)
	}
	if got := preacceptCapability(t, false); got[5] != 0xbb {
		t.Fatalf("ligado + áudio: capability de áudio esperada, got %x", got)
	}
}

func TestVideoDecForAcceptAB(t *testing.T) {
	if VideoDecForAccept() != "H264,AV1" {
		t.Fatal("padrão deve manter o valor legado")
	}
	t.Setenv("WACALLS_1A1_SIGNALING_AB", "true")
	if VideoDecForAccept() != "H264" {
		t.Fatal("AB deve anunciar só H264")
	}
}

// offerFakeSock é o mínimo de core.VoipSocket p/ montar o offer sem rede: um device,
// um tctoken e o <device-identity> pedido.
type offerFakeSock struct{}

func (offerFakeSock) OwnPN() types.JID  { return types.NewJID("999", types.DefaultUserServer) }
func (offerFakeSock) OwnLID() types.JID { return types.NewJID("888", types.HiddenUserServer) }
func (offerFakeSock) AccountDeviceIdentityNode() (waBinary.Node, bool) {
	return waBinary.Node{Tag: "device-identity", Content: []byte{0xde}}, true
}
func (offerFakeSock) SendNode(context.Context, waBinary.Node) error { return nil }
func (offerFakeSock) Query(context.Context, waBinary.Node) (*waBinary.Node, error) {
	return nil, nil
}
func (offerFakeSock) GetUSyncDevices(_ context.Context, jids []types.JID) ([]types.JID, error) {
	return jids, nil
}
func (offerFakeSock) AssertSessions(context.Context, []types.JID, bool) error { return nil }
func (offerFakeSock) CreateParticipantNodes(_ context.Context, devices []types.JID, _ []byte, encAttrs waBinary.Attrs) ([]waBinary.Node, bool, error) {
	var out []waBinary.Node
	for _, d := range devices {
		out = append(out, waBinary.Node{Tag: "to", Attrs: waBinary.Attrs{"jid": d},
			Content: []waBinary.Node{{Tag: "enc", Attrs: encAttrs, Content: []byte{0x01}}}})
	}
	return out, true, nil
}
func (offerFakeSock) DecryptCallKey(context.Context, types.JID, *waBinary.Node) ([]byte, error) {
	return nil, nil
}
func (offerFakeSock) GetTCToken(context.Context, types.JID) ([]byte, error) {
	return []byte{0x7c}, nil
}
func (offerFakeSock) ResolveLIDForPN(_ context.Context, pn types.JID) types.JID { return pn }

func offerChildren(t *testing.T, isVideo bool) []waBinary.Node {
	t.Helper()
	n, _, err := BuildOfferStanza(context.Background(), offerFakeSock{}, "CID", make([]byte, 32),
		types.NewJID("111", types.DefaultUserServer), isVideo)
	if err != nil {
		t.Fatalf("BuildOfferStanza: %v", err)
	}
	offer := findChild(&n, "offer")
	if offer == nil {
		t.Fatal("nó offer ausente")
	}
	return wanode.NodeChildren(offer)
}

func childTags(ch []waBinary.Node) []string {
	tags := make([]string, len(ch))
	for i, c := range ch {
		tags[i] = c.Tag
	}
	return tags
}

func assertTags(t *testing.T, ch []waBinary.Node, want ...string) {
	t.Helper()
	got := childTags(ch)
	if len(got) != len(want) {
		t.Fatalf("filhos do offer = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("filhos do offer = %v, want %v", got, want)
		}
	}
}

func assertAttrs(t *testing.T, n waBinary.Node, want map[string]string) {
	t.Helper()
	if len(n.Attrs) != len(want) {
		t.Fatalf("<%s> attrs = %v, want %v", n.Tag, n.Attrs, want)
	}
	for k, v := range want {
		if got := wanode.AttrString(n.Attrs, k); got != v {
			t.Fatalf("<%s> %s = %q, want %q", n.Tag, k, got, v)
		}
	}
}

func capabilityBytes(t *testing.T, ch []waBinary.Node) []byte {
	t.Helper()
	for _, c := range ch {
		if c.Tag == "capability" {
			if wanode.AttrString(c.Attrs, "ver") != "1" {
				t.Fatalf("capability ver = %v", c.Attrs)
			}
			b, _ := c.Content.([]byte)
			return b
		}
	}
	t.Fatal("capability ausente")
	return nil
}

// Ordem e atributos do offer de vídeo oficial (gravação do WhatsApp Desktop oficial,
// 24/10/2026). O <privacy> da gravação corresponde ao nosso <tctoken> (mantido).
var officialVideoOfferTags = []string{"tctoken", "audio", "audio", "video", "net", "capability", "destination", "encopt", "device-identity"}

func assertOfficialShape(t *testing.T, ch []waBinary.Node) {
	t.Helper()
	assertTags(t, ch, officialVideoOfferTags...)
	assertAttrs(t, ch[1], map[string]string{"enc": "opus", "rate": "8000"})
	assertAttrs(t, ch[2], map[string]string{"enc": "opus", "rate": "16000"})
	assertAttrs(t, ch[3], map[string]string{
		"dec": "H264", "enc": "h.264", "device_orientation": "0",
		"screen_width": "1920", "screen_height": "1080",
	})
	assertAttrs(t, ch[4], map[string]string{"medium": "3"})
	assertAttrs(t, ch[7], map[string]string{"keygen": "2"})
}

func TestOfferVideoOficialLigada(t *testing.T) {
	t.Setenv("WACALLS_1A1_OFERTA_OFICIAL", "1")
	ch := offerChildren(t, true)
	assertOfficialShape(t, ch)
	want := []byte{0x01, 0x05, 0xf5, 0x09, 0xe0, 0xbb, 0x53}
	if got := capabilityBytes(t, ch); !bytes.Equal(got, want) {
		t.Fatalf("capability = %x, want %x", got, want)
	}
	// Offer de áudio não muda com a flag.
	if got := capabilityBytes(t, offerChildren(t, false)); !bytes.Equal(got, capabilityOffer) {
		t.Fatalf("áudio: capability = %x, want %x", got, capabilityOffer)
	}
}

func TestOfferVideoOficialDesligada(t *testing.T) {
	t.Setenv("WACALLS_1A1_OFERTA_OFICIAL", "")
	ch := offerChildren(t, true)
	// Formato atual: mesma ordem/atributos, só a capability antiga de vídeo.
	assertOfficialShape(t, ch)
	want := []byte{0x01, 0x05, 0xf7, 0x09, 0xe0, 0xfa, 0x13}
	if got := capabilityBytes(t, ch); !bytes.Equal(got, want) {
		t.Fatalf("capability = %x, want %x", got, want)
	}
	audio := offerChildren(t, false)
	assertTags(t, audio, "tctoken", "audio", "audio", "net", "capability", "destination", "encopt", "device-identity")
	if got := capabilityBytes(t, audio); !bytes.Equal(got, capabilityOffer) {
		t.Fatalf("áudio: capability = %x, want %x", got, capabilityOffer)
	}
}
