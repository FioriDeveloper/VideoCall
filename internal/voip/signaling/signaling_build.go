package signaling

import (
	"context"
	"fmt"
	"wacalls/internal/voip/core"
	"wacalls/internal/voip/wanode"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

// Blobs de capability (ver=1) que o aparelho do peer lê para decidir tocar. Valores
// alinhados ao WhatsApp Web atual (referência: meowcaller/whatsapp-rust). O byte 5
// era 0xe4 (stale) — clientes atuais ignoravam a chamada em silêncio; passou a 0xe0.
var (
	capabilityOffer      = []byte{0x01, 0x05, 0xf7, 0x09, 0xe0, 0xbb, 0x13}
	capabilityVideoOffer = []byte{0x01, 0x05, 0xf7, 0x09, 0xe0, 0xfa, 0x13}
	capabilityPreaccept  = []byte{0x01, 0x05, 0xf7, 0x09, 0xe0, 0xbb, 0x07}
	// INFERIDO (não observado): o preaccept de vídeo troca o byte de mídia 0xbb->0xfa
	// como o offer de vídeo faz em relação ao de áudio.
	capabilityVideoPreaccept = []byte{0x01, 0x05, 0xf7, 0x09, 0xe0, 0xfa, 0x07}
)

// CapabilityOffer expõe a capability de offer (bytes) p/ o pacote call.
var CapabilityOffer = capabilityOffer

// CapabilityGroupOffer é a capability que o WhatsApp Web envia no offer de GRUPO
// (capturada de uma chamada de grupo REAL do WhatsApp Web, 2026-10-03). Difere da de
// 1:1 (f7...13) em 2 bytes (f5...53) — é o bit que diz ao servidor "este cliente faz
// chamada de grupo". Usar a de 1:1 fazia o servidor recusar o offer de grupo com 427.
var capabilityGroupOffer = []byte{0x01, 0x05, 0xf5, 0x09, 0xe0, 0xbb, 0x53}
var CapabilityGroupOffer = capabilityGroupOffer

// CapabilityGroupVideoOffer é a capability de chamada de grupo com VÍDEO (visto nos
// group_updates reais: 0105f509e0fa53, difere da de áudio só no byte 5: fa vs bb).
var capabilityGroupVideoOffer = []byte{0x01, 0x05, 0xf5, 0x09, 0xe0, 0xfa, 0x53}
var CapabilityGroupVideoOffer = capabilityGroupVideoOffer

// SignalingAB liga as variações de sinalização do teste A/B de qualidade de vídeo 1:1
// (WACALLS_1A1_SIGNALING_AB, padrão desligado): accept de vídeo no formato do offer,
// preaccept com capability de vídeo e aceite de upgrade sem AV1. Lido a cada stanza.
func SignalingAB() bool { return core.EnvFlag("WACALLS_1A1_SIGNALING_AB", false) }

// capabilityVideoOfferOficial é a capability que o WhatsApp Desktop oficial manda no
// offer de VÍDEO 1:1 (fonte: gravação do WhatsApp Desktop oficial, 24/10/2026, mesmo
// celular de teste). Repare que NÃO usa o byte de mídia 0xfa nem o par f7/13 que
// mandamos hoje: são os mesmos bytes da capability de grupo (f5...bb...53), e o
// celular devolveu exatamente esses bytes no preaccept. Hipótese em teste: o servidor
// escolhe o voip_settings (bwe_impl=5 no oficial x bwe_impl=8 + condicionais de baixa
// qualidade no nosso) a partir do que o offer anuncia.
var capabilityVideoOfferOficial = []byte{0x01, 0x05, 0xf5, 0x09, 0xe0, 0xbb, 0x53}

// OfertaOficial liga o offer de vídeo 1:1 no formato exato do WhatsApp Desktop
// oficial (WACALLS_1A1_OFERTA_OFICIAL, padrão desligado). Lido a cada offer.
// Comparado item a item com a gravação de 24/10/2026, a ÚNICA diferença do nosso
// offer de vídeo era a capability: os dois <audio> (8000, 16000), o <video>, o
// <net medium=3>, o <encopt keygen=2> e a ordem dos filhos já eram iguais.
func OfertaOficial() bool { return core.EnvFlag("WACALLS_1A1_OFERTA_OFICIAL", false) }

func BuildOfferStanza(ctx context.Context, sock core.VoipSocket, callID string, callKey []byte, peerJid types.JID, isVideo bool) (waBinary.Node, []types.JID, error) {
	creator := sock.OwnLID()
	if creator.IsEmpty() {
		creator = sock.OwnPN()
	}

	rawDevices, err := sock.GetUSyncDevices(ctx, []types.JID{peerJid})
	if err != nil {
		return waBinary.Node{}, nil, fmt.Errorf("usync devices: %w", err)
	}
	if err := sock.AssertSessions(ctx, rawDevices, false); err != nil {
		return waBinary.Node{}, nil, fmt.Errorf("assert sessions: %w", err)
	}

	destinations, includeDeviceIdentity, err := sock.CreateParticipantNodes(ctx, rawDevices, callKey, waBinary.Attrs{"count": "0"})
	if err != nil {
		return waBinary.Node{}, nil, fmt.Errorf("participant nodes: %w", err)
	}

	var offerContent []waBinary.Node

	if token, err := sock.GetTCToken(ctx, wanode.MustJID(wanode.CleanJID(peerJid.String()))); err == nil && len(token) > 0 {
		// tag CORRETA é "tctoken" (o whatsmeow usa Tag:"tctoken"); com "privacy"
		// o servidor ignora o token e o offer some sem ack.
		offerContent = append(offerContent, waBinary.Node{Tag: "tctoken", Content: token})
	}

	offerContent = append(offerContent,
		waBinary.Node{Tag: "audio", Attrs: waBinary.Attrs{"enc": "opus", "rate": "8000"}},
		waBinary.Node{Tag: "audio", Attrs: waBinary.Attrs{"enc": "opus", "rate": "16000"}},
	)
	capability := capabilityOffer
	if isVideo {
		// Formato atual do WhatsApp Web: enc "h.264" (com ponto), dec "H264" (maiúsc.),
		// SEM o atributo legado "orientation". E capability de vídeo próprio.
		capability = capabilityVideoOffer
		if OfertaOficial() {
			// Mesma capability do WhatsApp Desktop oficial (ver capabilityVideoOfferOficial).
			capability = capabilityVideoOfferOficial
		}
		offerContent = append(offerContent, waBinary.Node{Tag: "video", Attrs: waBinary.Attrs{
			"enc": "h.264", "dec": "H264",
			"screen_width": "1920", "screen_height": "1080", "device_orientation": "0",
		}})
	}
	// <destination> é o formato CORRETO do offer (o WaCalls original toca assim —
	// confirmado: preaccept recebido). Trocar por <enc> direto foi regressão.
	offerContent = append(offerContent,
		waBinary.Node{Tag: "net", Attrs: waBinary.Attrs{"medium": "3"}},
		waBinary.Node{Tag: "capability", Attrs: waBinary.Attrs{"ver": "1"}, Content: capability},
		waBinary.Node{Tag: "destination", Content: destinations},
		waBinary.Node{Tag: "encopt", Attrs: waBinary.Attrs{"keygen": "2"}},
	)
	if includeDeviceIdentity {
		if di, ok := sock.AccountDeviceIdentityNode(); ok {
			offerContent = append(offerContent, di)
		}
	}

	return waBinary.Node{
		Tag:   "call",
		Attrs: waBinary.Attrs{"to": peerJid, "id": GenerateCallStanzaID()},
		Content: []waBinary.Node{{
			Tag:     "offer",
			Attrs:   waBinary.Attrs{"call-id": callID, "call-creator": creator},
			Content: offerContent,
		}},
	}, rawDevices, nil
}

func BuildAcceptStanza(ctx context.Context, sock core.VoipSocket, callID string, callKey []byte, peerJid, callCreator types.JID, isVideo bool) (waBinary.Node, error) {
	if err := sock.AssertSessions(ctx, []types.JID{callCreator}, true); err != nil {
		return waBinary.Node{}, fmt.Errorf("assert creator session: %w", err)
	}

	nodes, includeDeviceIdentity, err := sock.CreateParticipantNodes(ctx, []types.JID{callCreator}, callKey, waBinary.Attrs{"count": "0"})
	if err != nil {
		return waBinary.Node{}, fmt.Errorf("encrypt accept: %w", err)
	}

	encNode := extractEncFromParticipant(nodes)
	if encNode == nil {
		return waBinary.Node{}, fmt.Errorf("no enc node produced for accept")
	}

	acceptContent := []waBinary.Node{
		{Tag: "audio", Attrs: waBinary.Attrs{"enc": "opus", "rate": "16000"}},
		{Tag: "net", Attrs: waBinary.Attrs{"medium": "3"}},
		*encNode,
		{Tag: "encopt", Attrs: waBinary.Attrs{"keygen": "2"}},
	}
	if includeDeviceIdentity {
		if di, ok := sock.AccountDeviceIdentityNode(); ok {
			acceptContent = append(acceptContent, di)
		}
	}
	if isVideo {
		if SignalingAB() {
			// Mesmo formato do offer de vídeo (enc "h.264", dec "H264", screen_*, orientação)
			// e capability de vídeo, em vez do formato antigo só com enc="h264".
			acceptContent = append(acceptContent,
				waBinary.Node{Tag: "video", Attrs: waBinary.Attrs{
					"enc": "h.264", "dec": "H264",
					"screen_width": "1920", "screen_height": "1080", "device_orientation": "0",
				}},
				waBinary.Node{Tag: "capability", Attrs: waBinary.Attrs{"ver": "1"}, Content: capabilityVideoOffer},
			)
		} else {
			acceptContent = append(acceptContent, waBinary.Node{Tag: "video", Attrs: waBinary.Attrs{"enc": "h264"}})
		}
	}

	return waBinary.Node{
		Tag:   "call",
		Attrs: waBinary.Attrs{"to": wanode.MustJID(wanode.CleanJID(peerJid.String())), "id": GenerateCallStanzaID()},
		Content: []waBinary.Node{{
			Tag:     "accept",
			Attrs:   waBinary.Attrs{"call-id": callID, "call-creator": callCreator},
			Content: acceptContent,
		}},
	}, nil
}

func extractEncFromParticipant(nodes []waBinary.Node) *waBinary.Node {
	for _, n := range nodes {
		n := n
		if n.Tag == "enc" {
			return &n
		}
		for _, c := range wanode.NodeChildren(&n) {
			c := c
			if c.Tag == "enc" {
				return &c
			}
		}
	}
	return nil
}

func BuildTerminateStanza(peerJid types.JID, callID string, callCreator types.JID) waBinary.Node {
	return callWrap(peerJid, waBinary.Node{
		Tag:   "terminate",
		Attrs: waBinary.Attrs{"call-id": callID, "call-creator": callCreator},
	})
}

// BuildTerminateElsewhereStanza avisa os dispositivos do destino que NÃO
// atenderam para pararem de tocar. Quando um device companheiro do destino
// atende, quem originou a chamada precisa fazer esse fan-out — senão os demais
// devices do destino ficam tocando até o timeout (e o reject tardio deles
// chegaria no meio da chamada já ativa).
func BuildTerminateElsewhereStanza(peerJid types.JID, callID string, callCreator types.JID, devices []types.JID) waBinary.Node {
	tos := make([]waBinary.Node, len(devices))
	for i, jid := range devices {
		tos[i] = waBinary.Node{Tag: "to", Attrs: waBinary.Attrs{"jid": jid}}
	}
	return callWrap(peerJid, waBinary.Node{
		Tag:     "terminate",
		Attrs:   waBinary.Attrs{"call-id": callID, "call-creator": callCreator, "reason": "accepted_elsewhere"},
		Content: []waBinary.Node{{Tag: "destination", Content: tos}},
	})
}

// BuildRejectStanza monta o <reject> enviado ao chamador quando recusamos uma
// chamada recebida. Replica EXATAMENTE o RejectCall de referência do whatsmeow:
// o <call> leva from=ownID e to=callFrom (ambos ToNonAD), e o <reject> leva
// call-creator=callFrom e count="0". Sem o from e o count o aparelho do chamador
// não registra a recusa e continua tocando até o timeout.
func BuildRejectStanza(peerJid types.JID, callID string, callCreator, ownJid types.JID) waBinary.Node {
	return waBinary.Node{
		Tag: "call",
		Attrs: waBinary.Attrs{
			"id":   GenerateCallStanzaID(),
			"from": ownJid.ToNonAD(),
			"to":   peerJid.ToNonAD(),
		},
		Content: []waBinary.Node{{
			Tag:   "reject",
			Attrs: waBinary.Attrs{"call-id": callID, "call-creator": callCreator.ToNonAD(), "count": "0"},
		}},
	}
}

func BuildPreacceptStanza(peerJid types.JID, callID string, callCreator types.JID) waBinary.Node {
	return BuildPreacceptStanzaFor(peerJid, callID, callCreator, false)
}

// BuildPreacceptStanzaFor é o preaccept; com isVideo e SignalingAB() anuncia a
// capability de vídeo (senão sempre a de áudio, como antes).
func BuildPreacceptStanzaFor(peerJid types.JID, callID string, callCreator types.JID, isVideo bool) waBinary.Node {
	capability := capabilityPreaccept
	if isVideo && SignalingAB() {
		capability = capabilityVideoPreaccept
	}
	return waBinary.Node{
		Tag:   "call",
		Attrs: waBinary.Attrs{"to": peerJid, "id": GenerateCallStanzaID()},
		Content: []waBinary.Node{{
			Tag:   "preaccept",
			Attrs: waBinary.Attrs{"call-id": callID, "call-creator": callCreator},
			Content: []waBinary.Node{
				{Tag: "audio", Attrs: waBinary.Attrs{"enc": "opus", "rate": "16000"}},
				{Tag: "encopt", Attrs: waBinary.Attrs{"keygen": "2"}},
				{Tag: "capability", Attrs: waBinary.Attrs{"ver": "1"}, Content: capability},
			},
		}},
	}
}

func CreateCallAck(nodeID string, peerJid types.JID, typ string) waBinary.Node {
	return waBinary.Node{
		Tag:   "ack",
		Attrs: waBinary.Attrs{"id": nodeID, "to": peerJid, "class": "call", "type": typ},
	}
}

type RelayLatencyEntry struct {
	RelayName    string
	Latency      int
	AddressBytes []byte
}

func BuildRelayLatencyStanza(peerJid types.JID, callID string, callCreator types.JID, relays []RelayLatencyEntry, destinationJids []types.JID) waBinary.Node {
	seen := map[string]bool{}
	var teNodes []waBinary.Node
	for _, r := range relays {
		if r.RelayName == "" || seen[r.RelayName] {
			continue
		}
		seen[r.RelayName] = true
		encodedLatency := 0x2000000 + r.Latency
		te := waBinary.Node{
			Tag:   "te",
			Attrs: waBinary.Attrs{"latency": fmt.Sprintf("%d", encodedLatency), "relay_name": r.RelayName},
		}
		if len(r.AddressBytes) > 0 {
			te.Content = r.AddressBytes
		}
		teNodes = append(teNodes, te)
	}

	content := append([]waBinary.Node(nil), teNodes...)
	if len(destinationJids) > 0 {
		var dst []waBinary.Node
		for _, jid := range destinationJids {
			dst = append(dst, waBinary.Node{Tag: "to", Attrs: waBinary.Attrs{"jid": jid}})
		}
		content = append(content, waBinary.Node{Tag: "destination", Content: dst})
	}

	return callWrap(wanode.MustJID(wanode.CleanJID(peerJid.String())), waBinary.Node{
		Tag:     "relaylatency",
		Attrs:   waBinary.Attrs{"call-id": callID, "call-creator": callCreator},
		Content: content,
	})
}

func BuildTransportStanza(peerJid types.JID, callID string, callCreator types.JID) waBinary.Node {
	return callWrap(wanode.MustJID(wanode.CleanJID(peerJid.String())), waBinary.Node{
		Tag: "transport",
		Attrs: waBinary.Attrs{
			"call-id": callID, "call-creator": callCreator,
			"transport-message-type": "0", "p2p-cand-round": "0",
		},
		Content: []waBinary.Node{{Tag: "net", Attrs: waBinary.Attrs{"medium": "2", "protocol": "0"}}},
	})
}

func BuildMuteV2Stanza(peerDeviceJid types.JID, callID string, callCreator types.JID, muteState int) waBinary.Node {
	return waBinary.Node{
		Tag:   "call",
		Attrs: waBinary.Attrs{"to": peerDeviceJid, "id": GenerateCallStanzaID()},
		Content: []waBinary.Node{{
			Tag: "mute_v2",
			Attrs: waBinary.Attrs{
				"call-id": callID, "call-creator": callCreator,
				"mute-state": fmt.Sprintf("%d", muteState),
			},
		}},
	}
}

func BuildAcceptReceiptStanza(peerDeviceJid types.JID, acceptMsgID, callID string, callCreator, ourJid types.JID) waBinary.Node {
	return waBinary.Node{
		Tag:   "receipt",
		Attrs: waBinary.Attrs{"to": peerDeviceJid, "id": acceptMsgID, "from": ourJid},
		Content: []waBinary.Node{{
			Tag:   "accept",
			Attrs: waBinary.Attrs{"call-id": callID, "call-creator": callCreator},
		}},
	}
}

func callWrap(to types.JID, inner waBinary.Node) waBinary.Node {
	return waBinary.Node{
		Tag:     "call",
		Attrs:   waBinary.Attrs{"to": to, "id": GenerateCallStanzaID()},
		Content: []waBinary.Node{inner},
	}
}
