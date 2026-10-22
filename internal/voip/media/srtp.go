package media

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"sync"

	"wacalls/internal/voip/core"
)

type SrtpErrorType string

const (
	SrtpErrPacketTooShort SrtpErrorType = "packet_too_short"
	SrtpErrAuthFailed     SrtpErrorType = "auth_failed"
	SrtpErrEncryption     SrtpErrorType = "encryption"
	SrtpErrDecryption     SrtpErrorType = "decryption"
)

type SrtpError struct {
	Type SrtpErrorType
	Msg  string
}

func (e *SrtpError) Error() string { return fmt.Sprintf("srtp %s: %s", e.Type, e.Msg) }

// rocState é o estado de ROC/sequência de UM stream (RFC 3711 §3.3.1).
type rocState struct {
	roc         uint32
	lastSeq     uint16
	initialized bool
}

// maxRecvStreams limita os streams lembrados na recepção (lixo/SSRC aleatório).
const maxRecvStreams = 64

type SrtpContext struct {
	sessionKey  []byte
	sessionSalt []byte
	authKey     []byte
	rocState    // envio (um SSRC só)
	authTagLen  int

	// Recepção: ROC por SSRC. Com um estado só, pacotes de outro SSRC (outro
	// aparelho do contato, outro fluxo, vários participantes) mexiam no ROC do
	// stream certo e, conforme as sequências, tudo passava a decifrar lixo.
	recvMu  sync.Mutex
	recvRoc map[uint32]*rocState
}

func NewSrtpContext(keying core.SrtpKeyingMaterial, authTagLen int) (*SrtpContext, error) {
	if authTagLen <= 0 {
		authTagLen = core.SRTPAuthTagLen
	}
	sk, err := deriveSrtpKey(keying.MasterKey, keying.MasterSalt, core.SRTPLabelEncryption, 16)
	if err != nil {
		return nil, err
	}
	ak, err := deriveSrtpKey(keying.MasterKey, keying.MasterSalt, core.SRTPLabelAuth, 20)
	if err != nil {
		return nil, err
	}
	ss, err := deriveSrtpKey(keying.MasterKey, keying.MasterSalt, core.SRTPLabelSalt, 14)
	if err != nil {
		return nil, err
	}
	return &SrtpContext{
		sessionKey:  sk,
		sessionSalt: ss,
		authKey:     ak,
		authTagLen:  authTagLen,
	}, nil
}

// WithKeying devolve um contexto NOVO com as chaves de sessão derivadas de keying mas
// preservando o estado de ROC/sequência deste — rotação de chave (epoch de grupo) sem
// quebrar a continuidade do stream. O contexto antigo não é alterado (sem data race
// com um Unprotect/Protect em andamento).
func (c *SrtpContext) WithKeying(keying core.SrtpKeyingMaterial) (*SrtpContext, error) {
	n, err := NewSrtpContext(keying, c.authTagLen)
	if err != nil {
		return nil, err
	}
	n.roc, n.lastSeq, n.initialized = c.roc, c.lastSeq, c.initialized
	c.recvMu.Lock()
	if len(c.recvRoc) > 0 {
		n.recvRoc = make(map[uint32]*rocState, len(c.recvRoc))
		for ssrc, st := range c.recvRoc {
			cp := *st
			n.recvRoc[ssrc] = &cp
		}
	}
	c.recvMu.Unlock()
	return n, nil
}

func (c *SrtpContext) SetAuthKeying(keying core.SrtpKeyingMaterial) error {
	ak, err := deriveSrtpKey(keying.MasterKey, keying.MasterSalt, core.SRTPLabelAuth, 20)
	if err != nil {
		return err
	}
	c.authKey = ak
	return nil
}

func (c *SrtpContext) Protect(packet *RtpPacket) ([]byte, error) {
	c.updateRoc(packet.Header.SequenceNumber)
	index := c.packetIndex(packet.Header.SequenceNumber)

	headerSize := packet.Header.Size()
	output := make([]byte, headerSize+len(packet.Payload)+c.authTagLen)

	if _, err := packet.Header.Encode(output); err != nil {
		return nil, &SrtpError{SrtpErrEncryption, err.Error()}
	}

	iv := c.generateIV(packet.Header.Ssrc, index)
	if err := aesCtrXor(c.sessionKey, iv, packet.Payload, output[headerSize:headerSize+len(packet.Payload)]); err != nil {
		return nil, &SrtpError{SrtpErrEncryption, err.Error()}
	}

	if c.authTagLen > 0 {
		authData := output[:headerSize+len(packet.Payload)]
		tag := c.computeAuthTag(authData, c.roc, c.authTagLen)
		copy(output[headerSize+len(packet.Payload):], tag)
	}

	return output, nil
}

func (c *SrtpContext) Unprotect(data []byte) (*RtpPacket, error) {
	if len(data) < 12 {
		return nil, &SrtpError{SrtpErrPacketTooShort, fmt.Sprintf("packet too short: %d bytes", len(data))}
	}

	header, err := DecodeRtpHeader(data)
	if err != nil {
		return nil, &SrtpError{SrtpErrDecryption, err.Error()}
	}
	headerSize := header.Size()
	payloadLen := len(data) - headerSize - c.authTagLen
	if payloadLen <= 0 {
		return nil, &SrtpError{SrtpErrPacketTooShort, fmt.Sprintf("no payload: %dB total, %dB header, auth=%d", len(data), headerSize, c.authTagLen)}
	}

	roc := c.recvRocFor(header.Ssrc, header.SequenceNumber)
	index := (uint64(roc) << 16) | uint64(header.SequenceNumber)

	iv := c.generateIV(header.Ssrc, index)
	decrypted := make([]byte, payloadLen)
	if err := aesCtrXor(c.sessionKey, iv, data[headerSize:headerSize+payloadLen], decrypted); err != nil {
		return nil, &SrtpError{SrtpErrDecryption, err.Error()}
	}

	return &RtpPacket{Header: header, Payload: decrypted}, nil
}

// updateRoc estima o ROC do pacote recebido (RFC 3711 §3.3.1) e avança o estado só
// quando o pacote é o mais novo visto. Pacotes FORA DE ORDEM perto da virada da
// sequência (seq 0 chegando antes do 65535, ou o contrário) usam o ROC certo e não
// mexem no estado — antes, o 65535 atrasado fazia o ROC subir duas vezes e todo o
// resto do stream decifrava lixo (o datachannel do relay é não ordenado).
func (c *SrtpContext) updateRoc(seq uint16) uint32 { return c.rocState.update(seq) }

// recvRocFor estima o ROC do pacote recebido no estado do SSRC dele.
func (c *SrtpContext) recvRocFor(ssrc uint32, seq uint16) uint32 {
	c.recvMu.Lock()
	defer c.recvMu.Unlock()
	st := c.recvRoc[ssrc]
	if st == nil {
		if c.recvRoc == nil || len(c.recvRoc) >= maxRecvStreams {
			c.recvRoc = map[uint32]*rocState{}
		}
		st = &rocState{}
		c.recvRoc[ssrc] = st
	}
	return st.update(seq)
}

func (st *rocState) update(seq uint16) uint32 {
	if !st.initialized {
		st.lastSeq = seq
		st.initialized = true
		return st.roc
	}
	roc := st.roc
	if st.lastSeq < 0x8000 {
		if seq > st.lastSeq && seq-st.lastSeq > 0x8000 && roc > 0 {
			return roc - 1 // pacote atrasado da volta anterior
		}
	} else if st.lastSeq-0x8000 > seq {
		// virou a sequência: este pacote já é da volta seguinte
		st.roc++
		st.lastSeq = seq
		return st.roc
	}
	if seq > st.lastSeq {
		st.lastSeq = seq
	}
	return roc
}

func (c *SrtpContext) packetIndex(seq uint16) uint64 {
	return (uint64(c.roc) << 16) | uint64(seq)
}

func (c *SrtpContext) generateIV(ssrc uint32, index uint64) []byte {
	iv := make([]byte, 16)
	copy(iv, c.sessionSalt[:14])

	var ssrcBuf [4]byte
	binary.BigEndian.PutUint32(ssrcBuf[:], ssrc)
	for i := 0; i < 4; i++ {
		iv[4+i] ^= ssrcBuf[i]
	}

	var idxBuf [8]byte
	binary.BigEndian.PutUint64(idxBuf[:], index)
	for i := 0; i < 6; i++ {
		iv[8+i] ^= idxBuf[2+i]
	}

	return iv
}

func (c *SrtpContext) computeAuthTag(data []byte, roc uint32, tagLen int) []byte {
	mac := hmac.New(sha1.New, c.authKey)
	mac.Write(data)
	var rocBuf [4]byte
	binary.BigEndian.PutUint32(rocBuf[:], roc)
	mac.Write(rocBuf[:])
	sum := mac.Sum(nil)
	return sum[:tagLen]
}

type SrtpSession struct {
	sendCtx *SrtpContext
	recvCtx *SrtpContext
}

func NewSrtpSession(sendKey, recvKey core.SrtpKeyingMaterial, sendAuthLen, recvAuthLen int) (*SrtpSession, error) {
	sc, err := NewSrtpContext(sendKey, sendAuthLen)
	if err != nil {
		return nil, err
	}
	rc, err := NewSrtpContext(recvKey, recvAuthLen)
	if err != nil {
		return nil, err
	}
	return &SrtpSession{sendCtx: sc, recvCtx: rc}, nil
}

func (s *SrtpSession) Protect(packet *RtpPacket) ([]byte, error) { return s.sendCtx.Protect(packet) }

func (s *SrtpSession) Unprotect(data []byte) (*RtpPacket, error) { return s.recvCtx.Unprotect(data) }

// WithKeying devolve uma sessão NOVA rechaveada (envio e recepção) preservando o
// estado de ROC dos dois lados. Ver SrtpContext.WithKeying.
func (s *SrtpSession) WithKeying(sendKey, recvKey core.SrtpKeyingMaterial) (*SrtpSession, error) {
	sc, err := s.sendCtx.WithKeying(sendKey)
	if err != nil {
		return nil, err
	}
	rc, err := s.recvCtx.WithKeying(recvKey)
	if err != nil {
		return nil, err
	}
	return &SrtpSession{sendCtx: sc, recvCtx: rc}, nil
}

func (s *SrtpSession) SetSendAuthKeying(keying core.SrtpKeyingMaterial) error {
	return s.sendCtx.SetAuthKeying(keying)
}

func deriveSrtpKey(masterKey, masterSalt []byte, label byte, length int) ([]byte, error) {
	if len(masterSalt) < 14 {
		return nil, fmt.Errorf("srtp: master salt curto demais (%d bytes, precisa 14)", len(masterSalt))
	}
	iv := make([]byte, 16)
	copy(iv, masterSalt[:14])
	iv[7] ^= label

	out := make([]byte, length)
	if err := aesCtrXor(masterKey, iv, make([]byte, length), out); err != nil {
		return nil, err
	}
	return out, nil
}

func aesCtrXor(key, iv, src, dst []byte) error {
	block, err := aes.NewCipher(key)
	if err != nil {
		return err
	}
	cipher.NewCTR(block, iv).XORKeyStream(dst, src)
	return nil
}
