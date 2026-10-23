// Package captura grava, para diagnóstico, os pacotes de uma chamada 1:1 já
// decifrados (áudio, vídeo e RTCP do celular), com a hora de chegada, para
// reproduzir a chamada fora do ar (cmd/analisar-captura).
//
// Só liga com WACALLS_1A1_CAPTURA_DIR. Um arquivo por chamada (abre outro depois
// de 10 s sem pacotes), no máximo 90 s e 20 MB cada. O conteúdo é a mídia da
// chamada: use só em chamada de teste e apague depois da análise.
//
// Formato: registros [int64 unix nano][1 B tipo 'a'|'v'|'r'][uint32 tamanho][bytes],
// big-endian; 'a'/'v' = cabeçalho RTP + payload decifrado, 'r' = RTCP decifrado.
package captura

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	maxDur   = 90 * time.Second
	maxBytes = 20 << 20
	idle     = 10 * time.Second
)

var (
	dir = strings.TrimSpace(os.Getenv("WACALLS_1A1_CAPTURA_DIR"))
	mu  sync.Mutex
	f   *os.File
	ini time.Time
	ult time.Time
	n   int64
)

// Ligada diz se a captura está configurada.
func Ligada() bool { return dir != "" }

// Grava um registro (hdr e corpo são concatenados). Nunca falha para quem chama.
func Grava(tipo byte, hdr, corpo []byte) {
	if dir == "" {
		return
	}
	agora := time.Now()
	mu.Lock()
	defer mu.Unlock()
	if f != nil && agora.Sub(ult) > idle {
		_ = f.Close()
		f = nil
	}
	if f == nil {
		if !ult.IsZero() && agora.Sub(ult) <= idle {
			ult = agora
			return // esta chamada já passou do limite
		}
		_ = os.MkdirAll(dir, 0o700)
		nf, err := os.OpenFile(filepath.Join(dir, agora.UTC().Format("20060102-150405")+".cap"), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
		if err != nil {
			ult = agora
			return
		}
		f, ini, n = nf, agora, 0
	}
	ult = agora
	if agora.Sub(ini) > maxDur || n > maxBytes {
		_ = f.Close()
		f = nil
		return
	}
	var cab [13]byte
	binary.BigEndian.PutUint64(cab[0:8], uint64(agora.UnixNano()))
	cab[8] = tipo
	binary.BigEndian.PutUint32(cab[9:13], uint32(len(hdr)+len(corpo)))
	_, _ = f.Write(cab[:])
	_, _ = f.Write(hdr)
	_, _ = f.Write(corpo)
	n += int64(13 + len(hdr) + len(corpo))
}
