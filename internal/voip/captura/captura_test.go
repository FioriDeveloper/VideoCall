package captura

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGravaFormatoENovoArquivoPorChamada(t *testing.T) {
	old := dir
	dir = t.TempDir()
	mu.Lock()
	f, ult = nil, time.Time{} // estado de uma rodada anterior (-count>1)
	mu.Unlock()
	defer func() { dir, f, ult = old, nil, time.Time{} }()

	Grava('a', []byte{0x80, 0x78}, []byte{1, 2, 3})
	Grava('r', nil, []byte{9})
	mu.Lock()
	_ = f.Close()
	f = nil
	mu.Unlock()

	nomes, _ := filepath.Glob(filepath.Join(dir, "*.cap"))
	if len(nomes) != 1 {
		t.Fatalf("arquivos: %v", nomes)
	}
	b, _ := os.ReadFile(nomes[0])
	if len(b) != 13+5+13+1 {
		t.Fatalf("tamanho %d", len(b))
	}
	if b[8] != 'a' || binary.BigEndian.Uint32(b[9:13]) != 5 || b[13] != 0x80 || b[17] != 3 {
		t.Fatalf("1º registro %x", b[:18])
	}
	if b[18+8] != 'r' || b[18+13] != 9 {
		t.Fatalf("2º registro %x", b[18:])
	}
	if st, _ := os.Stat(nomes[0]); st.Mode().Perm() != 0o600 {
		t.Fatalf("permissão %v", st.Mode().Perm())
	}
}
