package video

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Chaves WACALLS_1A1_* do pacote de qualidade do vídeo 1:1 (ver QUALIDADE-VIDEO-1A1.md).
// Lidas a cada Setup, então valem por chamada.
const (
	envRx      = "WACALLS_1A1_RX"         // recepção robusta (padrão LIGADO)
	envPLI     = "WACALLS_1A1_PLI"        // PLI no 1:1 (padrão LIGADO)
	envSRTCP   = "WACALLS_1A1_SRTCP"      // SR+SDES periódico + leitura do RTCP do peer (padrão desligado)
	envRR      = "WACALLS_1A1_RR"         // Receiver Report (padrão desligado)
	envTag     = "WACALLS_1A1_SRTCP_TAG"  // tamanho do tag SRTCP: 10, 4 ou both (padrão both)
	envReorder = "WACALLS_1A1_REORDER_MS" // janela de reordenação em ms (padrão 40)
)

func envBool(name string, def bool) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return def
}

func reorderWindow() time.Duration {
	ms, err := strconv.Atoi(strings.TrimSpace(os.Getenv(envReorder)))
	if err != nil || ms < 5 || ms > 200 {
		ms = 40
	}
	return time.Duration(ms) * time.Millisecond
}

// srtcpTagLens devolve os tamanhos de tag SRTCP a usar, em ordem. O tamanho no 1:1 não
// está confirmado (SRTP 1:1 = 4, SRTCP de grupo = 10), então o padrão manda os dois.
func srtcpTagLens() []int {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(envTag))) {
	case "10":
		return []int{10}
	case "4":
		return []int{4}
	}
	return []int{10, 4}
}
