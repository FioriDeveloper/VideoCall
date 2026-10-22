package core

import (
	"os"
	"strings"
)

// EnvFlag lê uma variável de ambiente booleana (1/true/yes/on e 0/false/no/off);
// vazia ou desconhecida devolve def. Usada pelos interruptores WACALLS_1A1_*.
func EnvFlag(name string, def bool) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return def
}
