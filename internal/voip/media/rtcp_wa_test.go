package media

import (
	"encoding/hex"
	"testing"
)

func mustHex(t *testing.T, s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Bytes reais do celular (produção, 22/10/2026).
func TestParseNackFromPhone(t *testing.T) {
	got := ParseNacks(mustHex(t, "91cd000356d99331e64ecb71b25b000f"), 0xe64ecb71)
	want := []uint16{0xb25b, 0xb25c, 0xb25d, 0xb25e, 0xb25f}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
	if n := ParseNacks(mustHex(t, "91cd000356d99331e64ecb71b25b000f"), 1); len(n) != 0 {
		t.Fatal("NACK de outro SSRC não é nosso")
	}
}

func TestPrstFromPhoneAndRoundTrip(t *testing.T) {
	phone := mustHex(t, "8fce00062ff9b888000000005052535400080a040880800520000000")
	if v := ParsePrst(phone); len(v) != 1 || v[0] != 81920 {
		t.Fatalf("PRST do celular: %v", v)
	}
	b := BuildPrst(0x2ff9b888, 81920)
	if hex.EncodeToString(b) != hex.EncodeToString(phone) {
		t.Fatalf("BuildPrst difere do celular:\n%x\n%x", b, phone)
	}
	if v := ParsePrst(BuildPrst(7, 400_000)); len(v) != 1 || v[0] != 400_000 {
		t.Fatalf("ida e volta: %v", v)
	}
}
