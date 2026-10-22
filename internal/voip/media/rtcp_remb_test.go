package media

import (
	"bytes"
	"testing"
)

func TestRembRoundTrip(t *testing.T) {
	b := BuildRemb(0x11223344, 500_000, []uint32{0xaabbccdd, 0x01020304})
	want := []byte{0x8f, 206, 0, 6, 0x11, 0x22, 0x33, 0x44, 0, 0, 0, 0, 'R', 'E', 'M', 'B'}
	if !bytes.Equal(b[:16], want) || len(b) != 28 {
		t.Fatalf("cabeçalho %x (len %d)", b[:16], len(b))
	}
	// 500000 = 0x7A120 -> exp 1, mantissa 0x3D090
	if b[16] != 2 || b[17] != 0x07 || b[18] != 0xd0 || b[19] != 0x90 {
		t.Fatalf("num/exp/mantissa %x", b[16:20])
	}
	pli := BuildPictureLossIndication(1, 2, true)
	rs := ParseRembs(append(append([]byte{}, pli[:]...), b...))
	if len(rs) != 1 || rs[0].Bitrate != 500_000 || len(rs[0].Ssrcs) != 2 || rs[0].Ssrcs[1] != 0x01020304 || rs[0].Sender != 0x11223344 {
		t.Fatalf("parse: %+v", rs)
	}
}
