package video

import (
	"reflect"
	"testing"
	"time"
)

func TestReorderInOrderAndDuplicates(t *testing.T) {
	b := newReorderBuffer[int](40*time.Millisecond, 64)
	now := time.Unix(100, 0)
	var got []int
	for _, s := range []uint16{10, 11, 11, 12, 10, 13} {
		out, lost := b.push(s, int(s), now)
		if lost != 0 {
			t.Fatalf("perda inesperada: %d", lost)
		}
		got = append(got, out...)
	}
	if !reflect.DeepEqual(got, []int{10, 11, 12, 13}) || b.Dups != 2 {
		t.Fatalf("got=%v dups=%d", got, b.Dups)
	}
}

func TestReorderFixesSwapWithinWindow(t *testing.T) {
	b := newReorderBuffer[int](40*time.Millisecond, 64)
	now := time.Unix(100, 0)
	b.push(1, 1, now)
	if out, _ := b.push(3, 3, now); len(out) != 0 {
		t.Fatalf("3 deveria ficar retido: %v", out)
	}
	out, lost := b.push(2, 2, now.Add(5*time.Millisecond))
	if lost != 0 || !reflect.DeepEqual(out, []int{2, 3}) {
		t.Fatalf("out=%v lost=%d", out, lost)
	}
}

func TestReorderGapExpiresAsLoss(t *testing.T) {
	b := newReorderBuffer[int](40*time.Millisecond, 64)
	now := time.Unix(100, 0)
	b.push(1, 1, now)
	b.push(4, 4, now) // faltam 2 e 3
	b.push(5, 5, now.Add(10*time.Millisecond))
	if d := b.nextDeadline(); !d.Equal(now.Add(40 * time.Millisecond)) {
		t.Fatalf("deadline=%v", d)
	}
	if out, lost := b.expire(now.Add(30 * time.Millisecond)); len(out) != 0 || lost != 0 {
		t.Fatalf("expirou cedo: %v %d", out, lost)
	}
	out, lost := b.expire(now.Add(41 * time.Millisecond))
	if lost != 2 || !reflect.DeepEqual(out, []int{4, 5}) {
		t.Fatalf("out=%v lost=%d", out, lost)
	}
	// o pacote atrasado que chega depois é descartado
	if out, _ := b.push(2, 2, now.Add(50*time.Millisecond)); len(out) != 0 || b.Dups != 1 {
		t.Fatalf("atrasado deveria ser descartado: %v dups=%d", out, b.Dups)
	}
}

func TestReorderWrapAround(t *testing.T) {
	b := newReorderBuffer[int](40*time.Millisecond, 64)
	now := time.Unix(100, 0)
	var got []int
	for _, s := range []uint16{65534, 0, 65535, 1} {
		out, _ := b.push(s, int(s), now)
		got = append(got, out...)
	}
	if !reflect.DeepEqual(got, []int{65534, 65535, 0, 1}) {
		t.Fatalf("got=%v", got)
	}
}

func TestReorderMaxHeldForcesSkip(t *testing.T) {
	b := newReorderBuffer[int](time.Hour, 3)
	now := time.Unix(100, 0)
	b.push(1, 1, now)
	var out []int
	var lost int
	for _, s := range []uint16{3, 4, 5, 6} {
		o, l := b.push(s, int(s), now)
		out, lost = append(out, o...), lost+l
	}
	if lost != 1 || !reflect.DeepEqual(out, []int{3, 4, 5, 6}) {
		t.Fatalf("out=%v lost=%d", out, lost)
	}
}
