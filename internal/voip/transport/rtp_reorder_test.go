package transport

import (
	"testing"
	"time"
)

func seqs(o []Ordered[int]) []uint16 {
	var s []uint16
	for _, x := range o {
		s = append(s, x.Seq)
	}
	return s
}

func eq(a, b []uint16) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestReordererInOrderAndReorder(t *testing.T) {
	r := NewRtpReorderer[int](40*time.Millisecond, 0)
	now := time.Unix(0, 0)
	if got := seqs(r.Push(10, 0, now)); !eq(got, []uint16{10}) {
		t.Fatalf("got %v", got)
	}
	if got := r.Push(12, 0, now); len(got) != 0 {
		t.Fatalf("12 deve ficar retido, got %v", seqs(got))
	}
	if got := seqs(r.Push(11, 0, now)); !eq(got, []uint16{11, 12}) {
		t.Fatalf("got %v", got)
	}
}

func TestReordererDropsDuplicates(t *testing.T) {
	r := NewRtpReorderer[int](40*time.Millisecond, 0)
	now := time.Unix(0, 0)
	r.Push(1, 0, now)
	if got := r.Push(1, 0, now); len(got) != 0 {
		t.Fatal("duplicado já entregue deve ser descartado")
	}
	r.Push(3, 0, now)
	if got := r.Push(3, 0, now); len(got) != 0 {
		t.Fatal("duplicado retido deve ser descartado")
	}
	if r.Duplicates != 2 {
		t.Fatalf("Duplicates=%d", r.Duplicates)
	}
}

func TestReordererGapOnTimeout(t *testing.T) {
	r := NewRtpReorderer[int](40*time.Millisecond, 0)
	t0 := time.Unix(0, 0)
	r.Push(1, 0, t0)
	r.Push(4, 0, t0)
	r.Push(5, 0, t0.Add(5*time.Millisecond))
	if got := r.Flush(t0.Add(10 * time.Millisecond)); len(got) != 0 {
		t.Fatal("antes da janela não libera")
	}
	got := r.Flush(t0.Add(50 * time.Millisecond))
	if !eq(seqs(got), []uint16{4, 5}) || got[0].LostBefore != 2 || got[1].LostBefore != 0 {
		t.Fatalf("got %+v", got)
	}
	// atrasado que chega depois do skip é descartado
	if g := r.Push(2, 0, t0.Add(60*time.Millisecond)); len(g) != 0 {
		t.Fatal("pacote atrasado deve ser descartado")
	}
}

func TestReordererSeqWrap(t *testing.T) {
	r := NewRtpReorderer[int](40*time.Millisecond, 0)
	now := time.Unix(0, 0)
	r.Push(65534, 0, now)
	r.Push(0, 0, now)
	if got := seqs(r.Push(65535, 0, now)); !eq(got, []uint16{65535, 0}) {
		t.Fatalf("got %v", got)
	}
}

func TestReordererBigJumpResync(t *testing.T) {
	r := NewRtpReorderer[int](40*time.Millisecond, 100)
	now := time.Unix(0, 0)
	r.Push(1, 0, now)
	got := r.Push(1000, 0, now)
	if !eq(seqs(got), []uint16{1000}) || got[0].LostBefore == 0 {
		t.Fatalf("got %+v", got)
	}
}
