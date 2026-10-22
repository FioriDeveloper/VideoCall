package transport

import "time"

// RtpReorderer é um pequeno buffer de reordenação por número de sequência RTP,
// sem timers próprios (quem usa chama Flush). Descarta duplicados e pacotes
// atrasados demais (já passados), entrega em ordem e informa lacunas.
//
// Motivo: o datachannel do relay é Ordered:false e há 2 relays abertos entregando
// no mesmo callback, então o vídeo do peer chega fora de ordem e duplicado.
// Não é thread-safe: o chamador serializa.
type RtpReorderer[T any] struct {
	window  time.Duration
	maxSpan int

	init bool
	next uint16
	buf  map[uint16]reorderEntry[T]

	Duplicates uint64 // descartados por já terem sido entregues/estarem no buffer
	Lost       uint64 // pacotes dados como perdidos (lacuna fechada por timeout)

	pendingLost int
}

type reorderEntry[T any] struct {
	item T
	at   time.Time
}

// Ordered é um item entregue em ordem. LostBefore > 0 indica quantos pacotes
// faltaram imediatamente antes dele.
type Ordered[T any] struct {
	Seq        uint16
	Item       T
	LostBefore int
}

// NewRtpReorderer cria o reordenador. window é quanto esperar por um pacote
// faltante antes de declará-lo perdido; maxSpan limita a distância aceita à frente.
func NewRtpReorderer[T any](window time.Duration, maxSpan int) *RtpReorderer[T] {
	if maxSpan <= 0 {
		maxSpan = 512
	}
	return &RtpReorderer[T]{window: window, maxSpan: maxSpan, buf: map[uint16]reorderEntry[T]{}}
}

// Pending diz quantos pacotes estão retidos esperando um faltante.
func (r *RtpReorderer[T]) Pending() int { return len(r.buf) }

// Push insere um pacote e devolve o que ficou pronto em ordem.
func (r *RtpReorderer[T]) Push(seq uint16, item T, now time.Time) []Ordered[T] {
	if !r.init {
		r.init = true
		r.next = seq
	}
	d := int(int16(seq - r.next))
	if d < 0 {
		r.Duplicates++
		return nil
	}
	if _, ok := r.buf[seq]; ok {
		r.Duplicates++
		return nil
	}
	var out []Ordered[T]
	if d >= r.maxSpan {
		// salto grande: entrega o que há (com lacuna) e ressincroniza no novo.
		out = r.skipTo(r.minPending(), out)
		out = r.drain(out)
		d = int(int16(seq - r.next))
		if d >= r.maxSpan || d < 0 {
			lost := 0
			if d > 0 {
				lost = d
				r.Lost += uint64(d)
			}
			r.buf[seq] = reorderEntry[T]{item, now}
			r.next = seq
			return r.drainWithLost(out, lost)
		}
	}
	r.buf[seq] = reorderEntry[T]{item, now}
	return r.drain(out)
}

// Flush libera pacotes retidos além da janela, pulando as lacunas (lost).
func (r *RtpReorderer[T]) Flush(now time.Time) []Ordered[T] {
	var out []Ordered[T]
	for len(r.buf) > 0 {
		first := r.minPending()
		e := r.buf[first]
		if now.Sub(e.at) < r.window {
			// o mais antigo na espera pode não ser o de menor seq; checa o menor.
			break
		}
		out = r.skipTo(first, out)
		out = r.drain(out)
	}
	return out
}

// NextDeadline devolve quando o Flush precisa rodar (zero = nada retido).
func (r *RtpReorderer[T]) NextDeadline() time.Time {
	if len(r.buf) == 0 {
		return time.Time{}
	}
	return r.buf[r.minPending()].at.Add(r.window)
}

func (r *RtpReorderer[T]) minPending() uint16 {
	var best uint16
	bestD := int(^uint(0) >> 1)
	for s := range r.buf {
		if d := int(uint16(s - r.next)); d < bestD {
			bestD, best = d, s
		}
	}
	return best
}

// skipTo avança next até seq registrando a lacuna; a perda é anexada ao próximo drain.
func (r *RtpReorderer[T]) skipTo(seq uint16, out []Ordered[T]) []Ordered[T] {
	if len(r.buf) == 0 {
		return out
	}
	gap := int(uint16(seq - r.next))
	r.Lost += uint64(gap)
	r.pendingLost = gap
	r.next = seq
	return out
}

func (r *RtpReorderer[T]) drain(out []Ordered[T]) []Ordered[T] {
	return r.drainWithLost(out, 0)
}

func (r *RtpReorderer[T]) drainWithLost(out []Ordered[T], lost int) []Ordered[T] {
	lost += r.pendingLost
	r.pendingLost = 0
	for {
		e, ok := r.buf[r.next]
		if !ok {
			break
		}
		delete(r.buf, r.next)
		out = append(out, Ordered[T]{Seq: r.next, Item: e.item, LostBefore: lost})
		lost = 0
		r.next++
	}
	if lost > 0 {
		r.pendingLost = lost
	}
	return out
}
