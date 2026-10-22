package video

import "time"

// reorderBuffer reordena pacotes RTP (por número de sequência de 16 bits), descarta
// duplicados/atrasados e declara perda quando uma lacuna não fecha dentro da janela.
// O datachannel do relay é não ordenado e há até 2 relays abertos entregando o mesmo
// pacote, então reordenar e deduplicar antes do despacote é obrigatório.
//
// Não é thread-safe: o Pipeline serializa com rxMu.
type reorderBuffer[T any] struct {
	window  time.Duration // quanto esperar por uma lacuna antes de declará-la perda
	maxHeld int           // teto de pacotes retidos (estoura => pula a lacuna)

	started bool
	next    uint16
	held    map[uint16]heldItem[T]

	Dups uint64 // duplicados/atrasados descartados
}

type heldItem[T any] struct {
	v  T
	at time.Time
}

func newReorderBuffer[T any](window time.Duration, maxHeld int) *reorderBuffer[T] {
	return &reorderBuffer[T]{window: window, maxHeld: maxHeld, held: map[uint16]heldItem[T]{}}
}

// seqDelta é a distância com sinal de a até b no espaço circular de 16 bits.
func seqDelta(a, b uint16) int { return int(int16(b - a)) }

// push insere o pacote seq. Devolve os itens agora entregáveis em ordem e quantos
// pacotes foram declarados perdidos ANTES do primeiro item devolvido (lacuna pulada).
func (b *reorderBuffer[T]) push(seq uint16, v T, now time.Time) (out []T, lost int) {
	if !b.started {
		b.started = true
		b.next = seq + 1
		return []T{v}, 0
	}
	d := seqDelta(b.next, seq)
	switch {
	case d < 0:
		b.Dups++ // já entregue/pulado: duplicado (2 relays) ou atrasado demais
		return nil, 0
	case d == 0:
		out = append(out, v)
		b.next++
	case d > 2000:
		// salto absurdo: o stream reiniciou. Ressincroniza sem reportar perda grande.
		b.held = map[uint16]heldItem[T]{}
		b.next = seq + 1
		return []T{v}, 1
	default:
		if _, dup := b.held[seq]; dup {
			b.Dups++
			return nil, 0
		}
		b.held[seq] = heldItem[T]{v, now}
	}
	out, lost = b.drain(out, 0)
	// Estourou o teto ou o mais antigo passou da janela: pula a lacuna.
	for len(b.held) > 0 && (len(b.held) > b.maxHeld || now.Sub(b.oldest()) >= b.window) {
		out, lost = b.skip(out, lost)
	}
	return out, lost
}

// expire pula as lacunas cujo pacote seguinte já esperou a janela inteira (chamado
// por timer quando nada novo chega).
func (b *reorderBuffer[T]) expire(now time.Time) (out []T, lost int) {
	for len(b.held) > 0 && now.Sub(b.oldest()) >= b.window {
		out, lost = b.skip(out, lost)
	}
	return out, lost
}

// nextDeadline diz quando o mais antigo retido vence (zero = nada retido).
func (b *reorderBuffer[T]) nextDeadline() time.Time {
	if len(b.held) == 0 {
		return time.Time{}
	}
	return b.oldest().Add(b.window)
}

func (b *reorderBuffer[T]) oldest() time.Time {
	var o time.Time
	for _, h := range b.held {
		if o.IsZero() || h.at.Before(o) {
			o = h.at
		}
	}
	return o
}

// drain entrega os retidos contíguos a partir de next.
func (b *reorderBuffer[T]) drain(out []T, lost int) ([]T, int) {
	for {
		h, ok := b.held[b.next]
		if !ok {
			return out, lost
		}
		delete(b.held, b.next)
		out = append(out, h.v)
		b.next++
	}
}

// skip avança next até o retido mais próximo, contando a lacuna como perda.
func (b *reorderBuffer[T]) skip(out []T, lost int) ([]T, int) {
	best, bestD := uint16(0), 1<<30
	for s := range b.held {
		if d := seqDelta(b.next, s); d >= 0 && d < bestD {
			best, bestD = s, d
		}
	}
	if bestD == 1<<30 { // só sobraram atrasados (não deveria ocorrer): limpa
		b.held = map[uint16]heldItem[T]{}
		return out, lost
	}
	lost += bestD
	b.next = best
	return b.drain(out, lost)
}

func (b *reorderBuffer[T]) reset() {
	b.started = false
	b.held = map[uint16]heldItem[T]{}
}
