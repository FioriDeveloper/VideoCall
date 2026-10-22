# Qualidade do vídeo 1:1 (fork FioriDeveloper/VideoCall)

Branch `qualidade-video-1a1`. Objetivo: aumentar fluidez/qualidade do vídeo **que o celular do
contato envia** (navegador ↔ servidor ↔ relay do WhatsApp ↔ celular). Medição em produção
antes das mudanças: o contato vê bem o atendente; o vídeo do contato chega a **26–30 kbps e
4–5 quadros/s**, congelando; áudio do contato em rajadas (jitter 63–78 ms). Config do WhatsApp
na chamada: `cond_low_quality_vid_mode` (15000–25000 bps), `bwe_impl 8` (estimador por atraso).
Só relay, sem P2P.

## Diagnóstico confirmado no código

| Achado | Onde |
|---|---|
| No 1:1 não saía RTCP algum e o recebido era descartado (só PT 97/120 tratados) | `callmanager_media.go` `onRelayData` |
| Grupo já tinha SR+SDES a cada 1,5 s, SRTCP e PLI com limite de 300 ms | `groupcall.go`, `media/rtcp.go`, `media/srtcp.go` |
| Vídeo recebido sem filtro de SSRC, sem reordenação (`Ordered:false`), sem descarte de duplicados (2 relays), `depack`/`srtp` fora do mutex, FU-A fora de ordem gerava NAL corrompido, sem detecção de lacuna, sem PLI | `video/pipeline.go`, `transport/h264_packet.go`, `sctprelay.go` |
| Nosso `InitialBandwidth` (ext. 0xDEBE id 5) vai 0 e o `TransportSequence` (id 9) é só enviado | `pipeline.go` `buildWhatsappVideoExt` |
| `Broadcast` manda tudo nos 2 relays abertos | `sctprelay.go` |
| `SRTP` 1:1 **não verifica o tag** (só calcula o tamanho do payload) — por isso o filtro de SSRC é o único guarda contra pacotes estranhos | `media/srtp.go` `Unprotect` |

## O que mudou e por quê

Variáveis (`1/true/on` ligam; `0/false/off` desligam). **Padrão ligado só nos itens 1 e 2.**

| Item | Variável | Padrão | Mudança |
|---|---|---|---|
| 1 PLI | `WACALLS_1A1_PLI` | **ligado** | PLI SRTCP ao SSRC de vídeo do peer: a cada 1 s até o 1º IDR, e (≥300 ms) a cada lacuna até chegar IDR. Navegador pode pedir com texto `{"type":"pli"}` no canal `h264` (`Bridge.OnBrowserKeyframeRequest`); texto desconhecido é ignorado. |
| 2 Recepção | `WACALLS_1A1_RX`, `WACALLS_1A1_REORDER_MS` | **ligado**, 40 ms | `video/rx.go` + `transport/rtp_reorder.go`: trava no SSRC do peer (1º pacote), descarta duplicados, reordena em janela curta (timer solta pacotes retidos), `depack`/`srtp` sob mutex. Lacuna ⇒ descarta o quadro em curso e os seguintes até o próximo IDR e pede PLI (nada corrompido vai ao navegador). Antes do 1º IDR o comportamento antigo é mantido (entrega) para não arriscar tela preta. Grupo continua no caminho antigo (só ganhou o mutex). |
| 3 SRTCP | `WACALLS_1A1_SRTCP` | desligado | SR+SDES (áudio e vídeo) a cada 1,5 s; leitura/autenticação do RTCP do celular; PLI/FIR para o NOSSO vídeo ⇒ `OnPeerKeyframeRequest` ⇒ texto `{"type":"keyframe_request"}` no canal `h264` ⇒ `VideoSender.forceKeyframe()` (cliente ajustado; **exige cliente novo**, por isso desligado). Loga uma vez cada (PT, FMT) recebido do celular (evidência p/ item 4). |
| 4 RR | `WACALLS_1A1_RR` | desligado | RR (PT 201) com fração/total perdido, jitter (RFC 3550) e LSR/DLSR, por stream (áudio 16 kHz, vídeo 90 kHz), sem contar duplicados. |
| 5 A/B | `WACALLS_1A1_SIGNALING_AB`, `WACALLS_1A1_SINGLE_RELAY` | desligado | Accept de vídeo no formato do offer (`enc="h.264" dec="H264" screen_* device_orientation`) + capability de vídeo; preaccept com capability de vídeo em chamada de vídeo; aceite de upgrade com `dec="H264"` (sem AV1). `SINGLE_RELAY`: `Broadcast` só no relay que entregou mídia nos últimos 3 s (senão, todos); não afeta grupo. |
| Ajustes | `WACALLS_1A1_SRTCP_TAG` (4\|10), `WACALLS_1A1_RTCP_PROFILE_BIT` | 10, ligado | Ver suposições. |

## Fato × suposição

**Fatos (no código/testes):** derivação SRTCP 1:1 = mesma fórmula do SRTP 1:1 (HKDF-SHA256, info = JID do device, 46 bytes) com rótulos 3/4/5, igual ao grupo; formatos de SR/SDES/PLI batem byte a byte com os vetores do meowcaller; RR segue a RFC 3550.

**Suposições (precisam de chamada real):**
- Tag SRTCP do 1:1: SRTP 1:1 usa 4 B e SRTCP do grupo 10 B (SRTP do grupo também 4 B) ⇒ assumimos 10 B. Na **recepção** testamos 10 e 4 e adotamos o que autenticar (log `srtcp 1:1: RTCP do celular autenticado ... tag=`), passando a enviar com ele. O PLI/SR enviado antes da detecção usa o valor configurado; se o celular rejeitar, nada quebra (ele ignora).
- Bit de perfil `0x10` nos RTCP de vídeo (valor do grupo).
- O relay entregar RTCP do celular ao nosso 1:1 e o celular honrar PLI/RR.
- Capability de preaccept de vídeo `…0xfa 0x07` é **inferida** (só existem observadas a de preaccept de áudio e a de offer de vídeo/áudio).
- SSRC do vídeo do peer: derivamos o esperado (slot 2) só para log; a trava é no 1º pacote aceito (se o JID do device diferir do derivado, não perdemos o vídeo). Log `bate=true/false` mostra se a derivação coincide.

**Feedback de banda (item 4) — pesquisa:** README/páginas públicas do meowcaller e buscas não documentam transport-cc (PT 205 FMT 15) sobre o TransportSequence id 9, REMB nem mensagem própria do WhatsApp; nem o tamanho do tag SRTCP 1:1. **Sem base, nada de transport-cc/REMB foi implementado** (seria chute que podia piorar a estimativa). O RR é padrão RFC e fica desligado; o log de (PT, FMT) recebidos do celular com `WACALLS_1A1_SRTCP=1` mostra o que o WhatsApp realmente manda e deve guiar o próximo passo. Hipótese a testar: `bwe_impl 8` (por atraso) usa o `TransportSequence` + feedback do receptor; como nós (receptor do vídeo do contato) não devolvemos feedback algum, o emissor (celular) pode estar travado em ~25 kbps (`cond_low_quality_vid_mode`).

## Testado (sem celular)

`go build ./...` ok; `go vet ./internal/voip/...` ok; `go test ./...` ok (cgo + opus_mlow compilaram no ambiente, nenhum pacote ficou de fora); `go test -race` nos pacotes `internal/voip/...` ok. Testes novos: reordenador (ordem, troca, duplicados, lacuna por timeout, wrap de seq, salto grande), pipeline de recepção (fragmentos fora de ordem + duplicados reconstroem o AU idêntico; lacuna ⇒ AU descartado, P descartado até IDR, PLI emitido; SSRC estranho filtrado; PLI antes do 1º IDR), bytes de PLI e RR, tag SRTCP 4/10 (ida e volta e rejeição cruzada), estatísticas RFC 3550 (perda, fração, duplicados, wrap, LSR/DLSR), SRTCP no `CallManager` (PLI autêntico, PLI/FIR do celular dispara keyframe, adaptação de tag, RTCP forjado ignorado, RR de áudio), mensagem `{"type":"pli"}` do navegador, capability do preaccept A/B e `dec` do upgrade. O cliente TypeScript foi editado mas **não** compilado/testado aqui.

## Só uma chamada real valida

1. Se o celular reage ao PLI com IDR (log `PLI enviado ao celular` seguido de IDR) e se o relay entrega o RTCP dele.
2. A tag SRTCP real (logs `autenticado ... tag=` / `não autenticou`) e o bit de perfil.
3. Se RR/SR mudam a taxa do emissor (a hipótese principal) e quais (PT, FMT) o celular envia.
4. Se `SIGNALING_AB` melhora (ou piora) o modo/taxa de vídeo; se `SINGLE_RELAY` reduz jitter.
5. A janela de 40 ms: ajuste com `WACALLS_1A1_REORDER_MS` (20–80) conforme o jitter.

## O que medir (A/B: uma variável por vez, mesmo contato/rede, ≥2 min por chamada)

- **kbps e quadros/s do vídeo do contato** (hoje 26–30 kbps / 4–5 fps), no navegador (`getStats`/contagem de AUs no canal `h264`) e nos logs.
- **Jitter do áudio do contato** (hoje 63–78 ms).
- **Congelamentos:** quadros descartados por lacuna, nº de PLIs e tempo entre PLI e o IDR seguinte (`rx.Dropped`/`rx.Gaps` no pipeline), tempo até o 1º quadro.
- Regressões: áudio do atendente, vídeo do atendente para o contato, chamadas de grupo e SIP.

Ordem sugerida: (a) padrão (1+2) → (b) `+WACALLS_1A1_SRTCP=1` → (c) `+WACALLS_1A1_RR=1` → (d) `+WACALLS_1A1_SINGLE_RELAY=1` → (e) `+WACALLS_1A1_SIGNALING_AB=1`.
