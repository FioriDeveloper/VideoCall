# Qualidade do vídeo 1:1 (contato → atendente)

Branch `qualidade-video-1a1`. Objetivo: mais fluidez/qualidade do vídeo que o **celular do
contato** manda numa chamada 1:1 (navegador ⇄ servidor ⇄ relay ⇄ celular, só relay).

**Medido em produção (ponto de partida):** vídeo do contato a 26–30 kbps e 4–5 quadros/s,
congelando; áudio do contato em rajadas (jitter 63–78 ms). A config da chamada traz
`cond_low_quality_vid_mode` (15–25 kbps) e `bwe_impl 8` (estimador por atraso).

## Diagnóstico confirmado no código

| Achado | Onde |
|---|---|
| RTCP recebido (PT 72/73/78 no `data[1]&0x7f`) caía fora em silêncio; o 1:1 não mandava RTCP | `callmanager_media.go` `onRelayData` |
| Vídeo recebido sem filtro de SSRC, sem reordenação (datachannel `Ordered:false`), sem descarte de duplicados (2 relays abertos), `depack`/`srtp` fora de lock, FU-A fora de ordem fechava NAL corrompido, sem PLI | `video/pipeline.go` (antigo `HandleRelayData`) |
| `Unprotect` SRTP: ROC subia **duas vezes** se o 65535 chegasse depois do 0 → resto do stream decifrava lixo | `media/srtp.go` `updateRoc` |
| `Broadcast` manda toda a mídia nos 2 relays | `transport/sctprelay.go` |

## O que mudou

**1. PLI no 1:1 — padrão LIGADO (`WACALLS_1A1_PLI`)**
PLI (PSFB FMT 1) ao SSRC de vídeo do peer: no 1º pacote e a cada 1 s até o 1º IDR, e a cada
lacuna (mín. 300 ms). Navegador pode pedir com texto `{"type":"pli"}` no canal `h264`
(o `OnMessage` agora separa texto de binário; antes texto iria como "quadro"). O servidor
avisa o navegador com `{"type":"keyframe_request"}` (só quando RTCP do peer autentica).
O formato binário do canal (1 AU Annex B por mensagem) não mudou.

**2. Recepção robusta — padrão LIGADO (`WACALLS_1A1_RX`, janela `WACALLS_1A1_REORDER_MS`=40, 5–200)**
Filtro por SSRC do peer (derivado do slot 2; se o observado for outro, trava no observado e
loga), descarte de duplicados/atrasados, reordenação, lacuna → descarta o quadro corrente e
tudo até o próximo IDR + PLI. Fail-open: se 3 s sem IDR após perda, volta a entregar.
Sem mudança no grupo (usa o caminho antigo, agora com lock).
Correção associada em `srtp.go` (ROC, RFC 3711 §3.3.1) — necessária porque o unprotect
acontece antes da reordenação; vale também para áudio/grupo.

**3. SRTCP 1:1 — padrão DESLIGADO (`WACALLS_1A1_SRTCP`)**
SR+SDES a cada 1,5 s do nosso vídeo e leitura/autenticação do RTCP do peer, com PLI/FIR →
`OnKeyframeRequest` → IDR na câmera do navegador. Chaves: `DeriveGroupSrtcpKeys(chaveDaChamada, deviceJid)`.
**Tag:** o tamanho no 1:1 é desconhecido; `WACALLS_1A1_SRTCP_TAG` = `10`, `4` ou `both`
(padrão: emite os dois, recebe tentando os dois). O log "1º RTCP do peer autenticado — … tag_bytes"
é a validação. O envio do PLI (item 1) usa os mesmos emissores mesmo com SRTCP desligado.

**4. Receiver Report — padrão DESLIGADO (`WACALLS_1A1_RR`)**
RR (PT 201) com perda/fração/jitter/maior seq para o SSRC de vídeo do peer (RFC 3550 A.3),
junto do laço de 1,5 s.
Pesquisa (meowcaller + web): o meowcaller mantém estatísticas de recepção por SSRC
(`rtcp_reception_set.go`) mas **não encontrei** documentação de transport-cc (PT 205 FMT 15),
REMB ou mensagem própria de banda do WhatsApp. **Fato:** a extensão 0xDEBE traz
TransportSequence (id 9) e InitialBandwidth (id 5). **Suposição (não implementada):** que o
emissor do celular use feedback de atraso (`bwe_impl 8`). Sem base, não implementei
transport-cc/REMB; o RR é só a hipótese mais barata de "algum retorno".

**5. A/B — padrão DESLIGADO**
- `WACALLS_1A1_SIGNALING_AB=1`: accept de vídeo com `enc="h.264" dec="H264" screen_* device_orientation`
  + capability de vídeo; preaccept com capability de vídeo (**suposição:** blob `01 05 f7 09 e0 fa 07`);
  aceite de upgrade com `dec="H264"` (sem AV1).
- `WACALLS_1A1_ACTIVE_RELAY=1`: `Broadcast` só no relay aberto que recebeu dados por último.

## Variáveis

| Variável | Padrão |
|---|---|
| `WACALLS_1A1_RX` | on |
| `WACALLS_1A1_PLI` | on |
| `WACALLS_1A1_REORDER_MS` | 40 |
| `WACALLS_1A1_SRTCP` | off |
| `WACALLS_1A1_SRTCP_TAG` | both (`10`/`4`) |
| `WACALLS_1A1_RR` | off |
| `WACALLS_1A1_SIGNALING_AB` | off |
| `WACALLS_1A1_ACTIVE_RELAY` | off |

## Testado

`go build ./...` e `go test ./...` (cgo + opus_mlow compilam aqui); `go test -race` no pacote
`video`. Novos: bytes de RR e PLI, estatísticas/ wrap, SRTCP com tag 4, ROC com reordenação,
reordenação/duplicados/lacuna/wrap/teto, pipeline (ordem, duplicados, SSRC alheio, lacuna →
descarte até IDR + PLI decifrado byte a byte, caminho antigo, PLI do peer autenticado),
sinalização A/B e escolha do relay ativo. Cliente TS/`widget.js`: só `node --check` no widget;
`tsc` não rodou (sem `node_modules`).

## Só uma chamada real valida

- O celular **aceita** nosso PLI (e com qual tag) e responde com IDR? (log "PLI enviado" +
  quadros entregues depois). Se o relay não encaminha RTCP no 1:1, nada muda.
- Chave/tag SRTCP: ligar `WACALLS_1A1_SRTCP=1` e procurar "1º RTCP do peer autenticado".
- Se o SSRC derivado (slot 2) é o do vídeo do peer (aviso "SSRC do peer diferente do derivado").
- Se a reordenação/IDR gating melhora ou piora a fluidez (janela de 40 ms adiciona latência).
- Subir o cliente antes de ligar `SRTCP` (servidor passa a mandar texto no canal `h264`).

**Medir (A/B por chamada, mesmo contato/rede):** kbps e quadros/s do vídeo do contato
(`RxStats`: pacotes, duplicados, perdidos, quadros entregues/descartados, PLIs), jitter do áudio
do contato, nº e duração de congelamentos, tempo até o 1º quadro.
