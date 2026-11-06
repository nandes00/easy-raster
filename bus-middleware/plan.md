# Plano: contra-fluxo (ACK / comandos) do serviço terceiro para o dispositivo

## Problema

Hoje o `bus-middleware` só encaminha em uma via: dispositivo → middleware → serviço
terceiro (`FORWARD_ADDR`, via `UDPForwarder`, `internal/tracker/forwarder.go`).

O serviço terceiro precisa mandar de volta:

1. **ACK** de um pacote que acabamos de encaminhar (round-trip curto, resposta a algo
   que partiu daqui).
2. **Comando** que um usuário disparou na plataforma do terceiro, em qualquer
   momento, sem relação direta com um pacote que passou pelo middleware agora.

Os dois precisam terminar no dispositivo certo, identificado por
`deviceIdentifier`/serial. O middleware não guarda hoje nenhum registro de "qual
socket/endereço pertence a qual dispositivo".

## Confirmado

- O terceiro **sempre** responde ACK e comando no mesmo socket UDP usado para o
  forward — sem distinção de canal entre os dois casos.
- A entrada de dados dos rastreadores é sempre um IP:porta fixo, hardcoded no
  dispositivo antes da instalação (não muda com a infra do middleware).
- O serviço vai rodar em ECS (AWS).
- O serviço terceiro é **transitório**: a intenção de médio prazo é o
  `bus-middleware` assumir ACK e geração de comando por conta própria, sem terceiro
  no meio.

## Decisão de infra (ECS)

Task única (`desiredCount=1`) com Elastic IP associado diretamente na ENI da task
(modo `awsvpc`), **sem NLB na frente do listener UDP**.

**Motivo:** o NAT da operadora do rastreador só aceita uma resposta cuja origem
aparente seja o mesmo IP:porta para onde ele mandou o pacote original. Com NLB +
múltiplas tasks, só a task que efetivamente recebeu o pacote consegue responder com
essa origem — o que reintroduziria "réplica dona da sessão" + canal de roteamento
entre réplicas (Redis + Kafka) só para conseguir escrever num socket. Como o volume
de tráfego (frota de ônibus) não justifica escalar horizontalmente o listener UDP,
EIP fixo numa task única entrega os ganhos que a ECS traz (restart automático,
deploy orquestrado, logs centralizados) sem herdar esse problema de roteamento
distribuído.

**Trade-off aceito:** alguns segundos de gap durante replace de task (reassociação
do EIP). Já existe hoje com instância única em `docker-compose.prod.yaml`, não é
regressão — o protocolo tolera via próximo heartbeat do dispositivo.

**Consequência direta: não precisa de Redis nem Kafka para este fluxo.** Tabela de
sessão em memória, numa única task, resolve tanto ACK quanto comando.

## Desenho unificado (sem separar Caso 1 / Caso 2 na implementação)

Como o terceiro sempre responde no mesmo socket, ACK e comando têm o mesmo caminho
técnico — a distinção entre os dois vira só uma questão de payload, não de
transporte:

1. Tabela de sessão em memória: `map[deviceIdentifier]*net.UDPAddr` (+ timestamp),
   TTL de expiração acima do intervalo de heartbeat do protocolo.
2. Populada em `Service.Handle` a cada pacote recebido do dispositivo.
3. Read loop no `UDPForwarder`, lendo do socket compartilhado de forward (hoje só
   usado para `Write`) — é um socket único para todos os dispositivos, não um por
   dispositivo.
4. Extrair `deviceIdentifier` do payload recebido (reaproveitar `parse.go`).
5. Consultar a sessão pelo `deviceIdentifier`; se existir, reenviar o payload via
   socket do listener (`WriteToUDP`) para o `net.UDPAddr` registrado. Se não existir
   (sessão expirada ou dispositivo nunca visto), logar e descartar.
6. **Keepalive de NAT:** goroutine com ticker mandando um pacote no-op no socket do
   forwarder (compartilhado, um só para todos os dispositivos) para manter viva a
   entrada do NAT Gateway da AWS enquanto não há tráfego de forward — comando pode
   chegar muito depois do último forward. Intervalo a confirmar contra o timeout de
   UDP do NAT Gateway na documentação da AWS antes de fixar (referência: timeout de
   UDP é bem mais curto que o de TCP; ~15-20s é margem segura para o que se conhece
   hoje, mas validar o valor atual antes de implementar).

## Arquitetura pensando na migração futura (fim do terceiro)

Separar a origem do payload do mecanismo de entrega. Definir uma função interna
única de escrita para o dispositivo:

```go
SendToDevice(deviceIdentifier string, payload []byte) error
```

Hoje quem chama essa função é o read loop do forwarder (passos 3-5 acima — payload
vem do terceiro). No dia em que o terceiro for desligado, ACK e comando passam a ser
gerados por lógica interna do próprio `bus-middleware`; troca-se só quem chama
`SendToDevice`, o mecanismo de sessão + write-back (passos 1, 2, 5) não muda.

O keepalive de NAT (passo 6) é código transitório: existe só enquanto há um
terceiro do outro lado do socket de forward, e desaparece junto com ele.

## Evolução futura (escala horizontal)

O desenho acima (task única + EIP) é a escolha certa **para agora** — hoje são ~40
veículos, e o volume não chega perto de justificar mais do que isso. Mas há
contratos previstos para rastreio de um estado inteiro (~100 mil veículos, a cada 5
min ≈ 333 pacotes/s em média). Nesse volume o problema deixa de ser throughput
(uma task só daria conta tranquilamente) e passa a ser **disponibilidade** — task
única vira ponto único de falha inaceitável para uma operação contratual desse
tamanho.

Caminho de evolução, quando o gatilho (contrato/volume) aparecer:

- NLB na frente de múltiplas tasks. NLB roteia UDP por hash determinístico da
  5-tupla, então em estado estável o mesmo dispositivo cai sempre na mesma task —
  isso já dá "réplica dona da sessão" sem precisar de Redis para decidir quem é
  dono, e a mesma task mantém seu próprio socket de forward pro terceiro, fechando
  o ciclo sem coordenação entre réplicas na maior parte do tempo.
- Redis entra só como rede de segurança para o caso de rebalanceamento (deploy,
  scale in/out muda o conjunto de tasks e o hash do NLB pode redirecionar um
  dispositivo pra uma task diferente da que tinha a sessão em memória). Para ACK
  isso se autocorrige no próximo pacote do dispositivo; para comando fora de banda
  em trânsito nesse instante, Redis com o último estado conhecido evita depender de
  esperar o próximo heartbeat.
- **Antes de comprometer esse desenho**: validar na prática o comportamento do NLB
  de reescrever a origem de uma resposta "solta" (não encadeada imediatamente ao
  pacote recebido — é o caso do comando, que chega pelo socket do forwarder minutos
  ou horas depois) — confirmar que ainda chega ao dispositivo com a origem que o
  NAT dele aceita. É a peça de que a decisão de arquitetura depende, e não é
  documentada com certeza suficiente para assumir sem teste.

A abstração `SendToDevice` e a tabela de sessão já desenhadas acima não são jogadas
fora nessa evolução — ganham uma camada de infra em cima (NLB + Redis) quando o
gatilho aparecer, mas o mecanismo central se mantém.

## Passos de implementação

1. [x] Tabela de sessão em memória (`deviceIdentifier → net.UDPAddr`, TTL) —
       `internal/tracker/session.go` (novo).
2. [x] Popular a sessão em `Service.Handle` a cada pacote recebido.
3. [x] Read loop no `UDPForwarder` para o socket compartilhado de forward —
       `UDPForwarder.Listen`.
4. [x] Extrair `deviceIdentifier` do payload de resposta (reuso de `parse.go`) —
       em `Service.HandleForwarderReply`.
5. [x] Função interna `SendToDevice(deviceIdentifier, payload)` — consulta sessão,
       escreve via socket do listener (`WriteToUDP`). Implementada como
       `Service.SendToDevice`, usando a interface `DeviceWriter` (`Server`
       implementa `WriteToDevice`); wiring via `Service.SetDeviceWriter` em
       `main.go`, já que o `Server` depende do `Service` na construção.
6. [x] Keepalive de NAT: ticker no socket do forwarder (`UDPForwarder.Keepalive`),
       intervalo default 15s, configurável via `NAT_KEEPALIVE_INTERVAL` — ainda
       falta validar esse valor contra o timeout real de UDP do NAT Gateway na AWS.
7. [x] Métricas/logs: pacotes de contra-fluxo sem sessão correspondente — log
       distinto via `ErrNoSession` em `Service.SendToDevice`/`HandleForwarderReply`
       ("forwarder reply dropped: no active session for device"), fácil de
       filtrar/alertar.
8. [ ] Infra: Elastic IP associado à ENI da task ECS (`awsvpc`), `desiredCount=1`,
       sem NLB no listener UDP. **Não implementável neste repositório** — não há
       Terraform/CDK/CloudFormation aqui; a infra parece ser provisionada em outro
       lugar. Fica pendente até localizar/criar esse código de infra.

## Fora de escopo

- Persistir sessões em disco/banco — é estado efêmero, reconstrói sozinho a partir
  do próximo pacote do dispositivo.
- Réplica múltipla / roteamento distribuído (NLB + Redis) para o listener UDP — ver
  [Evolução futura (escala horizontal)](#evolução-futura-escala-horizontal); não é
  necessário para o volume e criticidade de hoje.
- Negociar canal HTTP dedicado com o terceiro — não é uma opção; o terceiro é
  temporário e não há espaço para pedir mudança de protocolo do lado deles.
