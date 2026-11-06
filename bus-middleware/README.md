# easyraster-middleware

Microserviço UDP para recepção de pacotes de rastreadores GPS/IoT. Escuta uma porta UDP continuamente, responde com ACK e persiste os dados recebidos em SQLite.

## Stack

- **Go** com framework [GoFr](https://gofr.dev)
- **SQLite** via `modernc.org/sqlite` (sem CGO)
- **Docker** + **Docker Compose** para ambiente local

## Estrutura

```
cmd/tracker/main.go          — entry point
internal/tracker/
  model.go                   — estrutura Packet
  repository.go              — persistência SQLite
  service.go                 — lógica de negócio e logging
  server.go                  — UDP listener + ACK
```

## Variáveis de ambiente

| Variável   | Padrão        | Descrição                        |
|------------|---------------|----------------------------------|
| `UDP_ADDR` | `:5023`       | Endereço e porta do listener UDP |
| `DB_PATH`  | `tracker.db`  | Caminho para o arquivo SQLite    |
| `APP_ENV`  | —             | Ambiente (`dev`, `prod`)         |

---

## Rodando com Docker (recomendado)

### Subir

```bash
docker compose up --build
```

### Subir em background

```bash
docker compose up --build -d
```

### Ver logs

```bash
docker compose logs -f tracker
```

### Reiniciar após alterar código

```bash
docker compose restart tracker
```

### Derrubar

```bash
docker compose down
```

### Derrubar e remover volumes

```bash
docker compose down -v
```

O banco SQLite fica em `./data/tracker.db` no host e sobrevive ao `down`.

---

## Rodando localmente (sem Docker)

### Pré-requisitos

- Go 1.25+

### Instalar dependências

```bash
go mod download
```

### Subir o serviço

```bash
go run ./cmd/tracker
```

Para customizar porta e banco:

```bash
UDP_ADDR=:9000 DB_PATH=./data/tracker.db go run ./cmd/tracker
```

---

## Testando o listener UDP

```bash
# envio simples
echo -n "hello tracker" | nc -u -w1 127.0.0.1 9000

# payload binário
printf '\x01\x02\x03\x04' | nc -u -w1 127.0.0.1 9000
```

Saída esperada nos logs:

```json
{"level":"INFO","msg":"packet received","remote_addr":"127.0.0.1:xxxxx","size":13}
{"level":"DEBUG","msg":"packet persisted","remote_addr":"127.0.0.1:xxxxx","size":13}
```

---

## Fluxo de um pacote

```
dispositivo → UDP :9000 → server.go → service.go → repository.go → tracker.db
                                    ↓
                                  ACK (0x01)
```