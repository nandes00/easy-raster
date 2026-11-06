# easyraster

Platform for real-time vehicle tracking. Receives raw GPS data from trackers via UDP, processes and persists it, and delivers events to associated clients.

## Architecture

```
Tracker (UDP)
    ↓
bus-middleware       — receives UDP, publishes to SQS
    ↓
SQS (tracking-raw.fifo)
    ↓
bus-tracking        — consumes SQS, persists to MongoDB, publishes client events
    ↓
MongoDB             — devices, raw_messages, clients
    ↓
Kafka (tracking.client.events)
```

## Services

| Service | Description | Port |
|---|---|---|
| `bus-middleware` | UDP ingestion, publishes to SQS | `5023/udp` |
| `bus-tracking` | SQS consumer, persistence, Kafka publisher | — |
| `admin-api` | REST API for clients and devices management | `8080` |
| `kafka` | Client event output (local dev only — production uses Amazon MSK, see below) | `9094` (external) |
| `mongo` | Main database (local dev only — production uses Amazon DocumentDB, see below) | `27017` |
| `localstack` | AWS SQS emulation (local) | `4566` |

### Production infrastructure (MSK + DocumentDB)

`docker-compose.prod.yaml` does **not** run local `kafka`/`mongo` containers. `bus-tracking` and `admin-api` connect to:

- **Amazon MSK**, authenticating with `AWS_MSK_IAM` (SASL) using the ECS task's IAM role — no static credentials. Enabled via `KAFKA_IAM_AUTH=true`.
- **Amazon DocumentDB**, over TLS. The prod images (`Dockerfile.prod` / `Dockerfile`) bundle the AWS RDS CA bundle at `/etc/ssl/certs/global-bundle.pem`, referenced by `MONGO_URI` via `tlsCAFile`.

See `pending-steps.txt` for what's still needed on the infra side (IAM policy, VPC/security groups, MSK topic creation, DocumentDB endpoint/credentials).

## Supported Protocols

| Protocol | Message types |
|---|---|
| RST | `RST` |
| Suntech | `STT`, `ALT` |
| Virloc | `RUV`, `RTT`, `RSN`, `RVR`, `ACK`, ... |

## Running locally

**Requirements:** Docker and Docker Compose.

Uses `docker-compose.yaml`.

```bash
# Start all services (uses existing images)
make up

# Start all services, rebuilding images first
make up-build

# Follow logs
make logs svc=bus-middleware
make logs svc=bus-tracking

# Build a specific service (or all, if svc is omitted)
make build svc=bus-tracking

# Stop
make down

# Stop and remove volumes
make clean
```

## Running in production

Uses `docker-compose.prod.yaml`. Same commands as local, with the `-prod` suffix.

```bash
# Start all services (uses existing images)
make up-prod

# Start all services, rebuilding images first
make up-build-prod

# Follow logs
make logs-prod svc=bus-tracking

# Build a specific service (or all, if svc is omitted)
make build-prod svc=bus-tracking

# Stop
make down-prod

# Stop and remove volumes
make clean-prod
```

## Testing

Send a UDP packet to simulate a tracker. Locally, `bus-middleware` listens on container port `5023`, which is published to **host port `9000`** (see `ports` in `docker-compose.yaml`).

**Suntech STT:**
```bash
echo -n "STT;2200002772;FFFFFF;220;1.0.7;1;20260508;18:26:31;04314481;724;4;8648;35;-23.259083;-51.065322;25.19;225.53;20;1;00000001;00000010;1;3;5580;;038F8007;28.49;4.2;0;2387473;2524;0;776;5713;0000000000000000;204046989800583;0000000000000000" \
  | nc -u -w1 localhost 9000
```

**Suntech ALT:**
```bash
echo -n "ALT;2200002772;FFFFFF;220;1.0.7;1;20260508;18:30:02;04387082;724;4;8632;32;-23.256415;-51.058788;14.08;30.33;22;1;00000001;00000010;29;2;;;038F8007;28.35;4.2;0;2388675;3726;10;987;5717;0000000000000000;204046989800583;0000000000000000" \
  | nc -u -w1 localhost 9000
```

To confirm the packet made it through the pipeline:
```bash
make logs svc=bus-middleware   # should log "packet received" and "packet published to kafka" (SQS)
make logs svc=bus-tracking     # should log the consumed SQS message
```

**Virloc RUV:**
```bash
printf '>RUV0237,000,111,222,333,444,555;ID=0001;#167B;*33<\r\n' \
  | nc -u -w1 localhost 9000
```

## Environment variables

### bus-middleware

| Variable | Required | Default | Description |
|---|---|---|---|
| `SQS_QUEUE_URL` | Yes | — | SQS queue URL |
| `AWS_REGION` | Yes | — | AWS region |
| `AWS_ACCESS_KEY_ID` | Yes | — | AWS credentials |
| `AWS_SECRET_ACCESS_KEY` | Yes | — | AWS credentials |
| `AWS_ENDPOINT_URL` | No | — | Custom endpoint (LocalStack) |
| `UDP_ADDR` | No | `:5023` | UDP listen address |
| `FORWARD_ADDR` | No | — | Forward raw packet to another host |
| `LOG_LEVEL` | No | `info` | `debug` or `info` |

### bus-tracking

| Variable | Required | Default | Description |
|---|---|---|---|
| `SQS_QUEUE_URL` | Yes | — | SQS queue URL to consume |
| `AWS_REGION` | Yes | — | AWS region |
| `AWS_ACCESS_KEY_ID` | Yes | — | AWS credentials |
| `AWS_SECRET_ACCESS_KEY` | Yes | — | AWS credentials |
| `AWS_ENDPOINT_URL` | No | — | Custom endpoint (LocalStack) |
| `MONGO_URI` | Yes | — | MongoDB connection string. Dev: local replica set, no TLS. Prod: DocumentDB cluster endpoint with `tls=true&tlsCAFile=/etc/ssl/certs/global-bundle.pem&replicaSet=rs0&retryWrites=false` |
| `KAFKA_BROKER` | Yes | — | Kafka broker address. Dev: local container. Prod: MSK bootstrap brokers |
| `KAFKA_CLIENT_TOPIC` | No | `tracking.client.events` | Kafka output topic |
| `KAFKA_IAM_AUTH` | No | `false` | When `true`, authenticates to the broker via `AWS_MSK_IAM` using the ECS task's IAM role (required for MSK) |

### admin-api

Same `MONGO_URI`, `KAFKA_BROKER`, `KAFKA_CLIENT_TOPIC` and `KAFKA_IAM_AUTH` as `bus-tracking` above, plus `COGNITO_USER_POOL_ID` (required) and `AWS_REGION`.
