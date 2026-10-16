# Distributed Wagering Processor

Serviço financeiro distribuído para processamento de transações de apostas (*wagering*) de múltiplos provedores de jogos via **API HTTP** e mensageria assíncrona (**AWS SQS FIFO**), com garantias estritas de precisão monetária, controle de concorrência por carteira, idempotência persistente, *Transactional Inbox/Outbox* e reconciliação contábil via *Ledger* imutável.

> **Arquitetura, Decisões Técnicas e Trade-offs:** consulte o documento [`ARCHITECTURE.md`](./ARCHITECTURE.md).

---

## Stack Tecnológica

| Componente | Tecnologia |
|---|---|
| **Runtime / Package Manager / Test Runner** | **Bun 1.x** |
| **Linguagem** | **TypeScript 5** (`strict: true`) |
| **Framework** | **NestJS 10** |
| **Banco de Dados** | **PostgreSQL 16** |
| **ORM & Migrations** | **MikroORM 6** (`@mikro-orm/postgresql` + `@mikro-orm/migrations`) |
| **Mensageria** | **AWS SQS FIFO** (via **LocalStack**) |
| **Precisão Monetária** | **Decimal.js** (escala fixa de 2 casas decimais) |

---

## Setup e Execução Local

### 1. Instalar dependências
```bash
bun install
```

### 2. Configurar variáveis de ambiente
Copie o arquivo `.env.example` para `.env` (os valores padrão já estão prontos para o `docker-compose.yml` local):
```bash
cp .env.example .env
```

### 3. Subir a infraestrutura (PostgreSQL 16 + LocalStack SQS)
```bash
docker compose up -d
```
O script [`docker/localstack/init-aws.sh`](./docker/localstack/init-aws.sh) provisiona automaticamente as filas FIFO:
* `wager-transactions.fifo` (fila principal de entrada com `RedrivePolicy`)
* `wager-transactions-dlq.fifo` (*Dead-Letter Queue*)
* `wager-integration-events.fifo` (fila de saída para eventos publicados pela *Transactional Outbox*)

### 4. Executar as Migrations
```bash
# Aplicar migrations
bun run migration:up

# Reverter última migration
bun run migration:down
```

### 5. Iniciar a aplicação
```bash
# Produção / padrão
bun run start

# Desenvolvimento (watch mode)
bun run start:dev
```
Por padrão a API escuta em `http://localhost:3000`.

---

## Executando os Testes e Benchmark de Carga

A suíte de testes valida regras de domínio, constraints e triggers no PostgreSQL, atomicidade entre `Wallet` + `Ledger` + `Inbox` + `Outbox`, paralelismo real em múltiplas instâncias e recuperação após falhas:

```bash
# Testes de unidade (Money, Wallet, Ledger, WagerTransaction, Regras de Negócio, Canonical JSON)
bun run test:unit

# Testes de integração (Migrations reversíveis, Triggers, Atomicidade SQL, SQS Inbox, Outbox, DLQ, Health/Metrics)
bun run test:integration

# Testes de concorrência (50x mesma aposta em paralelo, disputa de saldo 2x80 em 100 BRL, >=3 instâncias, crash pós-commit antes do ack, out-of-order)
bun run test:concurrency

# Executar toda a suíte (Unidade + Integração + Concorrência)
bun run test:all

# Checagem estrita de tipos TypeScript
bun run typecheck

# Benchmark de carga e contenção multi-instância (relatório de RPS, p50/p95/p99 e reconciliação)
bun run test:load
```

---

## Endpoints da API HTTP

### 1. Criar Carteira (`POST /wallets`)
Quando `initialBalance > 0`, gera atomicamente uma transação interna `OPENING` (`PROCESSED`) e um lançamento `CREDIT` no ledger na mesma transação SQL.
```bash
curl -X POST http://localhost:3000/wallets \
  -H "Content-Type: application/json" \
  -d '{
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "initialBalance": { "amount": "1000.00", "currency": "BRL" }
  }'
```

### 2. Submeter Transação de Aposta (`POST /wagering/transactions`)
Suporta `BET`, `WIN`, `LOSS`, `REFUND` e `ROLLBACK`. Exige o header `Idempotency-Key`.
```bash
curl -X POST http://localhost:3000/wagering/transactions \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: provider-a:transaction-123" \
  -d '{
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }'
```

**Códigos de Resposta HTTP:**
* `200 OK`: Transação `PROCESSED` (inclui replays idempotentes com `idempotentReplay: true` e o saldo observado no momento da execução original).
* `202 Accepted`: Transação aceita em `PENDING_REFERENCE` (aguardando chegada da transação referenciada fora de ordem).
* `422 Unprocessable Entity`: Transação `REJECTED` por regra de negócio (ex.: `INSUFFICIENT_FUNDS`, `NEGATIVE_BALANCE_ON_REVERSAL`, `DUPLICATE_REVERSAL`, `CURRENCY_MISMATCH`).
* `409 Conflict`: Conflito de idempotência (`IDEMPOTENCY_PAYLOAD_CONFLICT`, `EXTERNAL_TX_CONFLICT`, `WALLET_ALREADY_EXISTS`).
* `400 Bad Request`: Contrato ou formato monetário inválido (`INVALID_MONEY`, `INVALID_PAYLOAD`, `IDEMPOTENCY_KEY_REQUIRED`).
* `404 Not Found`: Carteira ou transação não encontrada (`WALLET_NOT_FOUND`, `TRANSACTION_NOT_FOUND`).
* `503 Service Unavailable`: Falha transitória de infraestrutura (seguro reenviar com a mesma `Idempotency-Key`).

### 3. Consultas de Carteira, Ledger e Transações
```bash
# Consultar saldo e versão da carteira
curl http://localhost:3000/wallets/:walletId

# Consultar lançamentos do ledger com paginação por cursor opaco
curl "http://localhost:3000/wallets/:walletId/ledger?limit=50&cursor=..."

# Consultar transação pelo ID interno
curl http://localhost:3000/wagering/transactions/:transactionId

# Consultar transação por provedor e ID externo
curl http://localhost:3000/providers/:providerId/wagering/transactions/:externalTransactionId
```

### 4. Reconciliação Contábil (`POST /wallets/:walletId/reconciliation`)
Reconstrói o saldo da carteira a partir de todos os lançamentos imutáveis do ledger e compara com o saldo materializado em `wallets`:
```bash
curl -X POST http://localhost:3000/wallets/:walletId/reconciliation
```

### 5. Observabilidade: Health Checks e Métricas
```bash
# Liveness probe
curl http://localhost:3000/health/live

# Readiness probe (valida conectividade com PostgreSQL e SQS)
curl http://localhost:3000/health/ready

# Métricas em formato Prometheus
curl http://localhost:3000/metrics

# Snapshot de métricas em JSON
curl http://localhost:3000/metrics/json
```

---

## Estrutura do Projeto

```text
├── docker/
│   └── localstack/init-aws.sh       # Provisionamento automático das filas SQS FIFO e DLQ
├── scripts/
│   └── load-test.ts                 # Benchmark de carga, contenção em hot-wallet e reconciliação
├── src/
│   ├── domain/                      # Camada de Domínio pura (Money, Wallet, WagerTransaction, Ledger, Inbox, Outbox, Events)
│   ├── application/                 # Use Cases transacionais e Workers (SQS Consumer, Outbox Publisher, Pending Reference Retry)
│   ├── infrastructure/              # MikroORM Schemas/Mappers, Migrations SQL, SQS Client, Logger JSON e Métricas
│   └── presentation/                # Controllers REST, Auth Port/Guard e Global Exception Filter
├── test/
│   ├── unit/                        # Testes de unidade do modelo de domínio e regras de negócio
│   ├── integration/                 # Testes de integração (Postgres, Migrations, Triggers, SQS, Inbox/Outbox, DLQ)
│   ├── concurrency/                 # Testes de concorrência real e resiliência multi-instância
│   └── helpers/                     # Test harness multi-instância
├── ARCHITECTURE.md                  # Decisões arquiteturais, invariantes, máquina de estados, trade-offs e limitações
├── docker-compose.yml               # Orquestração local (PostgreSQL 16 + LocalStack)
└── package.json
```
