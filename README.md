# DistributedMQ

A production-inspired distributed message queue built with Java and Spring Boot, using Redis Streams as the message backbone. Designed to demonstrate core concepts behind systems like Amazon SQS — including topic-based pub/sub, at-least-once delivery, consumer group isolation, PEL-based redelivery, and dead letter queue handling.

---

## What it does

When an order event is published, three independent consumer groups (`email-group`, `inventory-group`, `payment-group`) each receive and process the same message separately. Each group tracks its own acknowledgement state — if one consumer fails, only that group's copy enters the Pending Entry List (PEL). The others are unaffected.

Messages that remain unacknowledged past the visibility timeout are detected by the `RedeliveryScheduler` and claimed for retry via `XCLAIM`. If a message exceeds the configured maximum delivery attempts, it is moved to a Dead Letter Queue (DLQ) — a separate Redis Stream — where it can be inspected and replayed via REST endpoints.

---

## Architecture

```
POST /api/messages/publish
          │
          ▼
  Redis Stream: order.placed
          │
  ┌───────┼───────────┐
  ▼       ▼           ▼
email   inventory   payment
group    group       group
  │       │           │
  ▼       ▼           ▼
 ACK     ACK      [FAIL if amount > 500]
                       │
              message NOT acknowledged
                       │
              stays in PEL (payment-group)
                       │
              RedeliveryScheduler (every 5s)
              checks idle time vs visibility timeout
                       │
              ┌────────┴──────────────┐
              ▼                       ▼
    idle < visibility timeout   idle >= visibility timeout
    (still being processed)           │
                               delivery count checked
                               ┌──────┴──────────┐
                               ▼                  ▼
                         count < limit      count >= limit
                         XCLAIM to          move to DLQ stream
                         RecoveryConsumer   (order.placed.dlq)
                               │            + ACK original PEL entry
                         routes back to
                         PaymentConsumer
                               │
                          GET  /api/dlq
                          POST /api/dlq/{dlqMessageId}/replay
```

---

## Two Scheduled Components

This project uses two distinct scheduled components. They serve different purposes and should not be confused:

### ConsumerRunner — message polling (`fixedDelay = 1000ms`)

`ConsumerRunner` polls the `order.placed` stream every second on behalf of all three consumer groups. It calls `XREADGROUP` using `ReadOffset.lastConsumed()` to fetch new, undelivered messages and dispatches them to the appropriate consumer (`EmailConsumer`, `InventoryConsumer`, `PaymentConsumer`).

This is the normal consumption path — it runs continuously regardless of failures.

### RedeliveryScheduler — PEL monitoring and recovery (`fixedDelay = 5000ms`)

`RedeliveryScheduler` runs every 5 seconds and inspects the Pending Entry List (PEL) for each consumer group using `XPENDING`. For every pending message it evaluates:

- **Idle time < visibility timeout** → still considered in-progress, logged but left alone
- **Idle time >= visibility timeout AND delivery count < max attempts** → claimed via `XCLAIM` and forwarded to `RecoveryConsumer` for retry
- **Idle time >= visibility timeout AND delivery count >= max attempts** → moved to the DLQ stream and acknowledged in the original stream to clear the PEL entry

---

## Intentional Failure Simulation

The `PaymentConsumer` is deliberately designed to **throw a `RuntimeException`** when `amount > 500.0`. This is not a bug — it is an intentional failure trigger to showcase the full PEL → redelivery → DLQ pipeline.

**Step-by-step flow for a failing message:**

1. `ConsumerRunner` delivers the message to `PaymentConsumer` via `XREADGROUP`
2. `PaymentConsumer.process()` throws `RuntimeException("Simulated Payment failure - Order rejected")`
3. `ConsumerBase.onMessage()` catches the exception and does **not** call `XACK`
4. The message remains in the **PEL** for `payment-group` — unacknowledged
5. `RedeliveryScheduler` runs every 5 seconds and calls `XPENDING` for `payment-group`
6. Once the message's idle time exceeds `mq.visibility-timeout-seconds` (default: 30s), it becomes eligible for recovery
7. If `totalDeliveryCount < mq.max-delivery-attempts` (default: 5), the scheduler calls `XCLAIM` to transfer ownership to `recovery-consumer` and forwards it to `RecoveryConsumer`
8. `RecoveryConsumer` looks up the correct consumer group handler and calls `PaymentConsumer.onMessage()` again
9. The message fails again → stays in PEL → `RedeliveryScheduler` picks it up again on the next cycle
10. Once `totalDeliveryCount >= mq.max-delivery-attempts`, the scheduler fetches the original message payload from the stream using `XRANGE`, publishes it to `order.placed.dlq` with DLQ metadata, then calls `XACK` on the original stream to clear it from the PEL
11. The message is now available in the DLQ for inspection and replay

`email-group` and `inventory-group` each process and acknowledge the same message independently — their PELs are unaffected by the payment group's failure.

> To trigger the failure: publish a message with `"amount": 600` and watch the full retry + DLQ flow in the logs.

---

## Key Concepts Demonstrated

| Concept | Implementation |
|---|---|
| Topic-based pub/sub | Single `order.placed` stream, 3 independent consumer groups |
| At-least-once delivery | Unacknowledged messages stay in PEL until explicitly `XACK`-ed |
| Pending Entry List (PEL) | Redis tracks unacknowledged messages per consumer group |
| Visibility timeout | Configurable idle threshold (`mq.visibility-timeout-seconds`) — a message idle beyond this is eligible for recovery |
| XCLAIM recovery | `RedeliveryScheduler` transfers ownership of stuck messages to `recovery-consumer` |
| Delivery attempt limit | Configurable (`mq.max-delivery-attempts`) — when `totalDeliveryCount` reaches this, message moves to DLQ |
| Dead Letter Queue | DLQ is a separate Redis Stream (`order.placed.dlq`) — not a database table |
| DLQ inspection | `GET /api/dlq` reads DLQ entries without removing them |
| DLQ replay | `POST /api/dlq/{dlqMessageId}/replay` publishes a new message to `order.placed` |
| Idempotency | PostgreSQL tracks processed `(messageId, consumerGroup)` pairs — same Redis message can have separate idempotency records per group |
| Consumer isolation | Each group manages its own PEL; one group's failure does not affect others |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 17 |
| Framework | Spring Boot 3 |
| Message store | Redis Streams (XADD, XREADGROUP, XACK, XPENDING, XCLAIM, XRANGE) |
| DLQ store | Redis Streams (`order.placed.dlq`) |
| Idempotency store | PostgreSQL + Spring Data JPA (`idempotent_records` table) |
| Containerisation | Docker + Docker Compose |
| Build tool | Maven |

---

## Getting Started

### Prerequisites
- Docker and Docker Compose installed
- Java 17+

### Run with Docker Compose

```bash
# Clone the repo
git clone https://github.com/FelicxWayne/DistributedMQ.git
cd DistributedMQ

# Copy environment file and fill in your values
cp .env.example .env

# Start Redis and PostgreSQL
docker compose up -d
```

Then start the Spring Boot application:
```bash
./mvnw spring-boot:run
```

---

## API Reference

### Publish a message

```bash
POST /api/messages/publish
Content-Type: application/json

{
  "orderId": "ORD-001",
  "userId":  "USR-123",
  "amount":  499.0
}
```

Response:
```
Message published to topic: order.placed | orderId: ORD-001
```

> Use `amount > 500` to trigger the intentional payment failure and observe the PEL → visibility timeout → XCLAIM → DLQ flow.

---

### Inspect DLQ

```bash
GET /api/dlq
GET /api/dlq?limit=50
```

Each entry is a Redis Stream message in `order.placed.dlq`. The `messageId` is the DLQ stream's own Redis-assigned ID — distinct from `originalMessageId`, which refers to the message in `order.placed` that failed.

The `payload` contains the original business fields (`orderId`, `userId`, `amount`) plus DLQ metadata added at the time of failure:

```json
[
  {
    "messageId": "1782999123456-0",
    "payload": {
      "orderId": "ORD-001",
      "userId": "USR-123",
      "amount": "600.0",
      "originalMessageId": "1782911068736-0",
      "originalConsumerGroup": "payment-group",
      "deliveryCount": "5",
      "movedAt": "2025-08-29T10:30:00Z"
    }
  }
]
```

**Field descriptions:**

| Field | Description |
|---|---|
| `messageId` | Redis Stream ID of the DLQ entry in `order.placed.dlq` |
| `orderId`, `userId`, `amount` | Original business payload fields |
| `originalMessageId` | Redis Stream ID of the failed message in `order.placed` |
| `originalConsumerGroup` | The consumer group whose retry limit was exceeded |
| `deliveryCount` | Total delivery attempts before moving to DLQ |
| `movedAt` | ISO-8601 timestamp when the message was moved to the DLQ |

> `limit` must be between 1 and 1000. The default is 100.

---

### Replay a DLQ message

```bash
POST /api/dlq/{dlqMessageId}/replay
```

`dlqMessageId` is the `messageId` from `GET /api/dlq` — the DLQ stream's own Redis ID, not `originalMessageId`.

The replay operation:
1. Fetches the DLQ entry by `dlqMessageId` from `order.placed.dlq`
2. Strips DLQ metadata fields (`originalMessageId`, `originalConsumerGroup`, `deliveryCount`, `movedAt`)
3. Publishes only the original business payload (`orderId`, `userId`, `amount`) as a **new message** to `order.placed` with a new Redis-assigned stream ID
4. All three consumer groups will receive this new message

**The original DLQ entry is not deleted.** It remains in `order.placed.dlq` for audit and history purposes.

Response:
```json
{
  "status": "SUCCESS",
  "message": "Message was successfully replayed.",
  "originalMessageId": "1782999123456-0",
  "newRecordId": "1783000000000-0"
}
```

---

## Configuration

`application.properties`:

```properties
# Seconds a message must be idle in the PEL before RedeliveryScheduler
# considers it eligible for XCLAIM recovery
mq.visibility-timeout-seconds=30

# Maximum total delivery attempts (tracked by Redis via XPENDING totalDeliveryCount)
# Once this limit is reached, the message is moved to the DLQ
mq.max-delivery-attempts=5
```

---

## Idempotency

At-least-once delivery means the same message can be delivered more than once — particularly during redelivery. To prevent duplicate business logic execution, `ConsumerBase` checks PostgreSQL before processing every message.

Idempotency is tracked per **(messageId, consumerGroup)** pair in the `idempotent_records` table. This means the same Redis Stream message ID can have up to three separate idempotency records — one for each consumer group — which is correct behaviour. `email-group` marking a message as processed does not prevent `payment-group` from processing it.

PostgreSQL stores only these idempotency records. The messages themselves and the DLQ entries are stored exclusively in Redis Streams.

---

## Observing the Failure Recovery Flow

1. Start the app and publish a message with `amount: 600`
2. Watch the logs — `PaymentConsumer` will throw a simulated failure immediately
3. Every 5 seconds, `RedeliveryScheduler` logs the PEL state for each group:

```
==============================
Consumer Group : payment-group
Message ID     : 1782911068736-0
Consumer       : payment-runner-1
Idle Time      : 32 sec
Deliveries     : 2
Status         : Eligible for redelivery (idle time 32s >= visibility timeout 30s)
==============================
```

4. `RedeliveryScheduler` claims the message via `XCLAIM` and routes it through `RecoveryConsumer` back to `PaymentConsumer` — which fails again
5. After `totalDeliveryCount` reaches `mq.max-delivery-attempts` (5), the log shows:

```
Message ID: 1782911068736-0 in group payment-group has exceeded retry limit
(delivery count: 5 >= max: 5). Moving to DLQ.
Successfully moved message ID: 1782911068736-0 to DLQ with DLQ Record ID: 1782999123456-0
```

6. Inspect the DLQ: `GET /api/dlq`
7. Replay the message: `POST /api/dlq/1782999123456-0/replay`
8. All three consumer groups receive the replayed message as a fresh delivery with a new stream ID

---

## Project Structure

```
src/main/java/com/DBMQ/DistributedMQ/
├── config/
│   └── RedisConfig.java                  # RedisTemplate with String serializers for all key/hash/value fields
├── constants/
│   └── StreamConstants.java              # Stream key (order.placed), group names, DLQ key (order.placed.dlq)
├── consumer/
│   ├── ConsumerBase.java                 # Abstract base — idempotency check, deserialization, ack, error handling
│   ├── ConsumerRunner.java               # Polls all 3 groups via XREADGROUP every 1 second
│   ├── EmailConsumer.java                # Simulates email notification — always succeeds
│   ├── InventoryConsumer.java            # Simulates inventory update — always succeeds
│   ├── PaymentConsumer.java              # Intentionally fails when amount > 500 (failure simulation)
│   └── RecoveryConsumer.java             # Routes XCLAIM-ed messages back to the correct group's consumer
├── controller/
│   └── DlqController.java                # GET /api/dlq, POST /api/dlq/{dlqMessageId}/replay
├── initializer/
│   └── ConsumerGroupInitializer.java     # Creates consumer groups at startup via XGROUP CREATE (BUSYGROUP-safe)
├── model/
│   ├── OrderEvent.java                   # Business message model
│   └── IdempotentRecord.java             # JPA entity: messageId + consumerGroup + processedAt
├── producer/
│   └── MessageProducerController.java    # POST /api/messages/publish → XADD to order.placed
├── repository/
│   └── IdempotentRecordRepository.java   # existsByMessageIdAndConsumerGroup query
├── scheduler/
│   └── RedeliveryScheduler.java          # XPENDING scan → visibility timeout check → XCLAIM or DLQ (every 5s)
└── service/
    ├── AckService.java                   # Wraps XACK
    ├── ClaimService.java                 # Wraps XCLAIM
    ├── DlqInspectionService.java         # XRANGE reads from order.placed.dlq (non-destructive)
    ├── DlqReplayService.java             # Fetches DLQ entry, strips metadata, XADD to order.placed
    ├── DlqService.java                   # XADD to order.placed.dlq with metadata fields
    ├── IdempotencyService.java           # isMessageProcessed + markMessageAsProcessed (per messageId + group)
    └── PendingMessageService.java        # XPENDING and XRANGE queries
```

---

## What makes this different from a basic queue

Most queue implementations stop at "publish a message, consume it, done." This project goes further:

- **Consumer group isolation** — each group manages its own PEL independently. A failing payment service doesn't block email or inventory processing.
- **Visibility timeout + XCLAIM** — unacknowledged messages are not silently dropped or redelivered by polling alone. The scheduler explicitly transfers ownership via `XCLAIM` after the idle threshold, mirroring how SQS visibility timeouts work.
- **Delivery attempt tracking** — Redis natively tracks `totalDeliveryCount` per message per group via `XPENDING`. No external counter is needed.
- **Idempotency per consumer group** — the same Redis message can be safely redelivered to multiple groups without cross-group interference. Each `(messageId, consumerGroup)` pair is tracked independently.
- **Operational DLQ** — failed messages are stored in a separate Redis Stream with full metadata. They can be inspected without destructive reads and replayed as new messages after a fix. The DLQ entry is preserved for audit history.
- **Configurable behaviour** — visibility timeout and max delivery attempts are externalised to `application.properties`, not hardcoded.

---

## Inspiration

This project draws architectural inspiration from **Amazon SQS** — specifically its consumer visibility timeout, dead letter queue, and at-least-once delivery model — implemented from scratch using Redis Streams as the underlying primitive.

---

## Author

**Amala Nirmal Felicx A**
[github.com/FelicxWayne](https://github.com/FelicxWayne) · [linkedin.com/in/amala-nirmal-felicx-b1790a264](https://linkedin.com/in/amala-nirmal-felicx-b1790a264)
