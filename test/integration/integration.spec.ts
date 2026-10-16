import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import { OutboxPublisherWorker } from "../../src/application/workers/outbox-publisher.worker";
import { PendingReferenceRetryWorker } from "../../src/application/workers/pending-reference-retry.worker";
import { SqsWagerConsumer } from "../../src/application/workers/sqs-wager.consumer";
import { WagerTransactionStatus } from "../../src/domain/entities/wager-transaction";
import { FailureCode } from "../../src/domain/errors/domain-errors";
import {
  InboxMessageRecord,
  OutboxMessageRecord,
  WagerTransactionRecord,
  WalletLedgerEntryRecord,
} from "../../src/infrastructure/database/entities/persistence-entities";
import {
  createAppInstance,
  ensureTestInfrastructure,
  resetDatabaseAndQueues,
  RunningAppInstance,
} from "../helpers/test-harness";

describe("Integration Tests — PostgreSQL, MikroORM, SQS, Inbox & Outbox", () => {
  let instance: RunningAppInstance;

  beforeAll(async () => {
    instance = await createAppInstance({
      instanceId: "integration-node-1",
      enableSqsPolling: false,
      enableOutboxWorker: false,
      enablePendingRefWorker: false,
    });
  });

  beforeEach(async () => {
    await resetDatabaseAndQueues(instance);
  });

  afterAll(async () => {
    if (instance) {
      await instance.close();
    }
    const infra = await ensureTestInfrastructure();
    await infra.stop();
  });

  describe("1. Database Schema, Migrations, Constraints & Triggers", () => {
    it("should support reversible migrations (down and up) and enforce DB-level constraints & triggers", async () => {
      const migrator = instance.orm.getMigrator();
      await migrator.down();
      await migrator.up();

      const conn = instance.orm.em.getConnection();
      const walletId = randomUUID();
      const playerId = randomUUID();

      // 1. Insert valid wallet
      await conn.execute(
        `INSERT INTO wallets (id, player_id, currency, balance_amount, version) VALUES (?, ?, 'BRL', 100.00, 1)`,
        [walletId, playerId],
      );

      // 2. Reject duplicate (player_id, currency) at DB schema level
      await expect(
        conn.execute(
          `INSERT INTO wallets (id, player_id, currency, balance_amount, version) VALUES (?, ?, 'BRL', 50.00, 1)`,
          [randomUUID(), playerId],
        ),
      ).rejects.toThrow();

      // 3. Reject negative balance at DB schema level (chk_wallets_balance_non_negative)
      await expect(
        conn.execute(`UPDATE wallets SET balance_amount = -0.01 WHERE id = ?`, [
          walletId,
        ]),
      ).rejects.toThrow();

      // 4. Insert a PROCESSED transaction and a valid ledger entry
      const txId = randomUUID();
      await conn.execute(
        `
        INSERT INTO wager_transactions (
          id, provider_id, external_transaction_id, idempotency_key, payload_hash,
          wallet_id, player_id, round_id, game_id, kind, money_amount, money_currency, status
        ) VALUES (?, 'prov-1', 'ext-1', 'prov-1:ext-1', ?, ?, ?, 'r-1', 'g-1', 'BET', 25.00, 'BRL', 'PROCESSED')
        `,
        [txId, "a".repeat(64), walletId, playerId],
      );

      const entryId = randomUUID();
      await conn.execute(
        `
        INSERT INTO wallet_ledger_entries (
          id, wallet_id, transaction_id, direction, money_amount, money_currency,
          balance_before_amount, balance_after_amount
        ) VALUES (?, ?, ?, 'DEBIT', 25.00, 'BRL', 100.00, 75.00)
        `,
        [entryId, walletId, txId],
      );

      // 5. Reject ledger UPDATE and DELETE via trigger trg_wallet_ledger_immutable
      await expect(
        conn.execute(
          `UPDATE wallet_ledger_entries SET money_amount = 10.00 WHERE id = ?`,
          [entryId],
        ),
      ).rejects.toThrow(/immutable/i);

      await expect(
        conn.execute(`DELETE FROM wallet_ledger_entries WHERE id = ?`, [
          entryId,
        ]),
      ).rejects.toThrow(/immutable/i);

      // 6. Reject terminal transaction status mutation and DELETE via trigger trg_guard_wager_transaction_terminal
      await expect(
        conn.execute(
          `UPDATE wager_transactions SET status = 'REJECTED' WHERE id = ?`,
          [txId],
        ),
      ).rejects.toThrow(/terminal/i);

      await expect(
        conn.execute(`DELETE FROM wager_transactions WHERE id = ?`, [txId]),
      ).rejects.toThrow(/forbidden/i);

      // 7. Reject partial unique index violation on duplicate PROCESSED reversal for same reference_transaction_id
      const rev1 = randomUUID();
      const rev2 = randomUUID();
      await conn.execute(
        `
        INSERT INTO wager_transactions (
          id, provider_id, external_transaction_id, idempotency_key, payload_hash,
          wallet_id, player_id, round_id, game_id, kind, money_amount, money_currency,
          reference_external_transaction_id, reference_transaction_id, status
        ) VALUES (?, 'prov-1', 'rev-1', 'prov-1:rev-1', ?, ?, ?, 'r-1', 'g-1', 'REFUND', 25.00, 'BRL', 'ext-1', ?, 'PROCESSED')
        `,
        [rev1, "b".repeat(64), walletId, playerId, txId],
      );

      await expect(
        conn.execute(
          `
          INSERT INTO wager_transactions (
            id, provider_id, external_transaction_id, idempotency_key, payload_hash,
            wallet_id, player_id, round_id, game_id, kind, money_amount, money_currency,
            reference_external_transaction_id, reference_transaction_id, status
          ) VALUES (?, 'prov-1', 'rev-2', 'prov-1:rev-2', ?, ?, ?, 'r-1', 'g-1', 'ROLLBACK', 25.00, 'BRL', 'ext-1', ?, 'PROCESSED')
          `,
          [rev2, "c".repeat(64), walletId, playerId, txId],
        ),
      ).rejects.toThrow();
    });
  });

  describe("2. HTTP API, Atomic Persistence (Wallet + Ledger + Outbox) & Reconciliation", () => {
    it("should create wallet with OPENING ledger entry, process BET/WIN/LOSS/REFUND, paginate ledger, and reconcile", async () => {
      const playerId = randomUUID();

      // 1. Create wallet with 1000.00 BRL
      const createRes = await fetch(`${instance.baseUrl}/wallets`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          playerId,
          initialBalance: { amount: "1000.00", currency: "BRL" },
        }),
      });
      expect(createRes.status).toBe(201);
      const wallet = (await createRes.json()) as {
        id: string;
        playerId: string;
        balance: { amount: string; currency: string };
        version: number;
      };
      expect(wallet.balance).toEqual({ amount: "1000.00", currency: "BRL" });
      expect(wallet.version).toBe(1);

      // Duplicate wallet for same playerId + currency -> 409 Conflict
      const dupWalletRes = await fetch(`${instance.baseUrl}/wallets`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          playerId,
          initialBalance: { amount: "500.00", currency: "BRL" },
        }),
      });
      expect(dupWalletRes.status).toBe(409);

      // 2. Submit BET of 25.00 BRL
      const betPayload = {
        providerId: "provider-a",
        externalTransactionId: "tx-bet-1",
        playerId,
        walletId: wallet.id,
        roundId: "round-100",
        gameId: "fortune-chimp",
        kind: "BET",
        money: { amount: "25.00", currency: "BRL" },
      };

      const betRes = await fetch(`${instance.baseUrl}/wagering/transactions`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": "provider-a:tx-bet-1",
        },
        body: JSON.stringify(betPayload),
      });
      expect(betRes.status).toBe(200);
      const betBody = (await betRes.json()) as any;
      expect(betBody.status).toBe("PROCESSED");
      expect(betBody.balance).toEqual({ amount: "975.00", currency: "BRL" });
      expect(betBody.idempotentReplay).toBe(false);

      // 3. Submit WIN of 150.00 BRL
      const winRes = await fetch(`${instance.baseUrl}/wagering/transactions`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": "provider-a:tx-win-1",
        },
        body: JSON.stringify({
          ...betPayload,
          externalTransactionId: "tx-win-1",
          kind: "WIN",
          money: { amount: "150.00", currency: "BRL" },
          referenceExternalTransactionId: "tx-bet-1",
        }),
      });
      expect(winRes.status).toBe(200);
      const winBody = (await winRes.json()) as any;
      expect(winBody.balance).toEqual({ amount: "1125.00", currency: "BRL" });

      // 4. Replay the earlier BET request -> must return original observed balance (975.00) and idempotentReplay: true
      const replayRes = await fetch(
        `${instance.baseUrl}/wagering/transactions`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Idempotency-Key": "provider-a:tx-bet-1",
          },
          body: JSON.stringify(betPayload),
        },
      );
      expect(replayRes.status).toBe(200);
      const replayBody = (await replayRes.json()) as any;
      expect(replayBody.idempotentReplay).toBe(true);
      expect(replayBody.transactionId).toBe(betBody.transactionId);
      expect(replayBody.balance).toEqual({ amount: "975.00", currency: "BRL" });

      // 5. Same Idempotency-Key with different payload -> 409 Conflict
      const conflictRes = await fetch(
        `${instance.baseUrl}/wagering/transactions`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Idempotency-Key": "provider-a:tx-bet-1",
          },
          body: JSON.stringify({
            ...betPayload,
            money: { amount: "99.00", currency: "BRL" },
          }),
        },
      );
      expect(conflictRes.status).toBe(409);

      // 6. Paginate Ledger with opaque cursor (OPENING + BET + WIN = 3 entries)
      const ledgerPage1Res = await fetch(
        `${instance.baseUrl}/wallets/${wallet.id}/ledger?limit=2`,
      );
      expect(ledgerPage1Res.status).toBe(200);
      const page1 = (await ledgerPage1Res.json()) as any;
      expect(page1.items.length).toBe(2);
      expect(page1.hasMore).toBe(true);
      expect(typeof page1.nextCursor).toBe("string");

      const ledgerPage2Res = await fetch(
        `${instance.baseUrl}/wallets/${wallet.id}/ledger?limit=2&cursor=${page1.nextCursor}`,
      );
      expect(ledgerPage2Res.status).toBe(200);
      const page2 = (await ledgerPage2Res.json()) as any;
      expect(page2.items.length).toBe(1);
      expect(page2.hasMore).toBe(false);
      expect(page2.nextCursor).toBeNull();

      // 7. Reconcile Wallet
      const recRes = await fetch(
        `${instance.baseUrl}/wallets/${wallet.id}/reconciliation`,
        { method: "POST" },
      );
      expect(recRes.status).toBe(200);
      const recBody = (await recRes.json()) as any;
      expect(recBody).toEqual({
        walletId: wallet.id,
        storedBalance: { amount: "1125.00", currency: "BRL" },
        calculatedBalance: { amount: "1125.00", currency: "BRL" },
        difference: { amount: "0.00", currency: "BRL" },
        consistent: true,
        checkedEntries: 3,
      });

      // 8. Verify Outbox messages were persisted atomically in the database
      const em = instance.orm.em.fork();
      const outboxMessages = await em.find(OutboxMessageRecord, {
        aggregateId: wallet.id,
      });
      // 2 from OPENING + 2 from BET + 2 from WIN = 6 outbox messages
      expect(outboxMessages.length).toBe(6);
    });
  });

  describe("3. SQS Consumer, Persistent Inbox Deduplication, Retries & DLQ", () => {
    it("should process SQS message, record InboxMessage, and deduplicate redeliveries without double debit", async () => {
      const createRes = await fetch(`${instance.baseUrl}/wallets`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          playerId: randomUUID(),
          initialBalance: { amount: "200.00", currency: "BRL" },
        }),
      });
      const wallet = (await createRes.json()) as any;

      const sqsEnvelope = {
        messageId: "msg-sqs-100",
        type: "WagerTransactionRequested",
        occurredAt: new Date().toISOString(),
        data: {
          providerId: "provider-sqs",
          externalTransactionId: "sqs-bet-100",
          idempotencyKey: "provider-sqs:sqs-bet-100",
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: "round-sqs-1",
          gameId: "fortune-chimp",
          kind: "BET",
          money: { amount: "50.00", currency: "BRL" },
        },
      };

      await instance.sqsClient.sendMessage({
        queueName: instance.sqsClient.wagerQueueName,
        body: JSON.stringify(sqsEnvelope),
        messageGroupId: wallet.id,
        messageDeduplicationId: "dedup-1",
      });

      const consumer = instance.app.get(SqsWagerConsumer);
      const received = await instance.sqsClient.receiveMessages(
        instance.sqsClient.wagerQueueName,
        1,
        0,
        5,
      );
      expect(received.length).toBe(1);

      const firstRun = await consumer.handleRawSqsMessage(received[0]);
      expect(firstRun?.status).toBe(WagerTransactionStatus.Processed);
      expect(firstRun?.balance.amount).toBe("150.00");
      expect(firstRun?.idempotentReplay).toBe(false);

      // Simulate SQS redelivery of the exact same messageId
      const secondRun = await consumer.handleRawSqsMessage({
        ...received[0],
        ReceiptHandle: undefined,
      });
      expect(secondRun?.idempotentReplay).toBe(true);
      expect(secondRun?.balance.amount).toBe("150.00");

      const em = instance.orm.em.fork();
      const inboxRec = await em.findOne(InboxMessageRecord, {
        consumerName: SqsWagerConsumer.CONSUMER_NAME,
        messageId: "msg-sqs-100",
      });
      expect(inboxRec).not.toBeNull();
      expect(inboxRec?.processedAt).not.toBeNull();

      const ledgerEntries = await em.find(WalletLedgerEntryRecord, {
        walletId: wallet.id,
      });
      // 1 OPENING + 1 BET = 2 entries
      expect(ledgerEntries.length).toBe(2);
    });

    it("should route malformed/permanent error messages to wager-transactions-dlq.fifo", async () => {
      await instance.sqsClient.sendMessage({
        queueName: instance.sqsClient.wagerQueueName,
        body: "{ invalid-json-payload",
        messageGroupId: "bad-group",
        messageDeduplicationId: "bad-dedup-1",
      });

      const consumer = instance.app.get(SqsWagerConsumer);
      const received = await instance.sqsClient.receiveMessages(
        instance.sqsClient.wagerQueueName,
        1,
        0,
        5,
      );
      expect(received.length).toBe(1);

      await consumer.handleRawSqsMessage(received[0]);

      const dlqMessages = await instance.sqsClient.receiveMessages(
        instance.sqsClient.wagerDlqName,
        10,
        0,
        5,
      );
      expect(dlqMessages.length).toBe(1);
      const dlqPayload = JSON.parse(dlqMessages[0].Body!);
      expect(dlqPayload.reason).toBe("INVALID_JSON_BODY");
    });
  });

  describe("4. Transactional Outbox Retry & Recovery After Restart", () => {
    it("should schedule backoff retry when Outbox publish fails and publish on recovery", async () => {
      const createRes = await fetch(`${instance.baseUrl}/wallets`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          playerId: randomUUID(),
          initialBalance: { amount: "100.00", currency: "BRL" },
        }),
      });
      expect(createRes.status).toBe(201);

      const outboxWorker = instance.app.get(OutboxPublisherWorker);

      // 1. Simulate broker outage during first outbox publish attempt
      const publishedOnFailure = await outboxWorker.processDueBatch(async () => {
        throw new Error("SQS temporarily unavailable");
      });
      expect(publishedOnFailure).toBe(0);

      const em = instance.orm.em.fork();
      const pendingAfterFailure = await em.find(OutboxMessageRecord, {
        publishedAt: null,
      });
      expect(pendingAfterFailure.length).toBe(2);
      expect(pendingAfterFailure[0].attempts).toBe(1);
      expect(pendingAfterFailure[0].nextAttemptAt).not.toBeNull();

      // Fast-forward next_attempt_at so retry is due immediately
      await em
        .getConnection()
        .execute(`UPDATE outbox_messages SET next_attempt_at = NOW() - INTERVAL '1 second'`);

      // 2. Broker recovers -> OutboxPublisherWorker publishes both events
      const publishedOnRecovery = await outboxWorker.processDueBatch();
      expect(publishedOnRecovery).toBe(2);

      const remainingPending = await em
        .fork()
        .find(OutboxMessageRecord, { publishedAt: null });
      expect(remainingPending.length).toBe(0);
    });

    it("should reject PENDING_REFERENCE transaction with REFERENCE_NOT_FOUND_TIMEOUT after exhausting retries", async () => {
      const createRes = await fetch(`${instance.baseUrl}/wallets`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          playerId: randomUUID(),
          initialBalance: { amount: "100.00", currency: "BRL" },
        }),
      });
      const wallet = (await createRes.json()) as any;

      // Submit a REFUND referencing a non-existent BET
      const refundRes = await fetch(
        `${instance.baseUrl}/wagering/transactions`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Idempotency-Key": "provider-a:orphan-refund-1",
          },
          body: JSON.stringify({
            providerId: "provider-a",
            externalTransactionId: "orphan-refund-1",
            playerId: wallet.playerId,
            walletId: wallet.id,
            roundId: "round-orphan",
            gameId: "fortune-chimp",
            kind: "REFUND",
            money: { amount: "25.00", currency: "BRL" },
            referenceExternalTransactionId: "never-arriving-bet",
          }),
        },
      );
      expect(refundRes.status).toBe(202);
      const refundBody = (await refundRes.json()) as any;
      expect(refundBody.status).toBe("PENDING_REFERENCE");

      const retryWorker = instance.app.get(PendingReferenceRetryWorker);

      // Run 3 retry cycles with maxAttemptsOverride = 3
      await retryWorker.processDuePendingReferences({
        forceDue: true,
        maxAttemptsOverride: 3,
      });
      await retryWorker.processDuePendingReferences({
        forceDue: true,
        maxAttemptsOverride: 3,
      });
      const resolvedCount = await retryWorker.processDuePendingReferences({
        forceDue: true,
        maxAttemptsOverride: 3,
      });
      expect(resolvedCount).toBe(1);

      const txRec = await instance.orm.em
        .fork()
        .findOne(WagerTransactionRecord, { id: refundBody.transactionId });
      expect(txRec?.status).toBe(WagerTransactionStatus.Rejected);
      expect(txRec?.failureCode).toBe(FailureCode.REFERENCE_NOT_FOUND_TIMEOUT);
    });
  });

  describe("5. Observability — Health Checks & Metrics", () => {
    it("should expose /health/live, /health/ready, and /metrics without authentication", async () => {
      const liveRes = await fetch(`${instance.baseUrl}/health/live`);
      expect(liveRes.status).toBe(200);

      const readyRes = await fetch(`${instance.baseUrl}/health/ready`);
      expect(readyRes.status).toBe(200);
      const readyBody = (await readyRes.json()) as any;
      expect(readyBody.status).toBe("ready");
      expect(readyBody.checks).toEqual({ postgres: "up", sqs: "up" });

      const metricsRes = await fetch(`${instance.baseUrl}/metrics`);
      expect(metricsRes.status).toBe(200);
      const promText = await metricsRes.text();
      expect(promText).toContain("wagering_transactions_total");
      expect(promText).toContain("wagering_processing_latency_ms");
    });
  });
});
