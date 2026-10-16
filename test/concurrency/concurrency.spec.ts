import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import { OutboxPublisherWorker } from "../../src/application/workers/outbox-publisher.worker";
import { PendingReferenceRetryWorker } from "../../src/application/workers/pending-reference-retry.worker";
import { SqsWagerConsumer } from "../../src/application/workers/sqs-wager.consumer";
import { WagerTransactionStatus } from "../../src/domain/entities/wager-transaction";
import { LedgerDirection } from "../../src/domain/entities/wallet-ledger-entry";
import { FailureCode } from "../../src/domain/errors/domain-errors";
import {
  OutboxMessageRecord,
  WalletLedgerEntryRecord,
} from "../../src/infrastructure/database/entities/persistence-entities";
import {
  createAppInstance,
  ensureTestInfrastructure,
  resetDatabaseAndQueues,
  RunningAppInstance,
} from "../helpers/test-harness";

describe("Concurrency & Resilience Tests — Real Parallelism & Multi-Instance", () => {
  let instance1: RunningAppInstance;
  let instance2: RunningAppInstance;
  let instance3: RunningAppInstance;

  beforeAll(async () => {
    instance1 = await createAppInstance({
      instanceId: "node-1",
      enableSqsPolling: false,
      enableOutboxWorker: false,
      enablePendingRefWorker: false,
    });
    instance2 = await createAppInstance({
      instanceId: "node-2",
      enableSqsPolling: false,
      enableOutboxWorker: false,
      enablePendingRefWorker: false,
    });
    instance3 = await createAppInstance({
      instanceId: "node-3",
      enableSqsPolling: false,
      enableOutboxWorker: false,
      enablePendingRefWorker: false,
    });
  });

  beforeEach(async () => {
    await resetDatabaseAndQueues(instance1);
  });

  afterAll(async () => {
    await Promise.all([
      instance1?.close(),
      instance2?.close(),
      instance3?.close(),
    ]);
    const infra = await ensureTestInfrastructure();
    await infra.stop();
  });

  async function assertWalletLedgerInvariant(walletId: string, expectedBalance: string) {
    const recRes = await fetch(
      `${instance1.baseUrl}/wallets/${walletId}/reconciliation`,
      { method: "POST" },
    );
    expect(recRes.status).toBe(200);
    const rec = (await recRes.json()) as {
      storedBalance: { amount: string; currency: string };
      calculatedBalance: { amount: string; currency: string };
      difference: { amount: string; currency: string };
      consistent: boolean;
    };
    expect(rec.consistent).toBe(true);
    expect(rec.difference.amount).toBe("0.00");
    expect(rec.storedBalance.amount).toBe(expectedBalance);
    expect(rec.calculatedBalance.amount).toBe(expectedBalance);
  }

  it("1. Same bet sent 50 times in parallel -> exactly one debit and 49 idempotent replays", async () => {
    const createRes = await fetch(`${instance1.baseUrl}/wallets`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        playerId: randomUUID(),
        initialBalance: { amount: "500.00", currency: "BRL" },
      }),
    });
    const wallet = (await createRes.json()) as any;

    const payload = {
      providerId: "provider-50x",
      externalTransactionId: "bet-50x-same",
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: "round-50x",
      gameId: "fortune-chimp",
      kind: "BET",
      money: { amount: "25.00", currency: "BRL" },
    };

    const baseUrls = [instance1.baseUrl, instance2.baseUrl, instance3.baseUrl];

    // Dispatch 50 parallel HTTP requests distributed across the 3 instances
    const responses = await Promise.all(
      Array.from({ length: 50 }, (_, idx) => {
        const targetUrl = baseUrls[idx % baseUrls.length];
        return fetch(`${targetUrl}/wagering/transactions`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Idempotency-Key": "provider-50x:bet-50x-same",
          },
          body: JSON.stringify(payload),
        }).then(async (r) => ({
          status: r.status,
          body: (await r.json()) as any,
        }));
      }),
    );

    expect(responses.every((r) => r.status === 200)).toBe(true);
    expect(
      responses.every((r) => r.body.status === WagerTransactionStatus.Processed),
    ).toBe(true);
    expect(
      responses.every((r) => r.body.balance.amount === "475.00"),
    ).toBe(true);

    const freshExecutions = responses.filter((r) => r.body.idempotentReplay === false);
    const replays = responses.filter((r) => r.body.idempotentReplay === true);
    expect(freshExecutions.length).toBe(1);
    expect(replays.length).toBe(49);

    const em = instance1.orm.em.fork();
    const debitEntries = await em.find(WalletLedgerEntryRecord, {
      walletId: wallet.id,
      direction: LedgerDirection.Debit,
    });
    expect(debitEntries.length).toBe(1);

    await assertWalletLedgerInvariant(wallet.id, "475.00");
  });

  it("should process two simultaneous 80.00 BRL bets against 100.00 BRL balance with 1 PROCESSED and 1 REJECTED", async () => {
    const createRes = await fetch(`${instance1.baseUrl}/wallets`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        playerId: randomUUID(),
        initialBalance: { amount: "100.00", currency: "BRL" },
      }),
    });
    const wallet = (await createRes.json()) as any;

    const bet1Payload = {
      providerId: "provider-race",
      externalTransactionId: "bet-80-a",
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: "round-race-1",
      gameId: "fortune-chimp",
      kind: "BET",
      money: { amount: "80.00", currency: "BRL" },
    };

    const bet2Payload = {
      ...bet1Payload,
      externalTransactionId: "bet-80-b",
    };

    // Fire both 80.00 BRL bets simultaneously against two separate instances
    const [resA, resB] = await Promise.all([
      fetch(`${instance1.baseUrl}/wagering/transactions`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": "provider-race:bet-80-a",
        },
        body: JSON.stringify(bet1Payload),
      }).then(async (r) => ({ httpStatus: r.status, body: (await r.json()) as any })),
      fetch(`${instance2.baseUrl}/wagering/transactions`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": "provider-race:bet-80-b",
        },
        body: JSON.stringify(bet2Payload),
      }).then(async (r) => ({ httpStatus: r.status, body: (await r.json()) as any })),
    ]);

    const outcomes = [resA, resB];
    const processed = outcomes.filter(
      (o) => o.body.status === WagerTransactionStatus.Processed,
    );
    const rejected = outcomes.filter(
      (o) => o.body.status === WagerTransactionStatus.Rejected,
    );

    expect(processed.length).toBe(1);
    expect(rejected.length).toBe(1);
    expect(processed[0].httpStatus).toBe(200);
    expect(rejected[0].httpStatus).toBe(422);
    expect(rejected[0].body.failureCode).toBe(FailureCode.INSUFFICIENT_FUNDS);

    // Verify retry of both does not duplicate debit
    const retryA = await fetch(`${instance3.baseUrl}/wagering/transactions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": "provider-race:bet-80-a",
      },
      body: JSON.stringify(bet1Payload),
    }).then((r) => r.json() as Promise<any>);
    expect(retryA.idempotentReplay).toBe(true);

    const em = instance1.orm.em.fork();
    const debitEntries = await em.find(WalletLedgerEntryRecord, {
      walletId: wallet.id,
      direction: LedgerDirection.Debit,
    });
    expect(debitEntries.length).toBe(1);

    await assertWalletLedgerInvariant(wallet.id, "20.00");
  });

  it("3 & 4. Distinct wallets processed in parallel across >= 3 simultaneous instances", async () => {
    const baseUrls = [instance1.baseUrl, instance2.baseUrl, instance3.baseUrl];

    // Open 10 distinct wallets with 100.00 BRL each
    const wallets = await Promise.all(
      Array.from({ length: 10 }, (_, idx) =>
        fetch(`${baseUrls[idx % 3]}/wallets`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            playerId: `player-${idx}-${randomUUID()}`,
            initialBalance: { amount: "100.00", currency: "BRL" },
          }),
        }).then((r) => r.json() as Promise<any>),
      ),
    );

    // For each of the 10 wallets, fire 4 concurrent bets of 30.00 BRL across the 3 instances
    // (3 should succeed -> 90.00 debited, 1 should be rejected -> final balance 10.00 BRL on every wallet)
    await Promise.all(
      wallets.flatMap((w, wIdx) =>
        Array.from({ length: 4 }, (_, betIdx) => {
          const targetUrl = baseUrls[(wIdx + betIdx) % 3];
          const extId = `w-${wIdx}-bet-${betIdx}`;
          return fetch(`${targetUrl}/wagering/transactions`, {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "Idempotency-Key": `provider-multi:${extId}`,
            },
            body: JSON.stringify({
              providerId: "provider-multi",
              externalTransactionId: extId,
              playerId: w.playerId,
              walletId: w.id,
              roundId: `round-${wIdx}-${betIdx}`,
              gameId: "fortune-chimp",
              kind: "BET",
              money: { amount: "30.00", currency: "BRL" },
            }),
          });
        }),
      ),
    );

    for (const w of wallets) {
      await assertWalletLedgerInvariant(w.id, "10.00");
    }
  });

  it("5. Worker killed AFTER SQL commit and BEFORE SQS ack -> redelivery does not duplicate debit", async () => {
    const createRes = await fetch(`${instance1.baseUrl}/wallets`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        playerId: randomUUID(),
        initialBalance: { amount: "100.00", currency: "BRL" },
      }),
    });
    const wallet = (await createRes.json()) as any;

    const sqsEnvelope = {
      messageId: "msg-crash-after-commit",
      type: "WagerTransactionRequested",
      occurredAt: new Date().toISOString(),
      data: {
        providerId: "provider-crash",
        externalTransactionId: "tx-crash-1",
        idempotencyKey: "provider-crash:tx-crash-1",
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: "round-crash",
        gameId: "fortune-chimp",
        kind: "BET",
        money: { amount: "40.00", currency: "BRL" },
      },
    };

    await instance1.sqsClient.sendMessage({
      queueName: instance1.sqsClient.wagerQueueName,
      body: JSON.stringify(sqsEnvelope),
      messageGroupId: wallet.id,
      messageDeduplicationId: "dedup-crash-1",
    });

    const consumer1 = instance1.app.get(SqsWagerConsumer);
    const consumer2 = instance2.app.get(SqsWagerConsumer);

    const msgBatch1 = await instance1.sqsClient.receiveMessages(
      instance1.sqsClient.wagerQueueName,
      1,
      0,
      5,
    );
    expect(msgBatch1.length).toBe(1);

    // Instance 1 commits to PostgreSQL, then dies right before SQS ack
    await expect(
      consumer1.handleRawSqsMessage(msgBatch1[0], { crashBeforeAck: true }),
    ).rejects.toThrow("SIMULATED_WORKER_CRASH_AFTER_COMMIT_BEFORE_ACK");

    // Message becomes immediately visible again and is picked up by Instance 2
    const msgBatch2 = await instance2.sqsClient.receiveMessages(
      instance2.sqsClient.wagerQueueName,
      1,
      0,
      5,
    );
    expect(msgBatch2.length).toBe(1);

    const recoveryOutput = await consumer2.handleRawSqsMessage(msgBatch2[0]);
    expect(recoveryOutput?.idempotentReplay).toBe(true);
    expect(recoveryOutput?.balance.amount).toBe("60.00");

    await assertWalletLedgerInvariant(wallet.id, "60.00");
  });

  it("6. Two concurrent publishers over the same Outbox -> all events published without loss", async () => {
    const createRes = await fetch(`${instance1.baseUrl}/wallets`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        playerId: randomUUID(),
        initialBalance: { amount: "200.00", currency: "BRL" },
      }),
    });
    const wallet = (await createRes.json()) as any;

    // Generate 10 bets -> 2 (opening) + 20 (bets) = 22 outbox events
    for (let i = 0; i < 10; i++) {
      await fetch(`${instance1.baseUrl}/wagering/transactions`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": `provider-outbox:bet-${i}`,
        },
        body: JSON.stringify({
          providerId: "provider-outbox",
          externalTransactionId: `bet-${i}`,
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: `round-${i}`,
          gameId: "fortune-chimp",
          kind: "BET",
          money: { amount: "5.00", currency: "BRL" },
        }),
      });
    }

    const publisher1 = instance1.app.get(OutboxPublisherWorker);
    const publisher2 = instance2.app.get(OutboxPublisherWorker);

    const publishedEventIds: string[] = [];
    const customPublish = async (rec: OutboxMessageRecord) => {
      publishedEventIds.push(rec.id);
    };

    const [count1, count2] = await Promise.all([
      publisher1.processDueBatch(customPublish),
      publisher2.processDueBatch(customPublish),
    ]);

    expect(count1 + count2).toBe(22);
    expect(new Set(publishedEventIds).size).toBe(22);

    const unpublished = await instance1.orm.em
      .fork()
      .find(OutboxMessageRecord, { publishedAt: null });
    expect(unpublished.length).toBe(0);

    await assertWalletLedgerInvariant(wallet.id, "150.00");
  });

  it("7. Out-of-order REFUND/ROLLBACK delivered before referenced BET -> PENDING_REFERENCE then processed when BET arrives", async () => {
    const createRes = await fetch(`${instance1.baseUrl}/wallets`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        playerId: randomUUID(),
        initialBalance: { amount: "100.00", currency: "BRL" },
      }),
    });
    const wallet = (await createRes.json()) as any;

    // 1. ROLLBACK arrives first (before BET 'bet-late-1')
    const rollbackRes = await fetch(
      `${instance1.baseUrl}/wagering/transactions`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": "provider-ooo:rollback-early-1",
        },
        body: JSON.stringify({
          providerId: "provider-ooo",
          externalTransactionId: "rollback-early-1",
          playerId: wallet.playerId,
          walletId: wallet.id,
          roundId: "round-ooo-1",
          gameId: "fortune-chimp",
          kind: "ROLLBACK",
          money: { amount: "35.00", currency: "BRL" },
          referenceExternalTransactionId: "bet-late-1",
        }),
      },
    );
    expect(rollbackRes.status).toBe(202);
    const rollbackBody = (await rollbackRes.json()) as any;
    expect(rollbackBody.status).toBe(WagerTransactionStatus.PendingReference);

    // Balance is still 100.00 BRL while waiting for reference
    await assertWalletLedgerInvariant(wallet.id, "100.00");

    // 2. Referenced BET 'bet-late-1' of 35.00 BRL arrives -> balance becomes 65.00 BRL
    const betRes = await fetch(`${instance2.baseUrl}/wagering/transactions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": "provider-ooo:bet-late-1",
      },
      body: JSON.stringify({
        providerId: "provider-ooo",
        externalTransactionId: "bet-late-1",
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: "round-ooo-1",
        gameId: "fortune-chimp",
        kind: "BET",
        money: { amount: "35.00", currency: "BRL" },
      }),
    });
    expect(betRes.status).toBe(200);
    await assertWalletLedgerInvariant(wallet.id, "65.00");

    // 3. Scheduled PendingReferenceRetryWorker runs and applies the ROLLBACK -> balance restored to 100.00 BRL
    const retryWorker = instance3.app.get(PendingReferenceRetryWorker);
    const resolved = await retryWorker.processDuePendingReferences({
      forceDue: true,
    });
    expect(resolved).toBe(1);

    const updatedRollbackRes = await fetch(
      `${instance1.baseUrl}/wagering/transactions/${rollbackBody.transactionId}`,
    );
    const updatedRollback = (await updatedRollbackRes.json()) as any;
    expect(updatedRollback.status).toBe(WagerTransactionStatus.Processed);

    await assertWalletLedgerInvariant(wallet.id, "100.00");
  });

  it("8. Service restart with proof of final consistency across wallet, ledger, and outbox", async () => {
    // Start a dedicated ephemeral instance, process transactions, shut it down, start a new instance, and verify consistency
    const tempNodeA = await createAppInstance({
      instanceId: "restart-node-a",
      enableSqsPolling: false,
      enableOutboxWorker: false,
      enablePendingRefWorker: false,
    });

    const createRes = await fetch(`${tempNodeA.baseUrl}/wallets`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        playerId: randomUUID(),
        initialBalance: { amount: "300.00", currency: "BRL" },
      }),
    });
    const wallet = (await createRes.json()) as any;

    await fetch(`${tempNodeA.baseUrl}/wagering/transactions`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": "provider-restart:bet-1",
      },
      body: JSON.stringify({
        providerId: "provider-restart",
        externalTransactionId: "bet-1",
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: "round-restart",
        gameId: "fortune-chimp",
        kind: "BET",
        money: { amount: "75.00", currency: "BRL" },
      }),
    });

    // Shut down node A before outbox is published
    await tempNodeA.close();

    // Boot node B, publish pending outbox, and verify full reconciliation consistency
    const tempNodeB = await createAppInstance({
      instanceId: "restart-node-b",
      enableSqsPolling: false,
      enableOutboxWorker: false,
      enablePendingRefWorker: false,
    });

    try {
      const outboxWorkerB = tempNodeB.app.get(OutboxPublisherWorker);
      const publishedCount = await outboxWorkerB.processDueBatch();
      expect(publishedCount).toBe(4); // 2 from OPENING + 2 from BET

      const recRes = await fetch(
        `${tempNodeB.baseUrl}/wallets/${wallet.id}/reconciliation`,
        { method: "POST" },
      );
      const rec = (await recRes.json()) as any;
      expect(rec.consistent).toBe(true);
      expect(rec.storedBalance.amount).toBe("225.00");
      expect(rec.calculatedBalance.amount).toBe("225.00");
      expect(rec.checkedEntries).toBe(2);
    } finally {
      await tempNodeB.close();
    }
  });
});
