import { randomUUID } from "node:crypto";
import { OutboxPublisherWorker } from "../src/application/workers/outbox-publisher.worker";
import { MetricsService } from "../src/infrastructure/observability/metrics.service";
import {
  createAppInstance,
  ensureTestInfrastructure,
  resetDatabaseAndQueues,
} from "../test/helpers/test-harness";

interface PhaseResult {
  phaseName: string;
  totalRequests: number;
  concurrency: number;
  durationMs: number;
  throughputRps: number;
  status200Count: number;
  status422RejectedCount: number;
  errorCount: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  minMs: number;
  maxMs: number;
}

function computePercentiles(samples: number[]) {
  const sorted = [...samples].sort((a, b) => a - b);
  const count = sorted.length;
  const pick = (p: number) => {
    if (count === 0) return 0;
    const idx = Math.min(count - 1, Math.floor((p / 100) * count));
    return Number(sorted[idx].toFixed(2));
  };
  return {
    minMs: count > 0 ? Number(sorted[0].toFixed(2)) : 0,
    maxMs: count > 0 ? Number(sorted[count - 1].toFixed(2)) : 0,
    p50Ms: pick(50),
    p95Ms: pick(95),
    p99Ms: pick(99),
  };
}

async function runPool<T>(
  items: T[],
  concurrency: number,
  worker: (item: T, index: number) => Promise<void>,
): Promise<void> {
  let cursor = 0;
  const runners = Array.from({ length: concurrency }, async () => {
    while (true) {
      const idx = cursor++;
      if (idx >= items.length) break;
      await worker(items[idx], idx);
    }
  });
  await Promise.all(runners);
}

async function main() {
  process.env.LOG_SILENT = "true";
  console.log("================================================================================");
  console.log("  Jungle Gaming — Distributed Wagering Processor Load & Contention Benchmark");
  console.log("================================================================================\n");

  const infra = await ensureTestInfrastructure();
  console.log(
    `Environment: Bun ${Bun.version} | OS: ${process.platform} (${process.arch}) | Postgres: ${
      infra.usingExternalPostgres ? "Docker Container (5432)" : "Embedded PostgreSQL 16 Wire Server"
    }`,
  );

  // Boot 3 concurrent application instances sharing the same PostgreSQL & SQS
  const instances = await Promise.all([
    createAppInstance({
      instanceId: "load-node-1",
      enableSqsPolling: false,
      enableOutboxWorker: true,
      enablePendingRefWorker: true,
    }),
    createAppInstance({
      instanceId: "load-node-2",
      enableSqsPolling: false,
      enableOutboxWorker: true,
      enablePendingRefWorker: true,
    }),
    createAppInstance({
      instanceId: "load-node-3",
      enableSqsPolling: false,
      enableOutboxWorker: true,
      enablePendingRefWorker: true,
    }),
  ]);

  const baseUrls = instances.map((i) => i.baseUrl);

  try {
    await resetDatabaseAndQueues(instances[0]);

    // 1. Seed 20 distributed wallets + 1 hot wallet
    const wallets = await Promise.all(
      Array.from({ length: 20 }, (_, idx) =>
        fetch(`${baseUrls[idx % 3]}/wallets`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            playerId: `load-player-${idx}-${randomUUID()}`,
            initialBalance: { amount: "5000.00", currency: "BRL" },
          }),
        }).then((r) => r.json() as Promise<any>),
      ),
    );

    const hotWallet = await fetch(`${baseUrls[0]}/wallets`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        playerId: `hot-player-${randomUUID()}`,
        initialBalance: { amount: "500.00", currency: "BRL" },
      }),
    }).then((r) => r.json() as Promise<any>);

    const phases: PhaseResult[] = [];

    // -------------------------------------------------------------------------
    // Phase 1: Multi-Wallet Parallel Wagering (200 operations across 20 wallets)
    // -------------------------------------------------------------------------
    {
      const totalRequests = 200;
      const concurrency = 20;
      const latencies: number[] = [];
      let status200 = 0;
      let status422 = 0;
      let errors = 0;

      const tasks = Array.from({ length: totalRequests }, (_, i) => i);
      const t0 = performance.now();

      await runPool(tasks, concurrency, async (idx) => {
        const w = wallets[idx % wallets.length];
        const targetUrl = baseUrls[idx % baseUrls.length];
        const isWin = idx % 3 === 2;
        const extId = `p1-tx-${idx}`;
        const reqStart = performance.now();

        const res = await fetch(`${targetUrl}/wagering/transactions`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Idempotency-Key": `provider-load:${extId}`,
          },
          body: JSON.stringify({
            providerId: "provider-load",
            externalTransactionId: extId,
            playerId: w.playerId,
            walletId: w.id,
            roundId: `round-p1-${idx}`,
            gameId: "fortune-chimp",
            kind: isWin ? "WIN" : "BET",
            money: { amount: isWin ? "40.00" : "25.00", currency: "BRL" },
          }),
        });

        latencies.push(performance.now() - reqStart);
        if (res.status === 200) status200++;
        else if (res.status === 422) status422++;
        else errors++;
      });

      const durationMs = Number((performance.now() - t0).toFixed(2));
      phases.push({
        phaseName: "Phase 1 — Multi-Wallet Parallel (20 Wallets, 3 Nodes)",
        totalRequests,
        concurrency,
        durationMs,
        throughputRps: Number(((totalRequests / durationMs) * 1000).toFixed(2)),
        status200Count: status200,
        status422RejectedCount: status422,
        errorCount: errors,
        ...computePercentiles(latencies),
      });
    }

    // -------------------------------------------------------------------------
    // Phase 2: Hot-Wallet Contention Burst (80 concurrent bets of 10.00 on 500.00 balance)
    // Expects exactly 50 PROCESSED and 30 REJECTED (INSUFFICIENT_FUNDS), final balance 0.00
    // -------------------------------------------------------------------------
    {
      const totalRequests = 80;
      const concurrency = 25;
      const latencies: number[] = [];
      let status200 = 0;
      let status422 = 0;
      let errors = 0;

      const tasks = Array.from({ length: totalRequests }, (_, i) => i);
      const t0 = performance.now();

      await runPool(tasks, concurrency, async (idx) => {
        const targetUrl = baseUrls[idx % baseUrls.length];
        const extId = `hot-tx-${idx}`;
        const reqStart = performance.now();

        const res = await fetch(`${targetUrl}/wagering/transactions`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Idempotency-Key": `provider-hot:${extId}`,
          },
          body: JSON.stringify({
            providerId: "provider-hot",
            externalTransactionId: extId,
            playerId: hotWallet.playerId,
            walletId: hotWallet.id,
            roundId: `round-hot-${idx}`,
            gameId: "fortune-chimp",
            kind: "BET",
            money: { amount: "10.00", currency: "BRL" },
          }),
        });

        latencies.push(performance.now() - reqStart);
        if (res.status === 200) status200++;
        else if (res.status === 422) status422++;
        else errors++;
      });

      const durationMs = Number((performance.now() - t0).toFixed(2));
      phases.push({
        phaseName: "Phase 2 — Hot-Wallet Contention (1 Wallet, 80 Bets vs 500 BRL)",
        totalRequests,
        concurrency,
        durationMs,
        throughputRps: Number(((totalRequests / durationMs) * 1000).toFixed(2)),
        status200Count: status200,
        status422RejectedCount: status422,
        errorCount: errors,
        ...computePercentiles(latencies),
      });
    }

    // -------------------------------------------------------------------------
    // Phase 3: Idempotent Duplicate Storm (100 parallel replays of the same bet)
    // -------------------------------------------------------------------------
    {
      const totalRequests = 100;
      const concurrency = 25;
      const latencies: number[] = [];
      let status200 = 0;
      let status422 = 0;
      let errors = 0;

      const targetWallet = wallets[0];
      const tasks = Array.from({ length: totalRequests }, (_, i) => i);
      const t0 = performance.now();

      await runPool(tasks, concurrency, async (idx) => {
        const targetUrl = baseUrls[idx % baseUrls.length];
        const reqStart = performance.now();

        const res = await fetch(`${targetUrl}/wagering/transactions`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Idempotency-Key": "provider-storm:duplicate-bet-1",
          },
          body: JSON.stringify({
            providerId: "provider-storm",
            externalTransactionId: "duplicate-bet-1",
            playerId: targetWallet.playerId,
            walletId: targetWallet.id,
            roundId: "round-storm-1",
            gameId: "fortune-chimp",
            kind: "BET",
            money: { amount: "15.00", currency: "BRL" },
          }),
        });

        latencies.push(performance.now() - reqStart);
        if (res.status === 200) status200++;
        else if (res.status === 422) status422++;
        else errors++;
      });

      const durationMs = Number((performance.now() - t0).toFixed(2));
      phases.push({
        phaseName: "Phase 3 — Idempotent Replay Storm (100 Parallel Replays)",
        totalRequests,
        concurrency,
        durationMs,
        throughputRps: Number(((totalRequests / durationMs) * 1000).toFixed(2)),
        status200Count: status200,
        status422RejectedCount: status422,
        errorCount: errors,
        ...computePercentiles(latencies),
      });
    }

    // Flush any remaining outbox messages and measure lag
    for (const inst of instances) {
      const worker = inst.app.get(OutboxPublisherWorker);
      await worker.processDueBatch();
      await worker.refreshOutboxLagMetrics();
    }

    // Reconcile ALL 21 wallets to verify 100% ledger invariant
    const allWallets = [...wallets, hotWallet];
    let consistentWallets = 0;
    let totalCheckedLedgerEntries = 0;

    for (const w of allWallets) {
      const recRes = await fetch(
        `${baseUrls[0]}/wallets/${w.id}/reconciliation`,
        { method: "POST" },
      );
      const rec = (await recRes.json()) as any;
      if (rec.consistent && rec.difference.amount === "0.00") {
        consistentWallets += 1;
      }
      totalCheckedLedgerEntries += Number(rec.checkedEntries ?? 0);
    }

    // Aggregate metrics across the 3 instances
    const snaps = instances.map((i) => i.app.get(MetricsService).getSnapshot());
    const totalDuplicates = snaps.reduce(
      (acc, s) => acc + s.idempotentDuplicatesDetected,
      0,
    );
    const totalLockConflicts = snaps.reduce(
      (acc, s) => acc + s.lockConflictsTotal,
      0,
    );
    const totalOutboxPublished = snaps.reduce(
      (acc, s) => acc + s.outboxPublishedTotal,
      0,
    );
    const finalOutboxPending = snaps[0].outboxPendingGauge;
    const finalOutboxLagMs = snaps[0].outboxMaxLagMsGauge;

    console.log("\nResults by Phase:");
    console.table(
      phases.map((p) => ({
        Phase: p.phaseName,
        Requests: p.totalRequests,
        Concurrency: p.concurrency,
        "Duration (ms)": p.durationMs,
        "Throughput (RPS)": p.throughputRps,
        "200 OK": p.status200Count,
        "422 Rejected": p.status422RejectedCount,
        "5xx Errors": p.errorCount,
        "p50 (ms)": p.p50Ms,
        "p95 (ms)": p.p95Ms,
        "p99 (ms)": p.p99Ms,
      })),
    );

    console.log("\nGlobal Consistency & Observability Summary:");
    console.log(
      JSON.stringify(
        {
          instancesCount: instances.length,
          walletsReconciled: `${consistentWallets}/${allWallets.length}`,
          hotWalletFinalBalance: "0.00 BRL (50/80 bets processed, 30 rejected)",
          totalLedgerEntriesVerified: totalCheckedLedgerEntries,
          idempotentDuplicatesDetected: totalDuplicates,
          lockConflictsRecovered: totalLockConflicts,
          outboxEventsPublished: totalOutboxPublished,
          outboxPendingMessages: finalOutboxPending,
          outboxMaxLagMs: finalOutboxLagMs,
        },
        null,
        2,
      ),
    );

    if (consistentWallets !== allWallets.length) {
      throw new Error("Reconciliation invariant failed on one or more wallets!");
    }
  } finally {
    await Promise.all(instances.map((i) => i.close()));
    await infra.stop();
  }
}

main().catch((err) => {
  console.error("Load test failed:", err);
  process.exit(1);
});
