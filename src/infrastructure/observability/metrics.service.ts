import { Injectable } from "@nestjs/common";

export interface MetricsSnapshot {
  transactionsByStatus: Record<string, number>;
  transactionsByKind: Record<string, number>;
  idempotentDuplicatesDetected: number;
  inboxDuplicatesDetected: number;
  retriesTotal: Record<string, number>;
  dlqMessagesTotal: number;
  lockConflictsTotal: number;
  reconciliationDivergencesTotal: number;
  outboxPublishedTotal: number;
  outboxPendingGauge: number;
  outboxMaxLagMsGauge: number;
  processingLatencyMs: {
    count: number;
    sum: number;
    min: number;
    max: number;
    p50: number;
    p95: number;
    p99: number;
  };
}

@Injectable()
export class MetricsService {
  private readonly transactionsByStatus = new Map<string, number>();
  private readonly transactionsByKind = new Map<string, number>();
  private idempotentDuplicatesDetected = 0;
  private inboxDuplicatesDetected = 0;
  private readonly retriesTotal = new Map<string, number>();
  private dlqMessagesTotal = 0;
  private lockConflictsTotal = 0;
  private reconciliationDivergencesTotal = 0;
  private outboxPublishedTotal = 0;
  private outboxPendingGauge = 0;
  private outboxMaxLagMsGauge = 0;

  private readonly latencySamplesMs: number[] = [];
  private readonly maxSamples = 10_000;

  recordTransaction(kind: string, status: string): void {
    this.transactionsByStatus.set(
      status,
      (this.transactionsByStatus.get(status) ?? 0) + 1,
    );
    this.transactionsByKind.set(
      kind,
      (this.transactionsByKind.get(kind) ?? 0) + 1,
    );
  }

  incrementIdempotentDuplicate(): void {
    this.idempotentDuplicatesDetected += 1;
  }

  incrementInboxDuplicate(): void {
    this.inboxDuplicatesDetected += 1;
  }

  incrementRetry(source: "sqs" | "outbox" | "pending_reference" | "db_lock"): void {
    this.retriesTotal.set(source, (this.retriesTotal.get(source) ?? 0) + 1);
  }

  incrementDlqMessages(): void {
    this.dlqMessagesTotal += 1;
  }

  incrementLockConflict(): void {
    this.lockConflictsTotal += 1;
  }

  incrementReconciliationDivergence(): void {
    this.reconciliationDivergencesTotal += 1;
  }

  incrementOutboxPublished(count = 1): void {
    this.outboxPublishedTotal += count;
  }

  setOutboxLag(pendingCount: number, maxLagMs: number): void {
    this.outboxPendingGauge = pendingCount;
    this.outboxMaxLagMsGauge = Math.max(0, Math.round(maxLagMs));
  }

  recordProcessingLatency(durationMs: number): void {
    if (this.latencySamplesMs.length >= this.maxSamples) {
      this.latencySamplesMs.shift();
    }
    this.latencySamplesMs.push(durationMs);
  }

  getSnapshot(): MetricsSnapshot {
    const sorted = [...this.latencySamplesMs].sort((a, b) => a - b);
    const count = sorted.length;
    const sum = sorted.reduce((acc, v) => acc + v, 0);

    const percentile = (p: number): number => {
      if (count === 0) return 0;
      const idx = Math.min(count - 1, Math.floor((p / 100) * count));
      return Number(sorted[idx].toFixed(2));
    };

    return {
      transactionsByStatus: Object.fromEntries(this.transactionsByStatus),
      transactionsByKind: Object.fromEntries(this.transactionsByKind),
      idempotentDuplicatesDetected: this.idempotentDuplicatesDetected,
      inboxDuplicatesDetected: this.inboxDuplicatesDetected,
      retriesTotal: Object.fromEntries(this.retriesTotal),
      dlqMessagesTotal: this.dlqMessagesTotal,
      lockConflictsTotal: this.lockConflictsTotal,
      reconciliationDivergencesTotal: this.reconciliationDivergencesTotal,
      outboxPublishedTotal: this.outboxPublishedTotal,
      outboxPendingGauge: this.outboxPendingGauge,
      outboxMaxLagMsGauge: this.outboxMaxLagMsGauge,
      processingLatencyMs: {
        count,
        sum: Number(sum.toFixed(2)),
        min: count > 0 ? Number(sorted[0].toFixed(2)) : 0,
        max: count > 0 ? Number(sorted[count - 1].toFixed(2)) : 0,
        p50: percentile(50),
        p95: percentile(95),
        p99: percentile(99),
      },
    };
  }

  toPrometheusText(): string {
    const snap = this.getSnapshot();
    const lines: string[] = [];

    lines.push("# HELP wagering_transactions_total Total wagering transactions by status");
    lines.push("# TYPE wagering_transactions_total counter");
    for (const [status, val] of Object.entries(snap.transactionsByStatus)) {
      lines.push(`wagering_transactions_total{status="${status}"} ${val}`);
    }

    lines.push("# HELP wagering_duplicates_detected_total Total duplicate operations detected");
    lines.push("# TYPE wagering_duplicates_detected_total counter");
    lines.push(
      `wagering_duplicates_detected_total{layer="idempotency_key"} ${snap.idempotentDuplicatesDetected}`,
    );
    lines.push(
      `wagering_duplicates_detected_total{layer="inbox"} ${snap.inboxDuplicatesDetected}`,
    );

    lines.push("# HELP wagering_retries_total Total retries by component");
    lines.push("# TYPE wagering_retries_total counter");
    for (const [source, val] of Object.entries(snap.retriesTotal)) {
      lines.push(`wagering_retries_total{source="${source}"} ${val}`);
    }

    lines.push("# HELP wagering_dlq_messages_total Total messages routed to DLQ");
    lines.push("# TYPE wagering_dlq_messages_total counter");
    lines.push(`wagering_dlq_messages_total ${snap.dlqMessagesTotal}`);

    lines.push("# HELP wagering_lock_conflicts_total Total concurrency lock conflicts");
    lines.push("# TYPE wagering_lock_conflicts_total counter");
    lines.push(`wagering_lock_conflicts_total ${snap.lockConflictsTotal}`);

    lines.push("# HELP wagering_reconciliation_divergences_total Total wallet reconciliation divergences");
    lines.push("# TYPE wagering_reconciliation_divergences_total counter");
    lines.push(`wagering_reconciliation_divergences_total ${snap.reconciliationDivergencesTotal}`);

    lines.push("# HELP wagering_outbox_pending_messages Current pending messages in outbox");
    lines.push("# TYPE wagering_outbox_pending_messages gauge");
    lines.push(`wagering_outbox_pending_messages ${snap.outboxPendingGauge}`);

    lines.push("# HELP wagering_outbox_max_lag_ms Maximum age in ms of unpublished outbox messages");
    lines.push("# TYPE wagering_outbox_max_lag_ms gauge");
    lines.push(`wagering_outbox_max_lag_ms ${snap.outboxMaxLagMsGauge}`);

    lines.push("# HELP wagering_processing_latency_ms Processing latency quantiles in ms");
    lines.push("# TYPE wagering_processing_latency_ms summary");
    lines.push(`wagering_processing_latency_ms{quantile="0.5"} ${snap.processingLatencyMs.p50}`);
    lines.push(`wagering_processing_latency_ms{quantile="0.95"} ${snap.processingLatencyMs.p95}`);
    lines.push(`wagering_processing_latency_ms{quantile="0.99"} ${snap.processingLatencyMs.p99}`);
    lines.push(`wagering_processing_latency_ms_sum ${snap.processingLatencyMs.sum}`);
    lines.push(`wagering_processing_latency_ms_count ${snap.processingLatencyMs.count}`);

    return lines.join("\n") + "\n";
  }
}
