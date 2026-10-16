import { EntityManager } from "@mikro-orm/postgresql";
import {
  Inject,
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from "@nestjs/common";
import { OutboxMessageRecord } from "../../infrastructure/database/entities/persistence-entities";
import { SqsClientService } from "../../infrastructure/messaging/sqs-client.service";
import { StructuredLogger } from "../../infrastructure/observability/logger.service";
import { MetricsService } from "../../infrastructure/observability/metrics.service";

export interface OutboxRawRow {
  id: string;
  aggregate_id: string;
  event_type: string;
  payload: Record<string, unknown> | string;
  occurred_at: string | Date;
  attempts: number;
  next_attempt_at: string | Date | null;
  published_at: string | Date | null;
}

@Injectable()
export class OutboxPublisherWorker implements OnModuleInit, OnModuleDestroy {
  private timer?: ReturnType<typeof setInterval>;
  private isShuttingDown = false;
  private activeBatchPromise: Promise<number> | null = null;

  private readonly pollIntervalMs = Number(
    process.env.OUTBOX_POLL_INTERVAL_MS ?? 200,
  );
  private readonly batchSize = Number(process.env.OUTBOX_BATCH_SIZE ?? 50);

  constructor(
    @Inject(EntityManager) private readonly em: EntityManager,
    @Inject(SqsClientService) private readonly sqsClient: SqsClientService,
    @Inject(StructuredLogger) private readonly logger: StructuredLogger,
    @Inject(MetricsService) private readonly metrics: MetricsService,
  ) {}

  onModuleInit(): void {
    if (process.env.OUTBOX_WORKER_DISABLED === "true") {
      return;
    }
    this.timer = setInterval(() => {
      if (!this.isShuttingDown && !this.activeBatchPromise) {
        this.activeBatchPromise = this.processDueBatch()
          .catch((err) => {
            this.logger.error("OutboxPublisherWorker iteration failed", {}, err);
            return 0;
          })
          .finally(() => {
            this.activeBatchPromise = null;
          });
      }
    }, this.pollIntervalMs);
  }

  async onModuleDestroy(): Promise<void> {
    this.isShuttingDown = true;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
    if (this.activeBatchPromise) {
      await this.activeBatchPromise;
    }
  }

  /**
   * Claims a batch of unpublished outbox messages using `FOR UPDATE SKIP LOCKED`
   * so multiple concurrent publisher workers never block each other or double-claim
   * the same in-flight batch.
   */
  async processDueBatch(customPublishFn?: (record: OutboxMessageRecord) => Promise<void>): Promise<number> {
    let publishedCount = 0;

    await this.em.transactional(async (txEm) => {
      const rows = (await txEm.execute(
        `
        SELECT id, aggregate_id, event_type, payload, occurred_at, attempts, next_attempt_at, published_at
        FROM outbox_messages
        WHERE published_at IS NULL
          AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
        ORDER BY occurred_at ASC
        LIMIT ?
        FOR UPDATE SKIP LOCKED
        `,
        [this.batchSize],
      )) as OutboxRawRow[];

      if (rows.length === 0) {
        return;
      }

      const now = new Date();

      for (const row of rows) {
        const record = new OutboxMessageRecord();
        record.id = row.id;
        record.aggregateId = row.aggregate_id;
        record.eventType = row.event_type;
        record.payload =
          typeof row.payload === "string"
            ? JSON.parse(row.payload)
            : row.payload;
        record.occurredAt = new Date(row.occurred_at);
        record.attempts = Number(row.attempts);
        record.nextAttemptAt = row.next_attempt_at
          ? new Date(row.next_attempt_at)
          : null;
        record.publishedAt = row.published_at
          ? new Date(row.published_at)
          : null;

        const domainOutbox = record.toDomain();

        try {
          if (customPublishFn) {
            await customPublishFn(record);
          } else {
            await this.sqsClient.sendMessage({
              queueName: this.sqsClient.eventsQueueName,
              body: JSON.stringify(domainOutbox.payload),
              messageGroupId: domainOutbox.aggregateId,
              messageDeduplicationId: domainOutbox.id,
            });
          }

          domainOutbox.markPublished(now);
          await txEm.execute(
            `
            UPDATE outbox_messages
            SET published_at = ?, next_attempt_at = NULL
            WHERE id = ?
            `,
            [now.toISOString(), domainOutbox.id],
          );

          publishedCount += 1;
        } catch (err) {
          domainOutbox.scheduleRetry(now);
          this.metrics.incrementRetry("outbox");

          await txEm.execute(
            `
            UPDATE outbox_messages
            SET attempts = ?, next_attempt_at = ?
            WHERE id = ?
            `,
            [
              domainOutbox.attempts,
              domainOutbox.nextAttemptAt?.toISOString() ?? now.toISOString(),
              domainOutbox.id,
            ],
          );

          this.logger.warn("Failed to publish outbox message; scheduled retry", {
            outboxId: domainOutbox.id,
            aggregateId: domainOutbox.aggregateId,
            eventType: domainOutbox.eventType,
            attempts: domainOutbox.attempts,
          });
        }
      }
    });

    if (publishedCount > 0) {
      this.metrics.incrementOutboxPublished(publishedCount);
    }

    await this.refreshOutboxLagMetrics();
    return publishedCount;
  }

  async refreshOutboxLagMetrics(): Promise<void> {
    try {
      const stats = (await this.em.execute(
        `
        SELECT
          COUNT(*)::int AS pending_count,
          COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(occurred_at))) * 1000, 0)::float AS max_lag_ms
        FROM outbox_messages
        WHERE published_at IS NULL
        `,
      )) as Array<{ pending_count: number; max_lag_ms: number }>;

      if (stats[0]) {
        this.metrics.setOutboxLag(
          Number(stats[0].pending_count ?? 0),
          Number(stats[0].max_lag_ms ?? 0),
        );
      }
    } catch {
      // Ignore lag metric errors during shutdown
    }
  }
}
