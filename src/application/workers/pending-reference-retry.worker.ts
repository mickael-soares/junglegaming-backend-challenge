import { LockMode } from "@mikro-orm/core";
import { EntityManager } from "@mikro-orm/postgresql";
import {
  Inject,
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from "@nestjs/common";
import { randomUUID } from "node:crypto";
import { OutboxMessage } from "../../domain/entities/outbox-message";
import {
  WagerTransaction,
  WagerTransactionStatus,
} from "../../domain/entities/wager-transaction";
import { FailureCode } from "../../domain/errors/domain-errors";
import { WageringDomainService } from "../../domain/services/wagering-domain-service";
import {
  OutboxMessageRecord,
  WagerTransactionRecord,
  WalletLedgerEntryRecord,
  WalletRecord,
} from "../../infrastructure/database/entities/persistence-entities";
import { StructuredLogger } from "../../infrastructure/observability/logger.service";
import { MetricsService } from "../../infrastructure/observability/metrics.service";

@Injectable()
export class PendingReferenceRetryWorker
  implements OnModuleInit, OnModuleDestroy
{
  private timer?: ReturnType<typeof setInterval>;
  private isShuttingDown = false;
  private activePromise: Promise<number> | null = null;

  private readonly pollIntervalMs = Number(
    process.env.PENDING_REF_POLL_INTERVAL_MS ?? 200,
  );
  private readonly maxAttempts = Number(
    process.env.PENDING_REF_MAX_ATTEMPTS ?? 8,
  );
  private readonly baseDelayMs = Number(
    process.env.PENDING_REF_BASE_DELAY_MS ?? 100,
  );

  constructor(
    @Inject(EntityManager) private readonly em: EntityManager,
    @Inject(StructuredLogger) private readonly logger: StructuredLogger,
    @Inject(MetricsService) private readonly metrics: MetricsService,
  ) {}

  onModuleInit(): void {
    if (process.env.PENDING_REF_WORKER_DISABLED === "true") {
      return;
    }
    this.timer = setInterval(() => {
      if (!this.isShuttingDown && !this.activePromise) {
        this.activePromise = this.processDuePendingReferences()
          .catch((err) => {
            this.logger.error(
              "PendingReferenceRetryWorker iteration failed",
              {},
              err,
            );
            return 0;
          })
          .finally(() => {
            this.activePromise = null;
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
    if (this.activePromise) {
      await this.activePromise;
    }
  }

  /**
   * Re-processes transactions in PENDING_REFERENCE status whose next_retry_at is due.
   * Uses FOR UPDATE SKIP LOCKED to coordinate safely across multiple instances.
   * When retryAttempts reaches maxAttempts, transitions the transaction to REJECTED
   * with FailureCode.REFERENCE_NOT_FOUND_TIMEOUT and emits WagerTransactionRejected.
   */
  async processDuePendingReferences(options?: {
    forceDue?: boolean;
    maxAttemptsOverride?: number;
  }): Promise<number> {
    const maxAttempts = options?.maxAttemptsOverride ?? this.maxAttempts;
    let resolvedCount = 0;

    const conn = this.em.getConnection();
    const candidateRows = (await conn.execute(
      `
      SELECT id
      FROM wager_transactions
      WHERE status = 'PENDING_REFERENCE'
        ${options?.forceDue ? "" : "AND (next_retry_at IS NULL OR next_retry_at <= NOW())"}
      ORDER BY created_at ASC
      LIMIT 25
      `,
    )) as Array<{ id: string }>;

    for (const row of candidateRows) {
      if (this.isShuttingDown) break;

      const resolved = await this.em.transactional(async (txEm) => {
        const txRecord = await txEm.findOne(
          WagerTransactionRecord,
          { id: row.id, status: WagerTransactionStatus.PendingReference },
          { lockMode: LockMode.PESSIMISTIC_WRITE },
        );

        if (!txRecord) {
          return false;
        }

        const walletRecord = await txEm.findOne(
          WalletRecord,
          { id: txRecord.walletId },
          { lockMode: LockMode.PESSIMISTIC_WRITE },
        );

        if (!walletRecord) {
          return false;
        }

        const wallet = walletRecord.toDomain();
        const tx = txRecord.toDomain();
        const now = new Date();
        const correlationId = randomUUID();

        let referenceTx: WagerTransaction | null = null;
        let existingReversals: WagerTransaction[] = [];

        if (tx.referenceExternalTransactionId) {
          const refRecord = await txEm.findOne(WagerTransactionRecord, {
            providerId: tx.providerId,
            externalTransactionId: tx.referenceExternalTransactionId,
          });
          if (refRecord) {
            referenceTx = refRecord.toDomain();
            const reversalRecords = await txEm.find(WagerTransactionRecord, {
              referenceTransactionId: referenceTx.id,
              status: WagerTransactionStatus.Processed,
            });
            existingReversals = reversalRecords.map((r) => r.toDomain());
          }
        }

        // Check if reference is now available and terminal
        const isReferenceReady =
          referenceTx !== null &&
          referenceTx.status !== WagerTransactionStatus.Pending &&
          referenceTx.status !== WagerTransactionStatus.PendingReference;

        if (isReferenceReady) {
          const applied = WageringDomainService.applyTransaction({
            wallet,
            transaction: tx,
            referenceTransaction: referenceTx,
            existingProcessedReversalsForReference: existingReversals,
            eventContext: {
              correlationId,
              causationId: tx.id,
              occurredAt: now,
            },
            now,
          });

          walletRecord.applyDomain(applied.wallet);
          txRecord.applyDomain(applied.transaction);
          txEm.persist(walletRecord);
          txEm.persist(txRecord);
          await txEm.flush();

          if (applied.ledgerEntry) {
            txEm.persist(WalletLedgerEntryRecord.fromDomain(applied.ledgerEntry));
          }

          for (const event of applied.events) {
            txEm.persist(
              OutboxMessageRecord.fromDomain(OutboxMessage.enqueue(event)),
            );
          }

          await txEm.flush();

          this.metrics.recordTransaction(
            applied.transaction.kind,
            applied.transaction.status,
          );

          this.logger.info("Resolved PENDING_REFERENCE transaction", {
            correlationId,
            transactionId: tx.id,
            walletId: wallet.id,
            providerId: tx.providerId,
            kind: tx.kind,
            status: applied.transaction.status,
            failureCode: applied.transaction.failureCode,
          });

          return true;
        }

        // Reference still missing -> check retry limit
        this.metrics.incrementRetry("pending_reference");
        const nextAttemptCount = tx.retryAttempts + 1;

        if (nextAttemptCount >= maxAttempts) {
          const rejected = WageringDomainService.rejectTransaction(
            wallet,
            tx,
            FailureCode.REFERENCE_NOT_FOUND_TIMEOUT,
            {
              correlationId,
              causationId: tx.id,
              occurredAt: now,
            },
            now,
          );

          txRecord.applyDomain(rejected.transaction);
          txRecord.retryAttempts = nextAttemptCount;
          txEm.persist(txRecord);

          for (const event of rejected.events) {
            txEm.persist(
              OutboxMessageRecord.fromDomain(OutboxMessage.enqueue(event)),
            );
          }

          await txEm.flush();

          this.metrics.recordTransaction(
            rejected.transaction.kind,
            rejected.transaction.status,
          );

          this.logger.warn(
            "PENDING_REFERENCE transaction exhausted retries and was REJECTED",
            {
              correlationId,
              transactionId: tx.id,
              walletId: wallet.id,
              providerId: tx.providerId,
              failureCode: FailureCode.REFERENCE_NOT_FOUND_TIMEOUT,
              attempts: nextAttemptCount,
            },
          );

          return true;
        }

        tx.scheduleReferenceRetry(now, this.baseDelayMs);
        txRecord.applyDomain(tx);
        txEm.persist(txRecord);
        await txEm.flush();
        return false;
      });

      if (resolved) {
        resolvedCount += 1;
      }
    }

    return resolvedCount;
  }
}
