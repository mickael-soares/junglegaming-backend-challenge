import { LockMode } from "@mikro-orm/core";
import { EntityManager } from "@mikro-orm/postgresql";
import { Inject, Injectable } from "@nestjs/common";
import { randomUUID } from "node:crypto";
import { InboxMessage } from "../../domain/entities/inbox-message";
import { OutboxMessage } from "../../domain/entities/outbox-message";
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../../domain/entities/wager-transaction";
import {
  EntityNotFoundError,
  FailureCode,
  IdempotencyConflictError,
  InvalidPayloadError,
} from "../../domain/errors/domain-errors";
import { WageringDomainService } from "../../domain/services/wagering-domain-service";
import { computeWagerPayloadHash } from "../../domain/utils/canonical-json";
import { Money, MoneyProps } from "../../domain/value-objects/money";
import {
  InboxMessageRecord,
  OutboxMessageRecord,
  WagerTransactionRecord,
  WalletLedgerEntryRecord,
  WalletRecord,
} from "../../infrastructure/database/entities/persistence-entities";
import { StructuredLogger } from "../../infrastructure/observability/logger.service";
import { MetricsService } from "../../infrastructure/observability/metrics.service";

export interface InboxExecutionContext {
  consumerName: string;
  messageId: string;
  messagePayloadHash: string;
}

export interface ProcessWagerTransactionInput {
  idempotencyKey: string;
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind | string;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
  correlationId?: string;
  inboxContext?: InboxExecutionContext;
}

export interface ProcessWagerTransactionOutput {
  transactionId: string;
  status: WagerTransactionStatus;
  balance: MoneyProps;
  idempotentReplay: boolean;
  failureCode?: FailureCode;
}

@Injectable()
export class ProcessWagerTransactionUseCase {
  constructor(
    @Inject(EntityManager) private readonly em: EntityManager,
    @Inject(StructuredLogger) private readonly logger: StructuredLogger,
    @Inject(MetricsService) private readonly metrics: MetricsService,
  ) {}

  async execute(
    input: ProcessWagerTransactionInput,
  ): Promise<ProcessWagerTransactionOutput> {
    const startTime = performance.now();
    const correlationId = input.correlationId ?? randomUUID();

    this.validateContractInput(input);

    const idempotencyKey = input.idempotencyKey.trim();
    const providerId = input.providerId.trim();
    const externalTransactionId = input.externalTransactionId.trim();
    const playerId = input.playerId.trim();
    const walletId = input.walletId.trim();
    const roundId = input.roundId.trim();
    const gameId = input.gameId.trim();
    const kind = input.kind as WagerTransactionKind;
    const referenceExternalTransactionId =
      input.referenceExternalTransactionId?.trim() || undefined;

    if (kind === WagerTransactionKind.Opening) {
      throw new InvalidPayloadError(
        "OPENING transaction kind is internal and cannot be submitted via HTTP or SQS",
        FailureCode.FORBIDDEN_OPENING_KIND,
      );
    }

    const money = Money.from(input.money);

    const payloadHash = computeWagerPayloadHash({
      providerId,
      externalTransactionId,
      playerId,
      walletId,
      roundId,
      gameId,
      kind,
      money: money.toJSON(),
      referenceExternalTransactionId,
    });

    const candidateTransaction = WagerTransaction.create({
      id: randomUUID(),
      providerId,
      externalTransactionId,
      idempotencyKey,
      payloadHash,
      walletId,
      playerId,
      roundId,
      gameId,
      kind,
      money,
      referenceExternalTransactionId,
    });

    try {
      const result = await this.em.transactional(async (txEm) => {
        const now = new Date();

        if (input.inboxContext) {
          const existingInbox = await txEm.findOne(
            InboxMessageRecord,
            {
              consumerName: input.inboxContext.consumerName,
              messageId: input.inboxContext.messageId,
            },
            { lockMode: LockMode.PESSIMISTIC_WRITE },
          );

          if (existingInbox && existingInbox.processedAt) {
            const existingTxRec = await txEm.findOne(WagerTransactionRecord, {
              idempotencyKey,
            });
            if (existingTxRec) {
              const existingTx = existingTxRec.toDomain();
              this.metrics.incrementInboxDuplicate();
              const walletRec = await txEm.findOne(WalletRecord, { id: existingTx.walletId });
              const balanceProps =
                existingTx.observedBalance?.toJSON() ??
                walletRec?.toDomain().balance.toJSON() ??
                money.toJSON();

              return {
                transactionId: existingTx.id,
                status: existingTx.status,
                balance: balanceProps,
                idempotentReplay: true,
                ...(existingTx.failureCode ? { failureCode: existingTx.failureCode } : {}),
              };
            }
          }
        }

        const preExistingByKey = await txEm.findOne(WagerTransactionRecord, {
          idempotencyKey,
        });
        if (preExistingByKey) {
          return await this.handleExistingTransaction(
            txEm,
            preExistingByKey,
            payloadHash,
            idempotencyKey,
            input.inboxContext,
            now,
          );
        }

        const walletRecord = await txEm.findOne(
          WalletRecord,
          { id: walletId },
          { lockMode: LockMode.PESSIMISTIC_WRITE },
        );

        if (!walletRecord) {
          throw new EntityNotFoundError(
            FailureCode.WALLET_NOT_FOUND,
            `Wallet '${walletId}' not found`,
          );
        }

        // Re-check under the row lock in case a concurrent worker committed first
        const existingByKey = await txEm.findOne(WagerTransactionRecord, {
          idempotencyKey,
        });
        if (existingByKey) {
          return await this.handleExistingTransaction(
            txEm,
            existingByKey,
            payloadHash,
            idempotencyKey,
            input.inboxContext,
            now,
          );
        }

        const existingByExternal = await txEm.findOne(WagerTransactionRecord, {
          providerId,
          externalTransactionId,
        });
        if (existingByExternal) {
          throw new IdempotencyConflictError(
            idempotencyKey,
            `External transaction '${providerId}:${externalTransactionId}' already exists with a different idempotency key`,
          );
        }

        const wallet = walletRecord.toDomain();

        let referenceTransaction: WagerTransaction | null = null;
        let existingReversals: WagerTransaction[] = [];

        if (referenceExternalTransactionId) {
          const refRecord = await txEm.findOne(WagerTransactionRecord, {
            providerId,
            externalTransactionId: referenceExternalTransactionId,
          });
          if (refRecord) {
            referenceTransaction = refRecord.toDomain();
            const reversalRecords = await txEm.find(WagerTransactionRecord, {
              referenceTransactionId: referenceTransaction.id,
              status: WagerTransactionStatus.Processed,
            });
            existingReversals = reversalRecords.map((r) => r.toDomain());
          }
        }

        const applied = WageringDomainService.applyTransaction({
          wallet,
          transaction: candidateTransaction,
          referenceTransaction,
          existingProcessedReversalsForReference: existingReversals,
          eventContext: {
            correlationId,
            causationId: input.inboxContext?.messageId ?? idempotencyKey,
            occurredAt: now,
          },
          now,
        });

        walletRecord.applyDomain(applied.wallet);
        txEm.persist(walletRecord);

        const txRecord = WagerTransactionRecord.fromDomain(applied.transaction);
        txEm.persist(txRecord);
        await txEm.flush();

        if (applied.ledgerEntry) {
          txEm.persist(WalletLedgerEntryRecord.fromDomain(applied.ledgerEntry));
        }

        if (input.inboxContext) {
          let inboxRec = await txEm.findOne(InboxMessageRecord, {
            consumerName: input.inboxContext.consumerName,
            messageId: input.inboxContext.messageId,
          });
          if (!inboxRec) {
            const inboxDomain = InboxMessage.receive({
              consumerName: input.inboxContext.consumerName,
              messageId: input.inboxContext.messageId,
              payloadHash: input.inboxContext.messagePayloadHash,
              receivedAt: now,
            });
            inboxDomain.markProcessed(now);
            inboxRec = InboxMessageRecord.fromDomain(inboxDomain);
          } else {
            inboxRec.processedAt = now;
          }
          txEm.persist(inboxRec);
        }

        for (const event of applied.events) {
          const outboxMsg = OutboxMessage.enqueue(event);
          txEm.persist(OutboxMessageRecord.fromDomain(outboxMsg));
        }

        await txEm.flush();

        this.metrics.recordTransaction(
          applied.transaction.kind,
          applied.transaction.status,
        );

        return {
          transactionId: applied.transaction.id,
          status: applied.transaction.status,
          balance: applied.wallet.balance.toJSON(),
          idempotentReplay: false,
          ...(applied.transaction.failureCode
            ? { failureCode: applied.transaction.failureCode }
            : {}),
        };
      });

      const durationMs = Number((performance.now() - startTime).toFixed(2));
      this.metrics.recordProcessingLatency(durationMs);

      this.logger.info("Processed wager transaction", {
        correlationId,
        messageId: input.inboxContext?.messageId,
        transactionId: result.transactionId,
        walletId,
        providerId,
        playerId,
        roundId,
        kind,
        status: result.status,
        failureCode: result.failureCode,
        idempotentReplay: result.idempotentReplay,
        durationMs,
      });

      return result;
    } catch (err: unknown) {
      // Handle concurrent unique constraint race on idempotency_key or (provider_id, external_transaction_id)
      if (this.isPgUniqueViolation(err)) {
        this.metrics.incrementLockConflict();
        return await this.recoverFromUniqueViolationRace(
          idempotencyKey,
          providerId,
          externalTransactionId,
          payloadHash,
          input.inboxContext,
        );
      }
      throw err;
    }
  }

  private async handleExistingTransaction(
    txEm: EntityManager,
    existingRecord: WagerTransactionRecord,
    incomingPayloadHash: string,
    idempotencyKey: string,
    inboxContext: InboxExecutionContext | undefined,
    now: Date,
  ): Promise<ProcessWagerTransactionOutput> {
    const existingTx = existingRecord.toDomain();

    if (!existingTx.matchesPayload(incomingPayloadHash)) {
      throw new IdempotencyConflictError(idempotencyKey);
    }

    this.metrics.incrementIdempotentDuplicate();

    if (inboxContext) {
      let inboxRec = await txEm.findOne(InboxMessageRecord, {
        consumerName: inboxContext.consumerName,
        messageId: inboxContext.messageId,
      });
      if (!inboxRec) {
        const inboxDomain = InboxMessage.receive({
          consumerName: inboxContext.consumerName,
          messageId: inboxContext.messageId,
          payloadHash: inboxContext.messagePayloadHash,
          receivedAt: now,
        });
        inboxDomain.markProcessed(now);
        inboxRec = InboxMessageRecord.fromDomain(inboxDomain);
        txEm.persist(inboxRec);
        await txEm.flush();
      } else if (!inboxRec.processedAt) {
        inboxRec.processedAt = now;
        txEm.persist(inboxRec);
        await txEm.flush();
      }
    }

    let balanceProps = existingTx.observedBalance?.toJSON();
    if (!balanceProps) {
      const walletRec = await txEm.findOne(WalletRecord, {
        id: existingTx.walletId,
      });
      balanceProps =
        walletRec?.toDomain().balance.toJSON() ?? existingTx.money.toJSON();
    }

    return {
      transactionId: existingTx.id,
      status: existingTx.status,
      balance: balanceProps,
      idempotentReplay: true,
      ...(existingTx.failureCode ? { failureCode: existingTx.failureCode } : {}),
    };
  }

  private async recoverFromUniqueViolationRace(
    idempotencyKey: string,
    providerId: string,
    externalTransactionId: string,
    payloadHash: string,
    inboxContext?: InboxExecutionContext,
  ): Promise<ProcessWagerTransactionOutput> {
    return await this.em.transactional(async (txEm) => {
      const now = new Date();
      const byKey = await txEm.findOne(WagerTransactionRecord, {
        idempotencyKey,
      });
      if (byKey) {
        return await this.handleExistingTransaction(
          txEm,
          byKey,
          payloadHash,
          idempotencyKey,
          inboxContext,
          now,
        );
      }

      const byExternal = await txEm.findOne(WagerTransactionRecord, {
        providerId,
        externalTransactionId,
      });
      if (byExternal) {
        throw new IdempotencyConflictError(
          idempotencyKey,
          `External transaction '${providerId}:${externalTransactionId}' already exists with a different idempotency key`,
        );
      }

      throw new IdempotencyConflictError(idempotencyKey);
    });
  }

  private validateContractInput(input: ProcessWagerTransactionInput): void {
    if (!input || typeof input !== "object") {
      throw new InvalidPayloadError("Request body must be a valid JSON object");
    }
    if (
      typeof input.idempotencyKey !== "string" ||
      input.idempotencyKey.trim().length === 0
    ) {
      throw new InvalidPayloadError(
        "Idempotency-Key header is required",
        FailureCode.IDEMPOTENCY_KEY_REQUIRED,
      );
    }

    const requiredStrings: Array<[keyof ProcessWagerTransactionInput, string]> = [
      ["providerId", "providerId"],
      ["externalTransactionId", "externalTransactionId"],
      ["playerId", "playerId"],
      ["walletId", "walletId"],
      ["roundId", "roundId"],
      ["gameId", "gameId"],
      ["kind", "kind"],
    ];

    for (const [key, label] of requiredStrings) {
      const val = input[key];
      if (typeof val !== "string" || val.trim().length === 0) {
        throw new InvalidPayloadError(`Field '${label}' is required and must be a non-empty string`);
      }
    }

    const uuidRegex =
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    if (!uuidRegex.test(input.walletId.trim())) {
      throw new InvalidPayloadError("Field 'walletId' must be a valid UUID");
    }
  }

  private isPgUniqueViolation(err: unknown): boolean {
    if (!err || typeof err !== "object") return false;
    const pgErr = err as { code?: string };
    return pgErr.code === "23505";
  }
}
