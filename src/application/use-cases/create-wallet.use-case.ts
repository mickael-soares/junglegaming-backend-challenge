import { EntityManager } from "@mikro-orm/postgresql";
import { Inject, Injectable } from "@nestjs/common";
import { randomUUID } from "node:crypto";
import { OutboxMessage } from "../../domain/entities/outbox-message";
import {
  WagerTransaction,
  WagerTransactionKind,
} from "../../domain/entities/wager-transaction";
import { Wallet } from "../../domain/entities/wallet";
import {
  LedgerDirection,
  WalletLedgerEntry,
} from "../../domain/entities/wallet-ledger-entry";
import {
  DuplicateWalletError,
  InvalidPayloadError,
} from "../../domain/errors/domain-errors";
import {
  WagerTransactionProcessed,
  WalletBalanceChanged,
} from "../../domain/events/integration-event";
import { computeWagerPayloadHash } from "../../domain/utils/canonical-json";
import { Money, MoneyProps } from "../../domain/value-objects/money";
import {
  OutboxMessageRecord,
  WagerTransactionRecord,
  WalletLedgerEntryRecord,
  WalletRecord,
} from "../../infrastructure/database/entities/persistence-entities";
import { StructuredLogger } from "../../infrastructure/observability/logger.service";
import { MetricsService } from "../../infrastructure/observability/metrics.service";

export interface CreateWalletInput {
  id?: string;
  playerId: string;
  initialBalance: MoneyProps;
  correlationId?: string;
}

export interface CreateWalletOutput {
  id: string;
  playerId: string;
  balance: MoneyProps;
  version: number;
}

@Injectable()
export class CreateWalletUseCase {
  constructor(
    @Inject(EntityManager) private readonly em: EntityManager,
    @Inject(StructuredLogger) private readonly logger: StructuredLogger,
    @Inject(MetricsService) private readonly metrics: MetricsService,
  ) {}

  async execute(input: CreateWalletInput): Promise<CreateWalletOutput> {
    if (!input || typeof input.playerId !== "string" || input.playerId.trim().length === 0) {
      throw new InvalidPayloadError("playerId is required and must be a non-empty string");
    }

    const playerId = input.playerId.trim();
    const initialMoney = Money.from(input.initialBalance);
    const walletId = input.id ?? randomUUID();
    const correlationId = input.correlationId ?? randomUUID();
    const now = new Date();

    try {
      const wallet = await this.em.transactional(async (txEm) => {
        const existing = await txEm.findOne(WalletRecord, {
          playerId,
          currency: initialMoney.currency,
        });

        if (existing) {
          throw new DuplicateWalletError(playerId, initialMoney.currency);
        }

        const domainWallet = Wallet.open({
          id: walletId,
          playerId,
          initialBalance: initialMoney,
          createdAt: now,
        });

        const walletRecord = WalletRecord.fromDomain(domainWallet);
        txEm.persist(walletRecord);
        await txEm.flush();

        if (initialMoney.isPositive()) {
          const openingTxId = randomUUID();
          const externalTransactionId = `opening:${walletId}`;
          const idempotencyKey = `system:${externalTransactionId}`;
          const payloadHash = computeWagerPayloadHash({
            providerId: "system",
            externalTransactionId,
            playerId,
            walletId,
            roundId: `opening-round:${walletId}`,
            gameId: "wallet-opening",
            kind: WagerTransactionKind.Opening,
            money: initialMoney.toJSON(),
          });

          const openingTx = WagerTransaction.create({
            id: openingTxId,
            providerId: "system",
            externalTransactionId,
            idempotencyKey,
            payloadHash,
            walletId,
            playerId,
            roundId: `opening-round:${walletId}`,
            gameId: "wallet-opening",
            kind: WagerTransactionKind.Opening,
            money: initialMoney,
            allowOpeningKind: true,
            createdAt: now,
          });

          openingTx.markProcessed(undefined, now, initialMoney);

          const openingLedgerEntry = WalletLedgerEntry.create({
            id: randomUUID(),
            walletId,
            transactionId: openingTx.id,
            direction: LedgerDirection.Credit,
            money: initialMoney,
            balanceBefore: Money.zero(initialMoney.currency),
            balanceAfter: initialMoney,
            createdAt: now,
          });

          const balanceChangedEvent = WalletBalanceChanged.from(
            domainWallet,
            openingLedgerEntry,
            { correlationId, occurredAt: now },
          );
          const txProcessedEvent = WagerTransactionProcessed.from(
            openingTx,
            domainWallet.balance.toJSON(),
            { correlationId, occurredAt: now },
          );

          txEm.persist(WagerTransactionRecord.fromDomain(openingTx));
          await txEm.flush();

          txEm.persist(WalletLedgerEntryRecord.fromDomain(openingLedgerEntry));
          txEm.persist(
            OutboxMessageRecord.fromDomain(OutboxMessage.enqueue(balanceChangedEvent)),
          );
          txEm.persist(
            OutboxMessageRecord.fromDomain(OutboxMessage.enqueue(txProcessedEvent)),
          );
          await txEm.flush();

          this.metrics.recordTransaction(
            WagerTransactionKind.Opening,
            openingTx.status,
          );
        }

        return domainWallet;
      });

      this.logger.info("Wallet opened successfully", {
        correlationId,
        walletId: wallet.id,
        playerId: wallet.playerId,
      });

      return {
        id: wallet.id,
        playerId: wallet.playerId,
        balance: wallet.balance.toJSON(),
        version: wallet.version,
      };
    } catch (err: unknown) {
      if (this.isUniqueConstraintViolation(err, "uq_wallets_player_currency")) {
        throw new DuplicateWalletError(playerId, initialMoney.currency);
      }
      throw err;
    }
  }

  private isUniqueConstraintViolation(err: unknown, constraintName?: string): boolean {
    if (!err || typeof err !== "object") return false;
    const pgErr = err as { code?: string; constraint?: string; message?: string };
    if (pgErr.code === "23505") {
      if (!constraintName) return true;
      return (
        pgErr.constraint === constraintName ||
        Boolean(pgErr.message?.includes(constraintName))
      );
    }
    return false;
  }
}
