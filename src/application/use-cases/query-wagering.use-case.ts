import { EntityManager } from "@mikro-orm/postgresql";
import { Inject, Injectable } from "@nestjs/common";
import {
  EntityNotFoundError,
  FailureCode,
  InvalidPayloadError,
} from "../../domain/errors/domain-errors";
import { MoneyProps } from "../../domain/value-objects/money";
import {
  WagerTransactionRecord,
  WalletLedgerEntryRecord,
  WalletRecord,
} from "../../infrastructure/database/entities/persistence-entities";

export interface WalletDetailsDto {
  id: string;
  playerId: string;
  balance: MoneyProps;
  version: number;
  createdAt: string;
  updatedAt: string;
}

export interface LedgerEntryDto {
  id: string;
  walletId: string;
  transactionId: string;
  direction: string;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  createdAt: string;
}

export interface PaginatedLedgerDto {
  walletId: string;
  items: LedgerEntryDto[];
  nextCursor: string | null;
  hasMore: boolean;
}

export interface TransactionDetailsDto {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: string;
  status: string;
  money: MoneyProps;
  balance?: MoneyProps;
  referenceExternalTransactionId?: string;
  referenceTransactionId?: string;
  failureCode?: string;
  createdAt: string;
  processedAt?: string;
}

const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

@Injectable()
export class QueryWageringUseCase {
  constructor(@Inject(EntityManager) private readonly em: EntityManager) {}

  async getWalletById(walletId: string): Promise<WalletDetailsDto> {
    if (!walletId || !UUID_REGEX.test(walletId)) {
      throw new InvalidPayloadError("walletId must be a valid UUID");
    }

    const record = await this.em.findOne(WalletRecord, { id: walletId });
    if (!record) {
      throw new EntityNotFoundError(
        FailureCode.WALLET_NOT_FOUND,
        `Wallet '${walletId}' not found`,
      );
    }

    const wallet = record.toDomain();
    return {
      id: wallet.id,
      playerId: wallet.playerId,
      balance: wallet.balance.toJSON(),
      version: wallet.version,
      createdAt: wallet.createdAt.toISOString(),
      updatedAt: wallet.updatedAt.toISOString(),
    };
  }

  async getWalletLedger(params: {
    walletId: string;
    cursor?: string;
    limit?: number;
  }): Promise<PaginatedLedgerDto> {
    const { walletId } = params;
    if (!walletId || !UUID_REGEX.test(walletId)) {
      throw new InvalidPayloadError("walletId must be a valid UUID");
    }

    const walletExists = await this.em.findOne(WalletRecord, { id: walletId });
    if (!walletExists) {
      throw new EntityNotFoundError(
        FailureCode.WALLET_NOT_FOUND,
        `Wallet '${walletId}' not found`,
      );
    }

    const limit = Math.min(Math.max(Number(params.limit ?? 50), 1), 200);
    let afterSeq = 0n;

    if (params.cursor) {
      afterSeq = this.decodeCursor(params.cursor);
    }

    const records = await this.em.find(
      WalletLedgerEntryRecord,
      {
        walletId,
        ...(afterSeq > 0n ? { sequenceNumber: { $gt: afterSeq.toString() } } : {}),
      },
      {
        orderBy: { sequenceNumber: "ASC" },
        limit: limit + 1,
      },
    );

    const hasMore = records.length > limit;
    const pageRecords = hasMore ? records.slice(0, limit) : records;

    const items: LedgerEntryDto[] = pageRecords.map((rec) => {
      const domain = rec.toDomain();
      return {
        id: domain.id,
        walletId: domain.walletId,
        transactionId: domain.transactionId,
        direction: domain.direction,
        money: domain.money.toJSON(),
        balanceBefore: domain.balanceBefore.toJSON(),
        balanceAfter: domain.balanceAfter.toJSON(),
        createdAt: domain.createdAt.toISOString(),
      };
    });

    const lastRecord = pageRecords[pageRecords.length - 1];
    const nextCursor =
      hasMore && lastRecord && lastRecord.sequenceNumber !== undefined
        ? this.encodeCursor(BigInt(lastRecord.sequenceNumber))
        : null;

    return {
      walletId,
      items,
      nextCursor,
      hasMore,
    };
  }

  async getTransactionById(transactionId: string): Promise<TransactionDetailsDto> {
    if (!transactionId || !UUID_REGEX.test(transactionId)) {
      throw new InvalidPayloadError("transactionId must be a valid UUID");
    }

    const record = await this.em.findOne(WagerTransactionRecord, {
      id: transactionId,
    });
    if (!record) {
      throw new EntityNotFoundError(
        FailureCode.TRANSACTION_NOT_FOUND,
        `Transaction '${transactionId}' not found`,
      );
    }

    return this.toTransactionDto(record);
  }

  async getTransactionByProviderAndExternalId(
    providerId: string,
    externalTransactionId: string,
  ): Promise<TransactionDetailsDto> {
    if (!providerId || !externalTransactionId) {
      throw new InvalidPayloadError(
        "providerId and externalTransactionId are required",
      );
    }

    const record = await this.em.findOne(WagerTransactionRecord, {
      providerId,
      externalTransactionId,
    });
    if (!record) {
      throw new EntityNotFoundError(
        FailureCode.TRANSACTION_NOT_FOUND,
        `Transaction '${providerId}:${externalTransactionId}' not found`,
      );
    }

    return this.toTransactionDto(record);
  }

  private toTransactionDto(record: WagerTransactionRecord): TransactionDetailsDto {
    const tx = record.toDomain();
    return {
      id: tx.id,
      providerId: tx.providerId,
      externalTransactionId: tx.externalTransactionId,
      idempotencyKey: tx.idempotencyKey,
      walletId: tx.walletId,
      playerId: tx.playerId,
      roundId: tx.roundId,
      gameId: tx.gameId,
      kind: tx.kind,
      status: tx.status,
      money: tx.money.toJSON(),
      ...(tx.observedBalance ? { balance: tx.observedBalance.toJSON() } : {}),
      ...(tx.referenceExternalTransactionId
        ? { referenceExternalTransactionId: tx.referenceExternalTransactionId }
        : {}),
      ...(tx.referenceTransactionId
        ? { referenceTransactionId: tx.referenceTransactionId }
        : {}),
      ...(tx.failureCode ? { failureCode: tx.failureCode } : {}),
      createdAt: tx.createdAt.toISOString(),
      ...(tx.processedAt ? { processedAt: tx.processedAt.toISOString() } : {}),
    };
  }

  private encodeCursor(sequenceNumber: bigint): string {
    const payload = JSON.stringify({ seq: sequenceNumber.toString() });
    return Buffer.from(payload, "utf8").toString("base64url");
  }

  private decodeCursor(cursor: string): bigint {
    try {
      const json = Buffer.from(cursor, "base64url").toString("utf8");
      const parsed = JSON.parse(json) as { seq?: string };
      if (!parsed || typeof parsed.seq !== "string" || !/^\d+$/.test(parsed.seq)) {
        throw new Error("Invalid seq");
      }
      return BigInt(parsed.seq);
    } catch {
      throw new InvalidPayloadError("Invalid pagination cursor");
    }
  }
}
