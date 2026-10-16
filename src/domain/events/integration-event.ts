import { randomUUID } from "node:crypto";
import { LedgerDirection, WalletLedgerEntry } from "../entities/wallet-ledger-entry";
import { Wallet } from "../entities/wallet";
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../entities/wager-transaction";
import { FailureCode } from "../errors/domain-errors";
import { MoneyProps } from "../value-objects/money";

export interface IntegrationEventProps<T> {
  eventId: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: Date;
  data: T;
}

export interface EventContext {
  eventId?: string;
  correlationId: string;
  causationId?: string;
  occurredAt?: Date;
}

export interface SerializedIntegrationEvent<T = unknown> {
  eventId: string;
  eventType: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: string; // ISO-8601
  version: number;
  data: T;
}

export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;

  readonly eventId: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId?: string;
  readonly occurredAt: Date;
  readonly data: Readonly<T>;

  protected constructor(props: IntegrationEventProps<T>) {
    this.eventId = props.eventId;
    this.aggregateId = props.aggregateId;
    this.correlationId = props.correlationId;
    this.causationId = props.causationId;
    this.occurredAt = props.occurredAt;
    this.data = Object.freeze({ ...props.data });
  }

  /** Envelope serializado gravado no payload da outbox. */
  toJSON(): SerializedIntegrationEvent<T> {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      ...(this.causationId ? { causationId: this.causationId } : {}),
      occurredAt: this.occurredAt.toISOString(),
      version: this.version,
      data: this.data,
    };
  }
}

// ---------------------------------------------------------------------------
// 1. WalletBalanceChanged
// ---------------------------------------------------------------------------
export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = "WalletBalanceChanged";
  readonly version = 1;

  static from(
    wallet: Wallet,
    entry: WalletLedgerEntry,
    ctx: EventContext,
  ): WalletBalanceChanged {
    return new WalletBalanceChanged({
      eventId: ctx.eventId ?? randomUUID(),
      aggregateId: wallet.id,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: ctx.occurredAt ?? new Date(),
      data: {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: wallet.version,
      },
    });
  }
}

// ---------------------------------------------------------------------------
// 2. WagerTransactionProcessed
// ---------------------------------------------------------------------------
export interface WagerTransactionProcessedData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  status: WagerTransactionStatus.Processed;
  money: MoneyProps;
  balance: MoneyProps;
  referenceTransactionId?: string;
  referenceExternalTransactionId?: string;
  processedAt: string;
}

export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionProcessedData> {
  readonly eventType = "WagerTransactionProcessed";
  readonly version = 1;

  static from(
    transaction: WagerTransaction,
    observedBalance: MoneyProps,
    ctx: EventContext,
  ): WagerTransactionProcessed {
    const now = ctx.occurredAt ?? transaction.processedAt ?? new Date();
    return new WagerTransactionProcessed({
      eventId: ctx.eventId ?? randomUUID(),
      aggregateId: transaction.walletId,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: now,
      data: {
        transactionId: transaction.id,
        providerId: transaction.providerId,
        externalTransactionId: transaction.externalTransactionId,
        walletId: transaction.walletId,
        playerId: transaction.playerId,
        roundId: transaction.roundId,
        gameId: transaction.gameId,
        kind: transaction.kind,
        status: WagerTransactionStatus.Processed,
        money: transaction.money.toJSON(),
        balance: observedBalance,
        ...(transaction.referenceTransactionId
          ? { referenceTransactionId: transaction.referenceTransactionId }
          : {}),
        ...(transaction.referenceExternalTransactionId
          ? { referenceExternalTransactionId: transaction.referenceExternalTransactionId }
          : {}),
        processedAt: (transaction.processedAt ?? now).toISOString(),
      },
    });
  }
}

// ---------------------------------------------------------------------------
// 3. WagerTransactionRejected
// ---------------------------------------------------------------------------
export interface WagerTransactionRejectedData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  status: WagerTransactionStatus.Rejected;
  failureCode: FailureCode;
  money: MoneyProps;
  balance: MoneyProps;
  referenceExternalTransactionId?: string;
  rejectedAt: string;
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionRejectedData> {
  readonly eventType = "WagerTransactionRejected";
  readonly version = 1;

  static from(
    transaction: WagerTransaction,
    observedBalance: MoneyProps,
    ctx: EventContext,
  ): WagerTransactionRejected {
    const now = ctx.occurredAt ?? transaction.processedAt ?? new Date();
    return new WagerTransactionRejected({
      eventId: ctx.eventId ?? randomUUID(),
      aggregateId: transaction.walletId,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: now,
      data: {
        transactionId: transaction.id,
        providerId: transaction.providerId,
        externalTransactionId: transaction.externalTransactionId,
        walletId: transaction.walletId,
        playerId: transaction.playerId,
        roundId: transaction.roundId,
        gameId: transaction.gameId,
        kind: transaction.kind,
        status: WagerTransactionStatus.Rejected,
        failureCode: transaction.failureCode ?? FailureCode.INVALID_PAYLOAD,
        money: transaction.money.toJSON(),
        balance: observedBalance,
        ...(transaction.referenceExternalTransactionId
          ? { referenceExternalTransactionId: transaction.referenceExternalTransactionId }
          : {}),
        rejectedAt: (transaction.processedAt ?? now).toISOString(),
      },
    });
  }
}

// ---------------------------------------------------------------------------
// 4. WagerTransactionPendingReference
// ---------------------------------------------------------------------------
export interface WagerTransactionPendingReferenceData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  status: WagerTransactionStatus.PendingReference;
  money: MoneyProps;
  referenceExternalTransactionId: string;
}

export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionPendingReferenceData> {
  readonly eventType = "WagerTransactionPendingReference";
  readonly version = 1;

  static from(
    transaction: WagerTransaction,
    ctx: EventContext,
  ): WagerTransactionPendingReference {
    const now = ctx.occurredAt ?? new Date();
    return new WagerTransactionPendingReference({
      eventId: ctx.eventId ?? randomUUID(),
      aggregateId: transaction.walletId,
      correlationId: ctx.correlationId,
      causationId: ctx.causationId,
      occurredAt: now,
      data: {
        transactionId: transaction.id,
        providerId: transaction.providerId,
        externalTransactionId: transaction.externalTransactionId,
        walletId: transaction.walletId,
        playerId: transaction.playerId,
        roundId: transaction.roundId,
        gameId: transaction.gameId,
        kind: transaction.kind,
        status: WagerTransactionStatus.PendingReference,
        money: transaction.money.toJSON(),
        referenceExternalTransactionId: transaction.referenceExternalTransactionId ?? "",
      },
    });
  }
}
