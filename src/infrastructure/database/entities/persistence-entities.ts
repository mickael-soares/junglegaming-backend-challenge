import { EntitySchema } from "@mikro-orm/core";
import { InboxMessage } from "../../../domain/entities/inbox-message";
import { OutboxMessage } from "../../../domain/entities/outbox-message";
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../../../domain/entities/wager-transaction";
import { Wallet } from "../../../domain/entities/wallet";
import {
  LedgerDirection,
  WalletLedgerEntry,
} from "../../../domain/entities/wallet-ledger-entry";
import { FailureCode } from "../../../domain/errors/domain-errors";
import { Money } from "../../../domain/value-objects/money";

const formatNumeric2 = (val: string | number): string => {
  if (typeof val === "number") return val.toFixed(2);
  if (val.includes(".")) {
    const [intPart, decPart] = val.split(".");
    return `${intPart}.${(decPart ?? "").padEnd(2, "0").slice(0, 2)}`;
  }
  return `${val}.00`;
};

export class WalletRecord {
  id!: string;
  playerId!: string;
  currency!: string;
  balanceAmount!: string;
  version!: number;
  createdAt!: Date;
  updatedAt!: Date;

  static fromDomain(wallet: Wallet): WalletRecord {
    const rec = new WalletRecord();
    rec.id = wallet.id;
    rec.playerId = wallet.playerId;
    rec.currency = wallet.currency;
    rec.balanceAmount = wallet.balance.amount;
    rec.version = wallet.version;
    rec.createdAt = wallet.createdAt;
    rec.updatedAt = wallet.updatedAt;
    return rec;
  }

  applyDomain(wallet: Wallet): void {
    this.balanceAmount = wallet.balance.amount;
    this.version = wallet.version;
    this.updatedAt = wallet.updatedAt;
  }

  toDomain(): Wallet {
    return Wallet.rehydrate({
      id: this.id,
      playerId: this.playerId,
      currency: this.currency.trim(),
      balance: Money.rehydrate({
        amount: formatNumeric2(this.balanceAmount),
        currency: this.currency.trim(),
      }),
      version: Number(this.version),
      createdAt: new Date(this.createdAt),
      updatedAt: new Date(this.updatedAt),
    });
  }
}

export const WalletEntitySchema = new EntitySchema<WalletRecord>({
  class: WalletRecord,
  tableName: "wallets",
  properties: {
    id: { type: "uuid", primary: true, fieldName: "id" },
    playerId: { type: "string", length: 128, fieldName: "player_id" },
    currency: { type: "string", length: 3, fieldName: "currency" },
    balanceAmount: {
      type: "decimal",
      precision: 20,
      scale: 2,
      fieldName: "balance_amount",
    },
    version: { type: "integer", fieldName: "version", default: 1 },
    createdAt: { type: "Date", fieldName: "created_at" },
    updatedAt: { type: "Date", fieldName: "updated_at" },
  },
});

export class WagerTransactionRecord {
  id!: string;
  providerId!: string;
  externalTransactionId!: string;
  idempotencyKey!: string;
  payloadHash!: string;
  walletId!: string;
  playerId!: string;
  roundId!: string;
  gameId!: string;
  kind!: string;
  moneyAmount!: string;
  moneyCurrency!: string;
  referenceExternalTransactionId?: string | null;
  referenceTransactionId?: string | null;
  status!: string;
  failureCode?: string | null;
  observedBalanceAmount?: string | null;
  observedBalanceCurrency?: string | null;
  retryAttempts!: number;
  nextRetryAt?: Date | null;
  createdAt!: Date;
  processedAt?: Date | null;

  static fromDomain(tx: WagerTransaction): WagerTransactionRecord {
    const rec = new WagerTransactionRecord();
    rec.id = tx.id;
    rec.providerId = tx.providerId;
    rec.externalTransactionId = tx.externalTransactionId;
    rec.idempotencyKey = tx.idempotencyKey;
    rec.payloadHash = tx.payloadHash;
    rec.walletId = tx.walletId;
    rec.playerId = tx.playerId;
    rec.roundId = tx.roundId;
    rec.gameId = tx.gameId;
    rec.kind = tx.kind;
    rec.moneyAmount = tx.money.amount;
    rec.moneyCurrency = tx.money.currency;
    rec.referenceExternalTransactionId =
      tx.referenceExternalTransactionId ?? null;
    rec.referenceTransactionId = tx.referenceTransactionId ?? null;
    rec.status = tx.status;
    rec.failureCode = tx.failureCode ?? null;
    rec.observedBalanceAmount = tx.observedBalance?.amount ?? null;
    rec.observedBalanceCurrency = tx.observedBalance?.currency ?? null;
    rec.retryAttempts = tx.retryAttempts;
    rec.nextRetryAt = tx.nextRetryAt ?? null;
    rec.createdAt = tx.createdAt;
    rec.processedAt = tx.processedAt ?? null;
    return rec;
  }

  applyDomain(tx: WagerTransaction): void {
    this.referenceTransactionId = tx.referenceTransactionId ?? null;
    this.status = tx.status;
    this.failureCode = tx.failureCode ?? null;
    this.observedBalanceAmount = tx.observedBalance?.amount ?? null;
    this.observedBalanceCurrency = tx.observedBalance?.currency ?? null;
    this.retryAttempts = tx.retryAttempts;
    this.nextRetryAt = tx.nextRetryAt ?? null;
    this.processedAt = tx.processedAt ?? null;
  }

  toDomain(): WagerTransaction {
    const currency = this.moneyCurrency.trim();
    const observedBalance =
      this.observedBalanceAmount !== null &&
      this.observedBalanceAmount !== undefined &&
      this.observedBalanceCurrency
        ? Money.rehydrate({
            amount: formatNumeric2(this.observedBalanceAmount),
            currency: this.observedBalanceCurrency.trim(),
          })
        : undefined;

    return WagerTransaction.rehydrate({
      id: this.id,
      providerId: this.providerId,
      externalTransactionId: this.externalTransactionId,
      idempotencyKey: this.idempotencyKey,
      payloadHash: this.payloadHash.trim(),
      walletId: this.walletId,
      playerId: this.playerId,
      roundId: this.roundId,
      gameId: this.gameId,
      kind: this.kind as WagerTransactionKind,
      money: Money.rehydrate({
        amount: formatNumeric2(this.moneyAmount),
        currency,
      }),
      referenceExternalTransactionId:
        this.referenceExternalTransactionId ?? undefined,
      createdAt: new Date(this.createdAt),
      status: this.status as WagerTransactionStatus,
      referenceTransactionId: this.referenceTransactionId ?? undefined,
      failureCode: (this.failureCode as FailureCode) ?? undefined,
      processedAt: this.processedAt ? new Date(this.processedAt) : undefined,
      observedBalance,
      retryAttempts: Number(this.retryAttempts ?? 0),
      nextRetryAt: this.nextRetryAt ? new Date(this.nextRetryAt) : undefined,
    });
  }
}

export const WagerTransactionEntitySchema =
  new EntitySchema<WagerTransactionRecord>({
    class: WagerTransactionRecord,
    tableName: "wager_transactions",
    properties: {
      id: { type: "uuid", primary: true, fieldName: "id" },
      providerId: { type: "string", length: 128, fieldName: "provider_id" },
      externalTransactionId: {
        type: "string",
        length: 255,
        fieldName: "external_transaction_id",
      },
      idempotencyKey: {
        type: "string",
        length: 255,
        fieldName: "idempotency_key",
      },
      payloadHash: { type: "string", length: 64, fieldName: "payload_hash" },
      walletId: { type: "uuid", fieldName: "wallet_id" },
      playerId: { type: "string", length: 128, fieldName: "player_id" },
      roundId: { type: "string", length: 255, fieldName: "round_id" },
      gameId: { type: "string", length: 255, fieldName: "game_id" },
      kind: { type: "string", length: 32, fieldName: "kind" },
      moneyAmount: {
        type: "decimal",
        precision: 20,
        scale: 2,
        fieldName: "money_amount",
      },
      moneyCurrency: { type: "string", length: 3, fieldName: "money_currency" },
      referenceExternalTransactionId: {
        type: "string",
        length: 255,
        nullable: true,
        fieldName: "reference_external_transaction_id",
      },
      referenceTransactionId: {
        type: "uuid",
        nullable: true,
        fieldName: "reference_transaction_id",
      },
      status: { type: "string", length: 32, fieldName: "status" },
      failureCode: {
        type: "string",
        length: 64,
        nullable: true,
        fieldName: "failure_code",
      },
      observedBalanceAmount: {
        type: "decimal",
        precision: 20,
        scale: 2,
        nullable: true,
        fieldName: "observed_balance_amount",
      },
      observedBalanceCurrency: {
        type: "string",
        length: 3,
        nullable: true,
        fieldName: "observed_balance_currency",
      },
      retryAttempts: {
        type: "integer",
        default: 0,
        fieldName: "retry_attempts",
      },
      nextRetryAt: {
        type: "Date",
        nullable: true,
        fieldName: "next_retry_at",
      },
      createdAt: { type: "Date", fieldName: "created_at" },
      processedAt: { type: "Date", nullable: true, fieldName: "processed_at" },
    },
  });

export class WalletLedgerEntryRecord {
  id!: string;
  walletId!: string;
  transactionId!: string;
  direction!: string;
  moneyAmount!: string;
  moneyCurrency!: string;
  balanceBeforeAmount!: string;
  balanceAfterAmount!: string;
  createdAt!: Date;
  sequenceNumber?: string | number;

  static fromDomain(entry: WalletLedgerEntry): WalletLedgerEntryRecord {
    const rec = new WalletLedgerEntryRecord();
    rec.id = entry.id;
    rec.walletId = entry.walletId;
    rec.transactionId = entry.transactionId;
    rec.direction = entry.direction;
    rec.moneyAmount = entry.money.amount;
    rec.moneyCurrency = entry.money.currency;
    rec.balanceBeforeAmount = entry.balanceBefore.amount;
    rec.balanceAfterAmount = entry.balanceAfter.amount;
    rec.createdAt = entry.createdAt;
    return rec;
  }

  toDomain(): WalletLedgerEntry {
    const currency = this.moneyCurrency.trim();
    return WalletLedgerEntry.rehydrate({
      id: this.id,
      walletId: this.walletId,
      transactionId: this.transactionId,
      direction: this.direction as LedgerDirection,
      money: Money.rehydrate({
        amount: formatNumeric2(this.moneyAmount),
        currency,
      }),
      balanceBefore: Money.rehydrate({
        amount: formatNumeric2(this.balanceBeforeAmount),
        currency,
      }),
      balanceAfter: Money.rehydrate({
        amount: formatNumeric2(this.balanceAfterAmount),
        currency,
      }),
      createdAt: new Date(this.createdAt),
    });
  }
}

export const WalletLedgerEntryEntitySchema =
  new EntitySchema<WalletLedgerEntryRecord>({
    class: WalletLedgerEntryRecord,
    tableName: "wallet_ledger_entries",
    properties: {
      id: { type: "uuid", primary: true, fieldName: "id" },
      walletId: { type: "uuid", fieldName: "wallet_id" },
      transactionId: { type: "uuid", fieldName: "transaction_id" },
      direction: { type: "string", length: 16, fieldName: "direction" },
      moneyAmount: {
        type: "decimal",
        precision: 20,
        scale: 2,
        fieldName: "money_amount",
      },
      moneyCurrency: { type: "string", length: 3, fieldName: "money_currency" },
      balanceBeforeAmount: {
        type: "decimal",
        precision: 20,
        scale: 2,
        fieldName: "balance_before_amount",
      },
      balanceAfterAmount: {
        type: "decimal",
        precision: 20,
        scale: 2,
        fieldName: "balance_after_amount",
      },
      createdAt: { type: "Date", fieldName: "created_at" },
      sequenceNumber: {
        type: "bigint",
        fieldName: "sequence_number",
        persist: false,
        nullable: true,
      },
    },
  });

export class InboxMessageRecord {
  consumerName!: string;
  messageId!: string;
  payloadHash!: string;
  receivedAt!: Date;
  processedAt?: Date | null;

  static fromDomain(inbox: InboxMessage): InboxMessageRecord {
    const rec = new InboxMessageRecord();
    rec.consumerName = inbox.consumerName;
    rec.messageId = inbox.messageId;
    rec.payloadHash = inbox.payloadHash;
    rec.receivedAt = inbox.receivedAt;
    rec.processedAt = inbox.processedAt ?? null;
    return rec;
  }

  applyDomain(inbox: InboxMessage): void {
    this.processedAt = inbox.processedAt ?? null;
  }

  toDomain(): InboxMessage {
    return InboxMessage.rehydrate({
      consumerName: this.consumerName,
      messageId: this.messageId,
      payloadHash: this.payloadHash.trim(),
      receivedAt: new Date(this.receivedAt),
      processedAt: this.processedAt ? new Date(this.processedAt) : undefined,
    });
  }
}

export const InboxMessageEntitySchema = new EntitySchema<InboxMessageRecord>({
  class: InboxMessageRecord,
  tableName: "inbox_messages",
  properties: {
    consumerName: {
      type: "string",
      length: 128,
      primary: true,
      fieldName: "consumer_name",
    },
    messageId: {
      type: "string",
      length: 255,
      primary: true,
      fieldName: "message_id",
    },
    payloadHash: { type: "string", length: 64, fieldName: "payload_hash" },
    receivedAt: { type: "Date", fieldName: "received_at" },
    processedAt: { type: "Date", nullable: true, fieldName: "processed_at" },
  },
});

export class OutboxMessageRecord {
  id!: string;
  aggregateId!: string;
  eventType!: string;
  payload!: Record<string, unknown>;
  occurredAt!: Date;
  attempts!: number;
  nextAttemptAt?: Date | null;
  publishedAt?: Date | null;

  static fromDomain(outbox: OutboxMessage): OutboxMessageRecord {
    const rec = new OutboxMessageRecord();
    rec.id = outbox.id;
    rec.aggregateId = outbox.aggregateId;
    rec.eventType = outbox.eventType;
    rec.payload = { ...outbox.payload };
    rec.occurredAt = outbox.occurredAt;
    rec.attempts = outbox.attempts;
    rec.nextAttemptAt = outbox.nextAttemptAt ?? null;
    rec.publishedAt = outbox.publishedAt ?? null;
    return rec;
  }

  applyDomain(outbox: OutboxMessage): void {
    this.attempts = outbox.attempts;
    this.nextAttemptAt = outbox.nextAttemptAt ?? null;
    this.publishedAt = outbox.publishedAt ?? null;
  }

  toDomain(): OutboxMessage {
    return OutboxMessage.rehydrate({
      id: this.id,
      aggregateId: this.aggregateId,
      eventType: this.eventType,
      payload:
        typeof this.payload === "string"
          ? JSON.parse(this.payload)
          : this.payload,
      occurredAt: new Date(this.occurredAt),
      attempts: Number(this.attempts),
      nextAttemptAt: this.nextAttemptAt
        ? new Date(this.nextAttemptAt)
        : undefined,
      publishedAt: this.publishedAt ? new Date(this.publishedAt) : undefined,
    });
  }
}

export const OutboxMessageEntitySchema = new EntitySchema<OutboxMessageRecord>({
  class: OutboxMessageRecord,
  tableName: "outbox_messages",
  properties: {
    id: { type: "uuid", primary: true, fieldName: "id" },
    aggregateId: { type: "string", length: 128, fieldName: "aggregate_id" },
    eventType: { type: "string", length: 128, fieldName: "event_type" },
    payload: { type: "json", fieldName: "payload" },
    occurredAt: { type: "Date", fieldName: "occurred_at" },
    attempts: { type: "integer", default: 0, fieldName: "attempts" },
    nextAttemptAt: {
      type: "Date",
      nullable: true,
      fieldName: "next_attempt_at",
    },
    publishedAt: { type: "Date", nullable: true, fieldName: "published_at" },
  },
});

export const ALL_ENTITY_SCHEMAS = [
  WalletEntitySchema,
  WagerTransactionEntitySchema,
  WalletLedgerEntryEntitySchema,
  InboxMessageEntitySchema,
  OutboxMessageEntitySchema,
];
