import {
  DomainError,
  FailureCode,
  InvalidMoneyError,
  InvalidPayloadError,
  InvalidTransactionStateError,
} from "../errors/domain-errors";
import { Money } from "../value-objects/money";
import { LedgerDirection } from "./wallet-ledger-entry";

export enum WagerTransactionKind {
  Opening  = "OPENING",   // interno: crédito de abertura da wallet
  Bet      = "BET",
  Win      = "WIN",
  Loss     = "LOSS",
  Refund   = "REFUND",
  Rollback = "ROLLBACK",
}

export enum WagerTransactionStatus {
  Pending          = "PENDING",            // aceita, ainda não aplicada
  PendingReference = "PENDING_REFERENCE",  // aguardando a transação referenciada
  Processed        = "PROCESSED",          // aplicada (terminal)
  Rejected         = "REJECTED",           // violação de regra de negócio (terminal)
  Failed           = "FAILED",             // erro permanente de infraestrutura (terminal, auditável)
}

export interface CreateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  allowOpeningKind?: boolean;
  createdAt?: Date;
}

export interface WagerTransactionState {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId?: string;
  createdAt: Date;
  status: WagerTransactionStatus;
  referenceTransactionId?: string;
  failureCode?: FailureCode;
  processedAt?: Date;
  observedBalance?: Money;
  retryAttempts?: number;
  nextRetryAt?: Date;
}

export class WagerTransaction {
  private constructor(
    public readonly id: string,
    public readonly providerId: string,
    public readonly externalTransactionId: string,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string,
    public readonly gameId: string,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    /** id no provedor — não o id interno */
    public readonly referenceExternalTransactionId: string | undefined,
    public readonly createdAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId?: string,
    private _failureCode?: FailureCode,
    private _processedAt?: Date,
    private _observedBalance?: Money,
    private _retryAttempts: number = 0,
    private _nextRetryAt?: Date,
  ) {}

  /** Nasce em PENDING. Valida a exigência de referência por kind. */
  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if (props.kind === WagerTransactionKind.Opening && !props.allowOpeningKind) {
      throw new InvalidPayloadError(
        "OPENING transaction kind is strictly internal and cannot be submitted externally",
        FailureCode.FORBIDDEN_OPENING_KIND,
      );
    }

    if (!Object.values(WagerTransactionKind).includes(props.kind)) {
      throw new InvalidPayloadError(`Unsupported transaction kind: '${String(props.kind)}'`);
    }

    if (
      (props.kind === WagerTransactionKind.Refund ||
        props.kind === WagerTransactionKind.Rollback) &&
      (!props.referenceExternalTransactionId ||
        props.referenceExternalTransactionId.trim().length === 0)
    ) {
      throw new InvalidPayloadError(
        `${props.kind} requires referenceExternalTransactionId`,
        FailureCode.MISSING_REFERENCE_ID,
      );
    }

    if (
      props.kind !== WagerTransactionKind.Loss &&
      !props.money.isPositive()
    ) {
      throw new InvalidMoneyError(
        `Transaction amount for kind ${props.kind} must be strictly greater than zero`,
      );
    }

    return new WagerTransaction(
      props.id,
      props.providerId,
      props.externalTransactionId,
      props.idempotencyKey,
      props.payloadHash,
      props.walletId,
      props.playerId,
      props.roundId,
      props.gameId,
      props.kind,
      props.money,
      props.referenceExternalTransactionId,
      props.createdAt ?? new Date(),
      WagerTransactionStatus.Pending,
    );
  }

  static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      state.id,
      state.providerId,
      state.externalTransactionId,
      state.idempotencyKey,
      state.payloadHash,
      state.walletId,
      state.playerId,
      state.roundId,
      state.gameId,
      state.kind,
      state.money,
      state.referenceExternalTransactionId,
      state.createdAt,
      state.status,
      state.referenceTransactionId,
      state.failureCode,
      state.processedAt,
      state.observedBalance,
      state.retryAttempts ?? 0,
      state.nextRetryAt,
    );
  }

  get status(): WagerTransactionStatus {
    return this._status;
  }

  get referenceTransactionId(): string | undefined {
    return this._referenceTransactionId;
  }

  get failureCode(): FailureCode | undefined {
    return this._failureCode;
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  get observedBalance(): Money | undefined {
    return this._observedBalance;
  }

  get retryAttempts(): number {
    return this._retryAttempts;
  }

  get nextRetryAt(): Date | undefined {
    return this._nextRetryAt;
  }

  // ---- transições (lançam InvalidTransactionStateError se o estado atual for terminal)

  markProcessed(
    referenceTransactionId: string | undefined,
    at: Date,
    observedBalance?: Money,
  ): void {
    this.assertNotTerminal(WagerTransactionStatus.Processed);
    this._status = WagerTransactionStatus.Processed;
    this._referenceTransactionId = referenceTransactionId;
    this._processedAt = at;
    if (observedBalance) {
      this._observedBalance = observedBalance;
    }
    this._nextRetryAt = undefined;
  }

  markPendingReference(nextRetryAt?: Date): void {
    this.assertNotTerminal(WagerTransactionStatus.PendingReference);
    this._status = WagerTransactionStatus.PendingReference;
    this._nextRetryAt = nextRetryAt ?? new Date(Date.now() + 100);
  }

  scheduleReferenceRetry(now: Date, baseDelayMs: number = 100): void {
    this.assertNotTerminal(WagerTransactionStatus.PendingReference);
    this._status = WagerTransactionStatus.PendingReference;
    this._retryAttempts += 1;
    const delayMs = baseDelayMs * Math.pow(2, Math.max(0, this._retryAttempts - 1));
    this._nextRetryAt = new Date(now.getTime() + delayMs);
  }

  reject(code: FailureCode, at: Date = new Date(), observedBalance?: Money): void {
    this.assertNotTerminal(WagerTransactionStatus.Rejected);
    this._status = WagerTransactionStatus.Rejected;
    this._failureCode = code;
    this._processedAt = at;
    if (observedBalance) {
      this._observedBalance = observedBalance;
    }
    this._nextRetryAt = undefined;
  }

  fail(code: FailureCode, at: Date = new Date(), observedBalance?: Money): void {
    this.assertNotTerminal(WagerTransactionStatus.Failed);
    this._status = WagerTransactionStatus.Failed;
    this._failureCode = code;
    this._processedAt = at;
    if (observedBalance) {
      this._observedBalance = observedBalance;
    }
    this._nextRetryAt = undefined;
  }

  // ---- consultas de domínio

  isTerminal(): boolean {
    return (
      this._status === WagerTransactionStatus.Processed ||
      this._status === WagerTransactionStatus.Rejected ||
      this._status === WagerTransactionStatus.Failed
    );
  }

  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;
  }

  requiresReference(): boolean {
    return (
      this.kind === WagerTransactionKind.Refund ||
      this.kind === WagerTransactionKind.Rollback
    );
  }

  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
      case WagerTransactionKind.Opening:
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
        return LedgerDirection.Credit;
      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;
      case WagerTransactionKind.Rollback: {
        if (!reference) {
          throw new DomainError(
            FailureCode.MISSING_REFERENCE_ID,
            "ROLLBACK requires reference transaction to determine ledger direction",
          );
        }
        const refDirection = reference.ledgerDirectionFor();
        return refDirection === LedgerDirection.Debit
          ? LedgerDirection.Credit
          : LedgerDirection.Debit;
      }
      case WagerTransactionKind.Loss:
        throw new DomainError(
          FailureCode.INVALID_TRANSACTION_STATE,
          "LOSS transactions do not affect balance or produce ledger entries",
        );
    }
  }

  private assertNotTerminal(targetStatus: WagerTransactionStatus): void {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(this.id, this._status, targetStatus);
    }
  }
}
