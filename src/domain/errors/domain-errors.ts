export enum FailureCode {
  // Business Rule Rejections (persisted on WagerTransaction as REJECTED)
  INSUFFICIENT_FUNDS = "INSUFFICIENT_FUNDS",
  NEGATIVE_BALANCE_ON_REVERSAL = "NEGATIVE_BALANCE_ON_REVERSAL",
  CURRENCY_MISMATCH = "CURRENCY_MISMATCH",
  INVALID_REFERENCE_KIND = "INVALID_REFERENCE_KIND",
  REFERENCE_CONTEXT_MISMATCH = "REFERENCE_CONTEXT_MISMATCH",
  REFERENCE_AMOUNT_MISMATCH = "REFERENCE_AMOUNT_MISMATCH",
  REFERENCE_NOT_PROCESSED = "REFERENCE_NOT_PROCESSED",
  DUPLICATE_REVERSAL = "DUPLICATE_REVERSAL",
  REFERENCE_NOT_FOUND_TIMEOUT = "REFERENCE_NOT_FOUND_TIMEOUT",

  // Contract / Validation / Conflict Errors (HTTP 4xx / Permanent Rejections)
  INVALID_MONEY = "INVALID_MONEY",
  INVALID_PAYLOAD = "INVALID_PAYLOAD",
  MISSING_REFERENCE_ID = "MISSING_REFERENCE_ID",
  FORBIDDEN_OPENING_KIND = "FORBIDDEN_OPENING_KIND",
  IDEMPOTENCY_KEY_REQUIRED = "IDEMPOTENCY_KEY_REQUIRED",
  IDEMPOTENCY_PAYLOAD_CONFLICT = "IDEMPOTENCY_PAYLOAD_CONFLICT",
  EXTERNAL_TX_CONFLICT = "EXTERNAL_TX_CONFLICT",
  WALLET_NOT_FOUND = "WALLET_NOT_FOUND",
  TRANSACTION_NOT_FOUND = "TRANSACTION_NOT_FOUND",
  WALLET_ALREADY_EXISTS = "WALLET_ALREADY_EXISTS",
  INVALID_TRANSACTION_STATE = "INVALID_TRANSACTION_STATE",
  LEDGER_IMBALANCE = "LEDGER_IMBALANCE",

  // Infrastructure / Transient Errors
  CONCURRENCY_CONFLICT = "CONCURRENCY_CONFLICT",
  INFRASTRUCTURE_FAILURE = "INFRASTRUCTURE_FAILURE",
}

export class DomainError extends Error {
  public readonly code: FailureCode;

  constructor(code: FailureCode, message: string) {
    super(message);
    this.name = new.target.name;
    this.code = code;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class InvalidMoneyError extends DomainError {
  constructor(message: string) {
    super(FailureCode.INVALID_MONEY, message);
  }
}

export class CurrencyMismatchError extends DomainError {
  constructor(expected: string, received: string) {
    super(
      FailureCode.CURRENCY_MISMATCH,
      `Currency mismatch: expected ${expected}, received ${received}`,
    );
  }
}

export class InsufficientFundsError extends DomainError {
  constructor(walletId: string, balance: string, requested: string) {
    super(
      FailureCode.INSUFFICIENT_FUNDS,
      `Insufficient funds in wallet ${walletId}: balance is ${balance}, requested debit is ${requested}`,
    );
  }
}

export class NegativeBalanceReversalError extends DomainError {
  constructor(walletId: string, balance: string, reversalAmount: string) {
    super(
      FailureCode.NEGATIVE_BALANCE_ON_REVERSAL,
      `Reversal on wallet ${walletId} would produce negative balance: current balance ${balance}, reversal debit ${reversalAmount}`,
    );
  }
}

export class InvalidTransactionStateError extends DomainError {
  constructor(transactionId: string, currentStatus: string, targetStatus: string) {
    super(
      FailureCode.INVALID_TRANSACTION_STATE,
      `Cannot transition transaction ${transactionId} from ${currentStatus} to ${targetStatus}`,
    );
  }
}

export class InvalidPayloadError extends DomainError {
  constructor(message: string, code: FailureCode = FailureCode.INVALID_PAYLOAD) {
    super(code, message);
  }
}

export class IdempotencyConflictError extends DomainError {
  constructor(idempotencyKey: string, reason?: string) {
    super(
      FailureCode.IDEMPOTENCY_PAYLOAD_CONFLICT,
      reason ?? `Idempotency key '${idempotencyKey}' was already used with a different payload`,
    );
  }
}

export class DuplicateWalletError extends DomainError {
  constructor(playerId: string, currency: string) {
    super(
      FailureCode.WALLET_ALREADY_EXISTS,
      `Wallet already exists for playerId '${playerId}' and currency '${currency}'`,
    );
  }
}

export class EntityNotFoundError extends DomainError {
  constructor(code: FailureCode.WALLET_NOT_FOUND | FailureCode.TRANSACTION_NOT_FOUND, message: string) {
    super(code, message);
  }
}

export class LedgerImbalanceError extends DomainError {
  constructor(message: string) {
    super(FailureCode.LEDGER_IMBALANCE, message);
  }
}
