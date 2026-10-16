import { randomUUID } from "node:crypto";
import {
  CurrencyMismatchError,
  InsufficientFundsError,
  InvalidMoneyError,
  InvalidPayloadError,
  NegativeBalanceReversalError,
} from "../errors/domain-errors";
import { Money } from "../value-objects/money";
import { LedgerDirection, WalletLedgerEntry } from "./wallet-ledger-entry";

export interface OpenWalletProps {
  id: string;
  playerId: string;
  initialBalance: Money;
  createdAt?: Date;
}

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface WalletMovementParams {
  entryId?: string;
  transactionId: string;
  amount: Money;
  isReversal?: boolean;
  at?: Date;
}

export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static open(props: OpenWalletProps): Wallet {
    if (!props.id || typeof props.id !== "string") {
      throw new InvalidPayloadError("Wallet id is required");
    }
    if (!props.playerId || typeof props.playerId !== "string") {
      throw new InvalidPayloadError("Wallet playerId is required");
    }
    if (!props.initialBalance || !(props.initialBalance instanceof Money)) {
      throw new InvalidMoneyError("Initial balance must be a valid Money instance");
    }
    if (props.initialBalance.isNegative()) {
      throw new InvalidMoneyError("Wallet initial balance cannot be negative");
    }

    const now = props.createdAt ?? new Date();
    return new Wallet(
      props.id,
      props.playerId,
      props.initialBalance.currency,
      props.initialBalance,
      1,
      now,
      now,
    );
  }

  /** Reconstrução a partir da persistência — não revalida transições. */
  static rehydrate(state: WalletState): Wallet {
    return new Wallet(
      state.id,
      state.playerId,
      state.currency,
      state.balance,
      state.version,
      state.createdAt,
      state.updatedAt,
    );
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  /**
   * Debits the wallet and generates the corresponding immutable DEBIT ledger entry.
   * Increments wallet version and guarantees non-negative balance.
   */
  debit(params: WalletMovementParams): WalletLedgerEntry {
    this.assertSameCurrency(params.amount);

    if (!params.amount.isPositive()) {
      throw new InvalidMoneyError("Debit amount must be strictly positive");
    }

    if (this._balance.isLessThan(params.amount)) {
      if (params.isReversal) {
        throw new NegativeBalanceReversalError(
          this.id,
          this._balance.toString(),
          params.amount.toString(),
        );
      }
      throw new InsufficientFundsError(
        this.id,
        this._balance.toString(),
        params.amount.toString(),
      );
    }

    const balanceBefore = this._balance;
    const balanceAfter = this._balance.subtract(params.amount);
    const now = params.at ?? new Date();

    this._balance = balanceAfter;
    this._version += 1;
    this._updatedAt = now;

    return WalletLedgerEntry.create({
      id: params.entryId ?? randomUUID(),
      walletId: this.id,
      transactionId: params.transactionId,
      direction: LedgerDirection.Debit,
      money: params.amount,
      balanceBefore,
      balanceAfter,
      createdAt: now,
    });
  }

  /**
   * Credits the wallet and generates the corresponding immutable CREDIT ledger entry.
   * Increments wallet version.
   */
  credit(params: WalletMovementParams): WalletLedgerEntry {
    this.assertSameCurrency(params.amount);

    if (!params.amount.isPositive()) {
      throw new InvalidMoneyError("Credit amount must be strictly positive");
    }

    const balanceBefore = this._balance;
    const balanceAfter = this._balance.add(params.amount);
    const now = params.at ?? new Date();

    this._balance = balanceAfter;
    this._version += 1;
    this._updatedAt = now;

    return WalletLedgerEntry.create({
      id: params.entryId ?? randomUUID(),
      walletId: this.id,
      transactionId: params.transactionId,
      direction: LedgerDirection.Credit,
      money: params.amount,
      balanceBefore,
      balanceAfter,
      createdAt: now,
    });
  }

  private assertSameCurrency(money: Money): void {
    if (!money || !(money instanceof Money)) {
      throw new InvalidMoneyError("Expected a valid Money instance");
    }
    if (this.currency !== money.currency) {
      throw new CurrencyMismatchError(this.currency, money.currency);
    }
  }
}
