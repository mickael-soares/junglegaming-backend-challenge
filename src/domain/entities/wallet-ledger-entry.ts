import { CurrencyMismatchError, InvalidMoneyError, LedgerImbalanceError } from "../errors/domain-errors";
import { Money } from "../value-objects/money";

export enum LedgerDirection {
  Debit = "DEBIT",
  Credit = "CREDIT",
}

export interface CreateLedgerEntryProps {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt?: Date;
}

export interface LedgerEntryState {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt: Date;
}

export class WalletLedgerEntry {
  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly createdAt: Date,
  ) {
    Object.freeze(this);
  }

  static create(props: CreateLedgerEntryProps): WalletLedgerEntry {
    if (!props.id || !props.walletId || !props.transactionId) {
      throw new LedgerImbalanceError("Ledger entry requires id, walletId, and transactionId");
    }

    if (
      props.money.currency !== props.balanceBefore.currency ||
      props.money.currency !== props.balanceAfter.currency
    ) {
      throw new CurrencyMismatchError(
        props.balanceBefore.currency,
        props.money.currency !== props.balanceBefore.currency
          ? props.money.currency
          : props.balanceAfter.currency,
      );
    }

    if (!props.money.isPositive()) {
      throw new InvalidMoneyError("Ledger entry money amount must be strictly positive");
    }

    if (props.balanceBefore.isNegative() || props.balanceAfter.isNegative()) {
      throw new LedgerImbalanceError("Ledger entry balances cannot be negative");
    }

    const entry = new WalletLedgerEntry(
      props.id,
      props.walletId,
      props.transactionId,
      props.direction,
      props.money,
      props.balanceBefore,
      props.balanceAfter,
      props.createdAt ?? new Date(),
    );

    if (!entry.isBalanced()) {
      throw new LedgerImbalanceError(
        `Ledger entry arithmetic mismatch: ${props.balanceBefore.toString()} ${
          props.direction === LedgerDirection.Credit ? "+" : "-"
        } ${props.money.toString()} !== ${props.balanceAfter.toString()}`,
      );
    }

    return entry;
  }

  static rehydrate(state: LedgerEntryState): WalletLedgerEntry {
    return new WalletLedgerEntry(
      state.id,
      state.walletId,
      state.transactionId,
      state.direction,
      state.money,
      state.balanceBefore,
      state.balanceAfter,
      state.createdAt,
    );
  }

  /** balanceBefore ± money === balanceAfter. Verificada na factory. */
  isBalanced(): boolean {
    if (
      this.money.currency !== this.balanceBefore.currency ||
      this.money.currency !== this.balanceAfter.currency
    ) {
      return false;
    }

    const expectedAfter =
      this.direction === LedgerDirection.Credit
        ? this.balanceBefore.add(this.money)
        : this.balanceBefore.subtract(this.money);

    return expectedAfter.equals(this.balanceAfter);
  }
}
