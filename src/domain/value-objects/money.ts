import Decimal from "decimal.js";
import { CurrencyMismatchError, InvalidMoneyError } from "../errors/domain-errors";

export interface MoneyProps {
  amount: string;   // decimal string, e.g. "25.00"
  currency: string; // ISO-4217, e.g. "BRL"
}

const CURRENCY_REGEX = /^[A-Z]{3}$/;
// Rejects scientific notation, NaN, Infinity, signs, spaces, and >2 decimal places.
const POSITIVE_DECIMAL_REGEX = /^\d+(\.\d{1,2})?$/;
const SIGNED_DECIMAL_REGEX = /^-?\d+(\.\d{1,2})?$/;

export class Money {
  private constructor(
    private readonly value: Decimal,
    public readonly currency: string,
  ) {
    Object.freeze(this);
  }

  /**
   * Validates and builds a Money instance from input contract props.
   * Rejects negative amounts, >2 decimal places, scientific notation, NaN, Infinity, empty strings.
   */
  static from(props: MoneyProps): Money {
    if (!props || typeof props !== "object") {
      throw new InvalidMoneyError("Money props must be a valid object");
    }

    const { amount, currency } = props;

    if (typeof currency !== "string" || !CURRENCY_REGEX.test(currency)) {
      throw new InvalidMoneyError(
        `Invalid ISO-4217 currency code: '${String(currency)}'`,
      );
    }

    if (typeof amount !== "string" || amount.length === 0) {
      throw new InvalidMoneyError("Money amount must be a non-empty decimal string");
    }

    if (!POSITIVE_DECIMAL_REGEX.test(amount)) {
      throw new InvalidMoneyError(
        `Invalid money amount '${amount}': must be a non-negative decimal string with up to 2 decimal places`,
      );
    }

    const decimalValue = new Decimal(amount);
    if (!decimalValue.isFinite() || decimalValue.isNaN() || decimalValue.isNegative()) {
      throw new InvalidMoneyError(`Invalid money amount '${amount}'`);
    }

    return new Money(decimalValue.toDecimalPlaces(2, Decimal.ROUND_HALF_UP), currency);
  }

  /**
   * Rehydrates persisted Money from database columns without re-running input contract rules,
   * while preserving exact 2-decimal scale representation.
   */
  static rehydrate(props: MoneyProps): Money {
    if (!props || typeof props.currency !== "string" || !CURRENCY_REGEX.test(props.currency)) {
      throw new InvalidMoneyError(`Invalid currency on rehydrate: '${String(props?.currency)}'`);
    }
    if (typeof props.amount !== "string" || !SIGNED_DECIMAL_REGEX.test(props.amount)) {
      throw new InvalidMoneyError(`Invalid amount on rehydrate: '${String(props?.amount)}'`);
    }
    return new Money(new Decimal(props.amount), props.currency);
  }

  static zero(currency: string): Money {
    if (typeof currency !== "string" || !CURRENCY_REGEX.test(currency)) {
      throw new InvalidMoneyError(`Invalid ISO-4217 currency code: '${String(currency)}'`);
    }
    return new Money(new Decimal("0.00"), currency);
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.plus(other.value), this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.minus(other.value), this.currency);
  }

  negate(): Money {
    if (this.value.isZero()) {
      return new Money(new Decimal("0.00"), this.currency);
    }
    return new Money(this.value.negated(), this.currency);
  }

  abs(): Money {
    return new Money(this.value.abs(), this.currency);
  }

  isZero(): boolean {
    return this.value.isZero();
  }

  isPositive(): boolean {
    return this.value.isPositive() && !this.value.isZero();
  }

  isNegative(): boolean {
    return this.value.isNegative() && !this.value.isZero();
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.lessThan(other.value);
  }

  isGreaterThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.greaterThan(other.value);
  }

  equals(other: Money): boolean {
    if (!other || this.currency !== other.currency) {
      return false;
    }
    return this.value.equals(other.value);
  }

  get amount(): string {
    return this.value.toFixed(2);
  }

  toJSON(): MoneyProps {
    return {
      amount: this.value.toFixed(2),
      currency: this.currency,
    };
  }

  toString(): string {
    return `${this.value.toFixed(2)} ${this.currency}`;
  }

  private assertSameCurrency(other: Money): void {
    if (!other || !(other instanceof Money)) {
      throw new InvalidMoneyError("Expected a valid Money instance");
    }
    if (this.currency !== other.currency) {
      throw new CurrencyMismatchError(this.currency, other.currency);
    }
  }
}
