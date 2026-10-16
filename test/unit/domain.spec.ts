import { describe, expect, it } from "bun:test";
import { randomUUID } from "node:crypto";
import { InboxMessage } from "../../src/domain/entities/inbox-message";
import { OutboxMessage } from "../../src/domain/entities/outbox-message";
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../../src/domain/entities/wager-transaction";
import { Wallet } from "../../src/domain/entities/wallet";
import {
  LedgerDirection,
  WalletLedgerEntry,
} from "../../src/domain/entities/wallet-ledger-entry";
import {
  CurrencyMismatchError,
  FailureCode,
  InsufficientFundsError,
  InvalidMoneyError,
  InvalidPayloadError,
  InvalidTransactionStateError,
  LedgerImbalanceError,
  NegativeBalanceReversalError,
} from "../../src/domain/errors/domain-errors";
import { WageringDomainService } from "../../src/domain/services/wagering-domain-service";
import {
  computeWagerPayloadHash,
  toCanonicalJson,
} from "../../src/domain/utils/canonical-json";
import { Money } from "../../src/domain/value-objects/money";

describe("Domain Layer — Unit Tests", () => {
  describe("Money Value Object", () => {
    it("should create Money with fixed 2 decimal scale and serialize properly", () => {
      const m1 = Money.from({ amount: "25.00", currency: "BRL" });
      expect(m1.toJSON()).toEqual({ amount: "25.00", currency: "BRL" });
      expect(m1.toString()).toBe("25.00 BRL");
      expect(m1.isPositive()).toBe(true);
      expect(m1.isZero()).toBe(false);
      expect(m1.isNegative()).toBe(false);

      const m2 = Money.from({ amount: "10", currency: "BRL" });
      expect(m2.toJSON()).toEqual({ amount: "10.00", currency: "BRL" });

      const zero = Money.zero("BRL");
      expect(zero.isZero()).toBe(true);
      expect(zero.isPositive()).toBe(false);
      expect(zero.toJSON()).toEqual({ amount: "0.00", currency: "BRL" });
    });

    it("should perform immutable arithmetic operations (add, subtract, negate, equals, isLessThan)", () => {
      const a = Money.from({ amount: "100.00", currency: "BRL" });
      const b = Money.from({ amount: "80.00", currency: "BRL" });

      const sum = a.add(b);
      const diff = a.subtract(b);
      const neg = b.negate();

      // Immutability check
      expect(a.toJSON().amount).toBe("100.00");
      expect(b.toJSON().amount).toBe("80.00");

      expect(sum.toJSON().amount).toBe("180.00");
      expect(diff.toJSON().amount).toBe("20.00");
      expect(neg.toJSON().amount).toBe("-80.00");
      expect(neg.isNegative()).toBe(true);
      expect(b.isLessThan(a)).toBe(true);
      expect(a.isLessThan(b)).toBe(false);
      expect(diff.equals(Money.from({ amount: "20.00", currency: "BRL" }))).toBe(true);
    });

    it("should reject invalid inputs (NaN, Infinity, scientific notation, empty, >2 decimals, negative)", () => {
      const invalidAmounts = [
        "",
        "   ",
        "NaN",
        "Infinity",
        "-Infinity",
        "1e5",
        "1E-2",
        "25.000",
        "25.123",
        "-10.00",
        "-0.01",
        "+10.00",
        "abc",
        "25,00",
        "25.00 ",
        " 25.00",
      ];

      for (const amount of invalidAmounts) {
        expect(() => Money.from({ amount, currency: "BRL" })).toThrow(InvalidMoneyError);
      }

      const invalidCurrencies = ["", "br", "brl", "BRLX", "123", "US$"];
      for (const currency of invalidCurrencies) {
        expect(() => Money.from({ amount: "10.00", currency })).toThrow(InvalidMoneyError);
      }
    });

    it("should throw CurrencyMismatchError when operating across different currencies", () => {
      const brl = Money.from({ amount: "50.00", currency: "BRL" });
      const usd = Money.from({ amount: "50.00", currency: "USD" });

      expect(() => brl.add(usd)).toThrow(CurrencyMismatchError);
      expect(() => brl.subtract(usd)).toThrow(CurrencyMismatchError);
      expect(() => brl.isLessThan(usd)).toThrow(CurrencyMismatchError);
      expect(brl.equals(usd)).toBe(false);
    });
  });

  describe("Wallet & WalletLedgerEntry Invariants", () => {
    it("should open a wallet with version 1 and increment version only when balance changes", () => {
      const wallet = Wallet.open({
        id: randomUUID(),
        playerId: randomUUID(),
        initialBalance: Money.from({ amount: "100.00", currency: "BRL" }),
      });

      expect(wallet.version).toBe(1);
      expect(wallet.balance.toJSON()).toEqual({ amount: "100.00", currency: "BRL" });

      const debitEntry = wallet.debit({
        transactionId: randomUUID(),
        amount: Money.from({ amount: "40.00", currency: "BRL" }),
      });

      expect(wallet.version).toBe(2);
      expect(wallet.balance.toJSON().amount).toBe("60.00");
      expect(debitEntry.direction).toBe(LedgerDirection.Debit);
      expect(debitEntry.balanceBefore.toJSON().amount).toBe("100.00");
      expect(debitEntry.balanceAfter.toJSON().amount).toBe("60.00");
      expect(debitEntry.isBalanced()).toBe(true);

      const creditEntry = wallet.credit({
        transactionId: randomUUID(),
        amount: Money.from({ amount: "15.50", currency: "BRL" }),
      });

      expect(wallet.version).toBe(3);
      expect(wallet.balance.toJSON().amount).toBe("75.50");
      expect(creditEntry.direction).toBe(LedgerDirection.Credit);
      expect(creditEntry.balanceBefore.toJSON().amount).toBe("60.00");
      expect(creditEntry.balanceAfter.toJSON().amount).toBe("75.50");
      expect(creditEntry.isBalanced()).toBe(true);
    });

    it("should prevent negative balance on debit and distinguish BET vs Reversal failure codes", () => {
      const wallet = Wallet.open({
        id: randomUUID(),
        playerId: randomUUID(),
        initialBalance: Money.from({ amount: "50.00", currency: "BRL" }),
      });

      expect(() =>
        wallet.debit({
          transactionId: randomUUID(),
          amount: Money.from({ amount: "50.01", currency: "BRL" }),
        }),
      ).toThrow(InsufficientFundsError);

      expect(() =>
        wallet.debit({
          transactionId: randomUUID(),
          amount: Money.from({ amount: "50.01", currency: "BRL" }),
          isReversal: true,
        }),
      ).toThrow(NegativeBalanceReversalError);

      // Balance and version remain untouched after failed debit
      expect(wallet.balance.toJSON().amount).toBe("50.00");
      expect(wallet.version).toBe(1);
    });

    it("should reject unbalanced ledger entries in WalletLedgerEntry.create", () => {
      expect(() =>
        WalletLedgerEntry.create({
          id: randomUUID(),
          walletId: randomUUID(),
          transactionId: randomUUID(),
          direction: LedgerDirection.Debit,
          money: Money.from({ amount: "20.00", currency: "BRL" }),
          balanceBefore: Money.from({ amount: "100.00", currency: "BRL" }),
          balanceAfter: Money.from({ amount: "90.00", currency: "BRL" }), // wrong
        }),
      ).toThrow(LedgerImbalanceError);
    });
  });

  describe("WagerTransaction & Business Rules (BET, WIN, LOSS, REFUND, ROLLBACK)", () => {
    const createTestWallet = (amount = "100.00", currency = "BRL") =>
      Wallet.open({
        id: randomUUID(),
        playerId: randomUUID(),
        initialBalance: Money.from({ amount, currency }),
      });

    const buildTx = (
      wallet: Wallet,
      overrides: Partial<{
        id: string;
        providerId: string;
        externalTransactionId: string;
        idempotencyKey: string;
        roundId: string;
        gameId: string;
        kind: WagerTransactionKind;
        money: Money;
        referenceExternalTransactionId: string;
        allowOpeningKind: boolean;
      }> = {},
    ) => {
      const providerId = overrides.providerId ?? "provider-a";
      const externalTransactionId =
        overrides.externalTransactionId ?? `tx-${randomUUID()}`;
      const kind = overrides.kind ?? WagerTransactionKind.Bet;
      const money = overrides.money ?? Money.from({ amount: "25.00", currency: wallet.currency });
      const roundId = overrides.roundId ?? "round-1";
      const gameId = overrides.gameId ?? "fortune-chimp";

      const payloadHash = computeWagerPayloadHash({
        providerId,
        externalTransactionId,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId,
        gameId,
        kind,
        money: money.toJSON(),
        referenceExternalTransactionId: overrides.referenceExternalTransactionId,
      });

      return WagerTransaction.create({
        id: overrides.id ?? randomUUID(),
        providerId,
        externalTransactionId,
        idempotencyKey:
          overrides.idempotencyKey ?? `${providerId}:${externalTransactionId}`,
        payloadHash,
        walletId: wallet.id,
        playerId: wallet.playerId,
        roundId,
        gameId,
        kind,
        money,
        referenceExternalTransactionId: overrides.referenceExternalTransactionId,
        allowOpeningKind: overrides.allowOpeningKind,
      });
    };

    it("should forbid external creation of OPENING transactions", () => {
      const wallet = createTestWallet();
      expect(() =>
        buildTx(wallet, {
          kind: WagerTransactionKind.Opening,
          allowOpeningKind: false,
        }),
      ).toThrow(InvalidPayloadError);
    });

    it("should require referenceExternalTransactionId for REFUND and ROLLBACK", () => {
      const wallet = createTestWallet();
      expect(() =>
        buildTx(wallet, { kind: WagerTransactionKind.Refund }),
      ).toThrow(InvalidPayloadError);
      expect(() =>
        buildTx(wallet, { kind: WagerTransactionKind.Rollback }),
      ).toThrow(InvalidPayloadError);
    });

    it("should enforce terminal state immutability on WagerTransaction", () => {
      const wallet = createTestWallet();
      const tx = buildTx(wallet);
      tx.markProcessed(undefined, new Date(), wallet.balance);

      expect(tx.isTerminal()).toBe(true);
      expect(() => tx.reject(FailureCode.INSUFFICIENT_FUNDS)).toThrow(
        InvalidTransactionStateError,
      );
      expect(() => tx.markPendingReference()).toThrow(InvalidTransactionStateError);
      expect(() => tx.fail(FailureCode.INFRASTRUCTURE_FAILURE)).toThrow(
        InvalidTransactionStateError,
      );
    });

    it("should process BET, WIN, and LOSS according to domain rules", () => {
      const wallet = createTestWallet("100.00");
      const ctx = { correlationId: "corr-1" };

      // 1. BET 25.00 -> balance 75.00, 1 DEBIT entry, 2 events (WalletBalanceChanged + WagerTransactionProcessed)
      const betTx = buildTx(wallet, {
        externalTransactionId: "bet-1",
        kind: WagerTransactionKind.Bet,
        money: Money.from({ amount: "25.00", currency: "BRL" }),
      });
      const betRes = WageringDomainService.applyTransaction({
        wallet,
        transaction: betTx,
        eventContext: ctx,
      });
      expect(betRes.transaction.status).toBe(WagerTransactionStatus.Processed);
      expect(wallet.balance.toJSON().amount).toBe("75.00");
      expect(betRes.ledgerEntry?.direction).toBe(LedgerDirection.Debit);
      expect(betRes.events.map((e) => e.eventType)).toEqual([
        "WalletBalanceChanged",
        "WagerTransactionProcessed",
      ]);

      // 2. LOSS 0.00 -> balance stays 75.00, NO ledger entry, 1 event (WagerTransactionProcessed)
      const lossTx = buildTx(wallet, {
        externalTransactionId: "loss-1",
        kind: WagerTransactionKind.Loss,
        money: Money.from({ amount: "0.00", currency: "BRL" }),
      });
      const lossRes = WageringDomainService.applyTransaction({
        wallet,
        transaction: lossTx,
        eventContext: ctx,
      });
      expect(lossRes.transaction.status).toBe(WagerTransactionStatus.Processed);
      expect(wallet.balance.toJSON().amount).toBe("75.00");
      expect(wallet.version).toBe(2); // did not increment on LOSS
      expect(lossRes.ledgerEntry).toBeUndefined();
      expect(lossRes.events.map((e) => e.eventType)).toEqual([
        "WagerTransactionProcessed",
      ]);

      // 3. WIN 50.00 referencing bet-1 -> balance 125.00, 1 CREDIT entry
      const winTx = buildTx(wallet, {
        externalTransactionId: "win-1",
        kind: WagerTransactionKind.Win,
        money: Money.from({ amount: "50.00", currency: "BRL" }),
        referenceExternalTransactionId: "bet-1",
      });
      const winRes = WageringDomainService.applyTransaction({
        wallet,
        transaction: winTx,
        referenceTransaction: betTx,
        eventContext: ctx,
      });
      expect(winRes.transaction.status).toBe(WagerTransactionStatus.Processed);
      expect(winRes.transaction.referenceTransactionId).toBe(betTx.id);
      expect(wallet.balance.toJSON().amount).toBe("125.00");
      expect(winRes.ledgerEntry?.direction).toBe(LedgerDirection.Credit);
    });

    it("should process REFUND and ROLLBACK, and reject duplicate reversals or negative-balance reversals", () => {
      const wallet = createTestWallet("100.00");
      const ctx = { correlationId: "corr-2" };

      // BET 80.00 -> balance 20.00
      const betTx = buildTx(wallet, {
        externalTransactionId: "bet-80",
        kind: WagerTransactionKind.Bet,
        money: Money.from({ amount: "80.00", currency: "BRL" }),
      });
      WageringDomainService.applyTransaction({
        wallet,
        transaction: betTx,
        eventContext: ctx,
      });
      expect(wallet.balance.toJSON().amount).toBe("20.00");

      // REFUND 80.00 on bet-80 -> balance 100.00
      const refundTx = buildTx(wallet, {
        externalTransactionId: "refund-80",
        kind: WagerTransactionKind.Refund,
        money: Money.from({ amount: "80.00", currency: "BRL" }),
        referenceExternalTransactionId: "bet-80",
      });
      const refundRes = WageringDomainService.applyTransaction({
        wallet,
        transaction: refundTx,
        referenceTransaction: betTx,
        existingProcessedReversalsForReference: [],
        eventContext: ctx,
      });
      expect(refundRes.transaction.status).toBe(WagerTransactionStatus.Processed);
      expect(wallet.balance.toJSON().amount).toBe("100.00");

      // Second REFUND on bet-80 -> REJECTED with DUPLICATE_REVERSAL
      const duplicateRefundTx = buildTx(wallet, {
        externalTransactionId: "refund-80-dup",
        kind: WagerTransactionKind.Refund,
        money: Money.from({ amount: "80.00", currency: "BRL" }),
        referenceExternalTransactionId: "bet-80",
      });
      const dupRes = WageringDomainService.applyTransaction({
        wallet,
        transaction: duplicateRefundTx,
        referenceTransaction: betTx,
        existingProcessedReversalsForReference: [refundTx],
        eventContext: ctx,
      });
      expect(dupRes.transaction.status).toBe(WagerTransactionStatus.Rejected);
      expect(dupRes.transaction.failureCode).toBe(FailureCode.DUPLICATE_REVERSAL);
      expect(wallet.balance.toJSON().amount).toBe("100.00");

      // Now spend 60.00 so balance is 40.00, then try to ROLLBACK the REFUND of 80.00
      // -> would leave -40.00 -> REJECTED with NEGATIVE_BALANCE_ON_REVERSAL
      const bet60 = buildTx(wallet, {
        externalTransactionId: "bet-60",
        kind: WagerTransactionKind.Bet,
        money: Money.from({ amount: "60.00", currency: "BRL" }),
      });
      WageringDomainService.applyTransaction({
        wallet,
        transaction: bet60,
        eventContext: ctx,
      });
      expect(wallet.balance.toJSON().amount).toBe("40.00");

      const rollbackRefundTx = buildTx(wallet, {
        externalTransactionId: "rollback-refund-80",
        kind: WagerTransactionKind.Rollback,
        money: Money.from({ amount: "80.00", currency: "BRL" }),
        referenceExternalTransactionId: "refund-80",
      });
      const rollbackRes = WageringDomainService.applyTransaction({
        wallet,
        transaction: rollbackRefundTx,
        referenceTransaction: refundTx,
        existingProcessedReversalsForReference: [],
        eventContext: ctx,
      });
      expect(rollbackRes.transaction.status).toBe(WagerTransactionStatus.Rejected);
      expect(rollbackRes.transaction.failureCode).toBe(
        FailureCode.NEGATIVE_BALANCE_ON_REVERSAL,
      );
      expect(wallet.balance.toJSON().amount).toBe("40.00");
    });

    it("should mark transaction as PENDING_REFERENCE when referenced transaction has not arrived yet", () => {
      const wallet = createTestWallet("100.00");
      const rollbackTx = buildTx(wallet, {
        externalTransactionId: "rollback-early",
        kind: WagerTransactionKind.Rollback,
        money: Money.from({ amount: "25.00", currency: "BRL" }),
        referenceExternalTransactionId: "bet-missing",
      });

      const res = WageringDomainService.applyTransaction({
        wallet,
        transaction: rollbackTx,
        referenceTransaction: null,
        eventContext: { correlationId: "corr-early" },
      });

      expect(res.transaction.status).toBe(WagerTransactionStatus.PendingReference);
      expect(res.ledgerEntry).toBeUndefined();
      expect(res.events.map((e) => e.eventType)).toEqual([
        "WagerTransactionPendingReference",
      ]);
    });

    it("should reject transaction when currency mismatches wallet currency", () => {
      const wallet = createTestWallet("100.00", "BRL");
      const usdTx = buildTx(wallet, {
        externalTransactionId: "bet-usd",
        kind: WagerTransactionKind.Bet,
        money: Money.from({ amount: "10.00", currency: "USD" }),
      });

      const res = WageringDomainService.applyTransaction({
        wallet,
        transaction: usdTx,
        eventContext: { correlationId: "corr-curr" },
      });

      expect(res.transaction.status).toBe(WagerTransactionStatus.Rejected);
      expect(res.transaction.failureCode).toBe(FailureCode.CURRENCY_MISMATCH);
      expect(wallet.balance.toJSON().amount).toBe("100.00");
    });
  });

  describe("Idempotency & Canonical Payload Hash", () => {
    it("should produce identical hash regardless of property insertion order and detect divergent payloads", () => {
      const wallet = Wallet.open({
        id: randomUUID(),
        playerId: randomUUID(),
        initialBalance: Money.from({ amount: "100.00", currency: "BRL" }),
      });

      const jsonA = toCanonicalJson({ b: 2, a: 1, nested: { z: "9", m: "1" } });
      const jsonB = toCanonicalJson({ nested: { m: "1", z: "9" }, a: 1, b: 2 });
      expect(jsonA).toBe(jsonB);

      const basePayload = {
        providerId: "provider-a",
        externalTransactionId: "tx-123",
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: "round-1",
        gameId: "fortune-chimp",
        kind: WagerTransactionKind.Bet,
        money: { amount: "25.00", currency: "BRL" },
      };

      const hash1 = computeWagerPayloadHash(basePayload);
      const hash2 = computeWagerPayloadHash({ ...basePayload });
      const divergentHash = computeWagerPayloadHash({
        ...basePayload,
        money: { amount: "50.00", currency: "BRL" },
      });

      expect(hash1).toBe(hash2);
      expect(hash1).not.toBe(divergentHash);

      const tx = WagerTransaction.create({
        id: randomUUID(),
        ...basePayload,
        idempotencyKey: "provider-a:tx-123",
        payloadHash: hash1,
        money: Money.from(basePayload.money),
      });

      expect(tx.matchesPayload(hash2)).toBe(true);
      expect(tx.matchesPayload(divergentHash)).toBe(false);
    });
  });

  describe("InboxMessage & OutboxMessage", () => {
    it("should track InboxMessage processing state", () => {
      const inbox = InboxMessage.receive({
        messageId: "msg-1",
        consumerName: "sqs-wager-consumer",
        payloadHash: "abc123hash",
      });
      expect(inbox.isProcessed()).toBe(false);

      const now = new Date();
      inbox.markProcessed(now);
      expect(inbox.isProcessed()).toBe(true);
      expect(inbox.processedAt).toEqual(now);
    });

    it("should schedule exponential backoff retries on OutboxMessage", () => {
      const wallet = Wallet.open({
        id: randomUUID(),
        playerId: randomUUID(),
        initialBalance: Money.from({ amount: "100.00", currency: "BRL" }),
      });
      const entry = wallet.credit({
        transactionId: randomUUID(),
        amount: Money.from({ amount: "10.00", currency: "BRL" }),
      });

      const { WalletBalanceChanged } = require("../../src/domain/events/integration-event");
      const event = WalletBalanceChanged.from(wallet, entry, {
        correlationId: "corr-outbox",
      });

      const outbox = OutboxMessage.enqueue(event);
      expect(outbox.isPending()).toBe(true);
      expect(outbox.attempts).toBe(0);

      const t0 = new Date();
      outbox.scheduleRetry(t0);
      expect(outbox.attempts).toBe(1);
      expect(outbox.nextAttemptAt!.getTime()).toBeGreaterThan(t0.getTime());

      const tPub = new Date();
      outbox.markPublished(tPub);
      expect(outbox.isPending()).toBe(false);
      expect(outbox.publishedAt).toEqual(tPub);
    });
  });
});
