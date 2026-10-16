import { Wallet } from "../entities/wallet";
import { LedgerDirection, WalletLedgerEntry } from "../entities/wallet-ledger-entry";
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from "../entities/wager-transaction";
import {
  CurrencyMismatchError,
  DomainError,
  FailureCode,
  InsufficientFundsError,
  NegativeBalanceReversalError,
} from "../errors/domain-errors";
import {
  EventContext,
  IntegrationEvent,
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
} from "../events/integration-event";

export interface ApplyWagerTransactionInput {
  wallet: Wallet;
  transaction: WagerTransaction;
  referenceTransaction?: WagerTransaction | null;
  existingProcessedReversalsForReference?: WagerTransaction[];
  eventContext: EventContext;
  now?: Date;
}

export interface ApplyWagerTransactionOutput {
  transaction: WagerTransaction;
  wallet: Wallet;
  ledgerEntry?: WalletLedgerEntry;
  events: IntegrationEvent<unknown>[];
}

export class WageringDomainService {
  /**
   * Applies a WagerTransaction against a locked Wallet aggregate, enforcing all
   * financial invariants, reference rules, and generating the atomic ledger entry
   * and integration events.
   */
  static applyTransaction(
    input: ApplyWagerTransactionInput,
  ): ApplyWagerTransactionOutput {
    const {
      wallet,
      transaction,
      referenceTransaction,
      existingProcessedReversalsForReference = [],
      eventContext,
    } = input;
    const now = input.now ?? new Date();

    // 1. Validate player & wallet currency consistency
    if (transaction.playerId !== wallet.playerId) {
      return this.rejectTransaction(
        wallet,
        transaction,
        FailureCode.REFERENCE_CONTEXT_MISMATCH,
        eventContext,
        now,
      );
    }

    if (transaction.money.currency !== wallet.currency) {
      return this.rejectTransaction(
        wallet,
        transaction,
        FailureCode.CURRENCY_MISMATCH,
        eventContext,
        now,
      );
    }

    // 2. Reference validation for operations that require or optionally specify a reference
    const hasReferenceId =
      typeof transaction.referenceExternalTransactionId === "string" &&
      transaction.referenceExternalTransactionId.trim().length > 0;

    if (transaction.requiresReference() || hasReferenceId) {
      // Missing reference in DB or reference still pending -> transition to PENDING_REFERENCE
      if (
        !referenceTransaction ||
        referenceTransaction.status === WagerTransactionStatus.Pending ||
        referenceTransaction.status === WagerTransactionStatus.PendingReference
      ) {
        transaction.markPendingReference(new Date(now.getTime() + 100));
        const pendingEvent = WagerTransactionPendingReference.from(
          transaction,
          { ...eventContext, occurredAt: now },
        );
        return {
          transaction,
          wallet,
          ledgerEntry: undefined,
          events: [pendingEvent],
        };
      }

      // Reference must belong to the same provider, player, wallet, currency, and round
      if (
        referenceTransaction.providerId !== transaction.providerId ||
        referenceTransaction.playerId !== transaction.playerId ||
        referenceTransaction.walletId !== transaction.walletId ||
        referenceTransaction.roundId !== transaction.roundId
      ) {
        return this.rejectTransaction(
          wallet,
          transaction,
          FailureCode.REFERENCE_CONTEXT_MISMATCH,
          eventContext,
          now,
        );
      }

      if (referenceTransaction.money.currency !== transaction.money.currency) {
        return this.rejectTransaction(
          wallet,
          transaction,
          FailureCode.CURRENCY_MISMATCH,
          eventContext,
          now,
        );
      }

      // Reference must be PROCESSED
      if (referenceTransaction.status !== WagerTransactionStatus.Processed) {
        return this.rejectTransaction(
          wallet,
          transaction,
          FailureCode.REFERENCE_NOT_PROCESSED,
          eventContext,
          now,
        );
      }

      // Specific rules for WIN referencing a BET
      if (transaction.kind === WagerTransactionKind.Win) {
        if (referenceTransaction.kind !== WagerTransactionKind.Bet) {
          return this.rejectTransaction(
            wallet,
            transaction,
            FailureCode.INVALID_REFERENCE_KIND,
            eventContext,
            now,
          );
        }
      }

      // Specific rules for REFUND and ROLLBACK
      if (
        transaction.kind === WagerTransactionKind.Refund ||
        transaction.kind === WagerTransactionKind.Rollback
      ) {
        if (
          transaction.kind === WagerTransactionKind.Refund &&
          referenceTransaction.kind !== WagerTransactionKind.Bet
        ) {
          return this.rejectTransaction(
            wallet,
            transaction,
            FailureCode.INVALID_REFERENCE_KIND,
            eventContext,
            now,
          );
        }

        if (
          transaction.kind === WagerTransactionKind.Rollback &&
          referenceTransaction.kind !== WagerTransactionKind.Bet &&
          referenceTransaction.kind !== WagerTransactionKind.Win &&
          referenceTransaction.kind !== WagerTransactionKind.Refund
        ) {
          return this.rejectTransaction(
            wallet,
            transaction,
            FailureCode.INVALID_REFERENCE_KIND,
            eventContext,
            now,
          );
        }

        // Exact amount match required (partial reversal out of scope)
        if (!transaction.money.equals(referenceTransaction.money)) {
          return this.rejectTransaction(
            wallet,
            transaction,
            FailureCode.REFERENCE_AMOUNT_MISMATCH,
            eventContext,
            now,
          );
        }

        // Prevent duplicate reversal of the same reference
        const alreadyReversedSameKind = existingProcessedReversalsForReference.some(
          (rev) =>
            rev.status === WagerTransactionStatus.Processed &&
            rev.id !== transaction.id &&
            (rev.kind === transaction.kind ||
              // Also prevent both REFUND and ROLLBACK from crediting the same BET twice
              (referenceTransaction.kind === WagerTransactionKind.Bet &&
                (rev.kind === WagerTransactionKind.Refund ||
                  rev.kind === WagerTransactionKind.Rollback))),
        );

        if (alreadyReversedSameKind) {
          return this.rejectTransaction(
            wallet,
            transaction,
            FailureCode.DUPLICATE_REVERSAL,
            eventContext,
            now,
          );
        }
      }
    }

    // 3. Apply financial effect on Wallet
    try {
      let ledgerEntry: WalletLedgerEntry | undefined;

      if (transaction.kind === WagerTransactionKind.Loss) {
        // LOSS does not change wallet balance or create ledger entry
        transaction.markProcessed(
          referenceTransaction?.id,
          now,
          wallet.balance,
        );
      } else {
        const direction = transaction.ledgerDirectionFor(
          referenceTransaction ?? undefined,
        );
        const isReversal =
          transaction.kind === WagerTransactionKind.Refund ||
          transaction.kind === WagerTransactionKind.Rollback;

        if (direction === LedgerDirection.Debit) {
          ledgerEntry = wallet.debit({
            transactionId: transaction.id,
            amount: transaction.money,
            isReversal,
            at: now,
          });
        } else {
          ledgerEntry = wallet.credit({
            transactionId: transaction.id,
            amount: transaction.money,
            isReversal,
            at: now,
          });
        }

        transaction.markProcessed(
          referenceTransaction?.id,
          now,
          wallet.balance,
        );
      }

      const events: IntegrationEvent<unknown>[] = [];
      if (ledgerEntry) {
        events.push(
          WalletBalanceChanged.from(wallet, ledgerEntry, {
            ...eventContext,
            occurredAt: now,
          }),
        );
      }

      events.push(
        WagerTransactionProcessed.from(transaction, wallet.balance.toJSON(), {
          ...eventContext,
          occurredAt: now,
        }),
      );

      return {
        transaction,
        wallet,
        ledgerEntry,
        events,
      };
    } catch (err) {
      if (
        err instanceof InsufficientFundsError ||
        err instanceof NegativeBalanceReversalError ||
        err instanceof CurrencyMismatchError
      ) {
        return this.rejectTransaction(
          wallet,
          transaction,
          err.code,
          eventContext,
          now,
        );
      }
      if (err instanceof DomainError) {
        return this.rejectTransaction(
          wallet,
          transaction,
          err.code,
          eventContext,
          now,
        );
      }
      throw err;
    }
  }

  static rejectTransaction(
    wallet: Wallet,
    transaction: WagerTransaction,
    code: FailureCode,
    eventContext: EventContext,
    now: Date = new Date(),
  ): ApplyWagerTransactionOutput {
    transaction.reject(code, now, wallet.balance);
    const rejectedEvent = WagerTransactionRejected.from(
      transaction,
      wallet.balance.toJSON(),
      { ...eventContext, occurredAt: now },
    );
    return {
      transaction,
      wallet,
      ledgerEntry: undefined,
      events: [rejectedEvent],
    };
  }
}
