import { EntityManager } from "@mikro-orm/postgresql";
import { Inject, Injectable } from "@nestjs/common";
import { LedgerDirection } from "../../domain/entities/wallet-ledger-entry";
import {
  EntityNotFoundError,
  FailureCode,
  InvalidPayloadError,
} from "../../domain/errors/domain-errors";
import { Money, MoneyProps } from "../../domain/value-objects/money";
import {
  WalletLedgerEntryRecord,
  WalletRecord,
} from "../../infrastructure/database/entities/persistence-entities";
import { StructuredLogger } from "../../infrastructure/observability/logger.service";
import { MetricsService } from "../../infrastructure/observability/metrics.service";

export interface ReconcileWalletOutput {
  walletId: string;
  storedBalance: MoneyProps;
  calculatedBalance: MoneyProps;
  difference: MoneyProps;
  consistent: boolean;
  checkedEntries: number;
}

@Injectable()
export class ReconcileWalletUseCase {
  constructor(
    @Inject(EntityManager) private readonly em: EntityManager,
    @Inject(StructuredLogger) private readonly logger: StructuredLogger,
    @Inject(MetricsService) private readonly metrics: MetricsService,
  ) {}

  async execute(walletId: string, correlationId?: string): Promise<ReconcileWalletOutput> {
    const uuidRegex =
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    if (!walletId || !uuidRegex.test(walletId)) {
      throw new InvalidPayloadError("walletId must be a valid UUID");
    }

    const walletRecord = await this.em.findOne(WalletRecord, { id: walletId });
    if (!walletRecord) {
      throw new EntityNotFoundError(
        FailureCode.WALLET_NOT_FOUND,
        `Wallet '${walletId}' not found`,
      );
    }

    const wallet = walletRecord.toDomain();

    // Load all ledger entries ordered deterministically by sequence_number ASC
    const entryRecords = await this.em.find(
      WalletLedgerEntryRecord,
      { walletId },
      { orderBy: { sequenceNumber: "ASC", createdAt: "ASC" } },
    );

    let calculatedBalance = Money.zero(wallet.currency);
    let chainConsistent = true;

    for (const rec of entryRecords) {
      const entry = rec.toDomain();
      if (!entry.isBalanced()) {
        chainConsistent = false;
      }
      if (!entry.balanceBefore.equals(calculatedBalance)) {
        chainConsistent = false;
      }
      calculatedBalance =
        entry.direction === LedgerDirection.Credit
          ? calculatedBalance.add(entry.money)
          : calculatedBalance.subtract(entry.money);
    }

    const storedBalance = wallet.balance;
    const difference = storedBalance.subtract(calculatedBalance);
    const consistent = storedBalance.equals(calculatedBalance) && chainConsistent;

    if (!consistent) {
      this.metrics.incrementReconciliationDivergence();
      this.logger.error("Wallet reconciliation divergence detected", {
        correlationId,
        walletId,
        playerId: wallet.playerId,
        storedBalance: storedBalance.amount,
        calculatedBalance: calculatedBalance.amount,
        difference: difference.amount,
        checkedEntries: entryRecords.length,
      });
    } else {
      this.logger.info("Wallet reconciliation completed consistently", {
        correlationId,
        walletId,
        checkedEntries: entryRecords.length,
      });
    }

    return {
      walletId: wallet.id,
      storedBalance: storedBalance.toJSON(),
      calculatedBalance: calculatedBalance.toJSON(),
      difference: difference.toJSON(),
      consistent,
      checkedEntries: entryRecords.length,
    };
  }
}
