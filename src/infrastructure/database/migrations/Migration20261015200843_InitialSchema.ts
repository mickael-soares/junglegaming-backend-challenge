import { Migration } from "@mikro-orm/migrations";

export class Migration20261015200843_InitialSchema extends Migration {
  override async up(): Promise<void> {
    this.addSql(`
      CREATE TABLE IF NOT EXISTS wallets (
        id UUID PRIMARY KEY,
        player_id VARCHAR(128) NOT NULL,
        currency CHAR(3) NOT NULL,
        balance_amount NUMERIC(20, 2) NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_wallets_player_currency UNIQUE (player_id, currency),
        CONSTRAINT chk_wallets_balance_non_negative CHECK (balance_amount >= 0),
        CONSTRAINT chk_wallets_version_positive CHECK (version >= 1),
        CONSTRAINT chk_wallets_currency_iso CHECK (currency ~ '^[A-Z]{3}$')
      );
    `);

    this.addSql(`
      CREATE INDEX IF NOT EXISTS idx_wallets_player_id ON wallets (player_id);
    `);

    this.addSql(`
      CREATE TABLE IF NOT EXISTS wager_transactions (
        id UUID PRIMARY KEY,
        provider_id VARCHAR(128) NOT NULL,
        external_transaction_id VARCHAR(255) NOT NULL,
        idempotency_key VARCHAR(255) NOT NULL,
        payload_hash CHAR(64) NOT NULL,
        wallet_id UUID NOT NULL REFERENCES wallets(id) ON DELETE RESTRICT,
        player_id VARCHAR(128) NOT NULL,
        round_id VARCHAR(255) NOT NULL,
        game_id VARCHAR(255) NOT NULL,
        kind VARCHAR(32) NOT NULL,
        money_amount NUMERIC(20, 2) NOT NULL,
        money_currency CHAR(3) NOT NULL,
        reference_external_transaction_id VARCHAR(255) NULL,
        reference_transaction_id UUID NULL REFERENCES wager_transactions(id) ON DELETE RESTRICT,
        status VARCHAR(32) NOT NULL,
        failure_code VARCHAR(64) NULL,
        observed_balance_amount NUMERIC(20, 2) NULL,
        observed_balance_currency CHAR(3) NULL,
        retry_attempts INTEGER NOT NULL DEFAULT 0,
        next_retry_at TIMESTAMPTZ NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        processed_at TIMESTAMPTZ NULL,
        CONSTRAINT uq_wager_tx_idempotency_key UNIQUE (idempotency_key),
        CONSTRAINT uq_wager_tx_provider_external UNIQUE (provider_id, external_transaction_id),
        CONSTRAINT chk_wager_tx_kind CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
        CONSTRAINT chk_wager_tx_status CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
        CONSTRAINT chk_wager_tx_amount_non_negative CHECK (money_amount >= 0),
        CONSTRAINT chk_wager_tx_positive_when_not_loss CHECK (kind = 'LOSS' OR money_amount > 0),
        CONSTRAINT chk_wager_tx_reference_required CHECK (kind NOT IN ('REFUND', 'ROLLBACK') OR reference_external_transaction_id IS NOT NULL)
      );
    `);

    this.addSql(`
      CREATE INDEX IF NOT EXISTS idx_wager_tx_wallet_id ON wager_transactions (wallet_id, created_at DESC);
    `);

    this.addSql(`
      CREATE INDEX IF NOT EXISTS idx_wager_tx_pending_reference
      ON wager_transactions (next_retry_at, created_at)
      WHERE status = 'PENDING_REFERENCE';
    `);

    this.addSql(`
      CREATE UNIQUE INDEX IF NOT EXISTS uq_wager_tx_single_processed_reversal
      ON wager_transactions (reference_transaction_id)
      WHERE status = 'PROCESSED'
        AND kind IN ('REFUND', 'ROLLBACK')
        AND reference_transaction_id IS NOT NULL;
    `);

    this.addSql(`
      CREATE OR REPLACE FUNCTION fn_guard_wager_transaction_terminal()
      RETURNS TRIGGER AS $$
      BEGIN
        IF TG_OP = 'DELETE' THEN
          RAISE EXCEPTION 'Deleting wager_transactions is forbidden for auditability';
        END IF;
        IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
          IF NEW.status <> OLD.status
             OR NEW.money_amount <> OLD.money_amount
             OR NEW.money_currency <> OLD.money_currency
             OR NEW.kind <> OLD.kind
             OR NEW.wallet_id <> OLD.wallet_id
             OR NEW.payload_hash <> OLD.payload_hash THEN
            RAISE EXCEPTION 'Cannot mutate terminal wager_transaction % from status % to %', OLD.id, OLD.status, NEW.status;
          END IF;
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    `);

    this.addSql(`
      DROP TRIGGER IF EXISTS trg_guard_wager_transaction_terminal ON wager_transactions;
      CREATE TRIGGER trg_guard_wager_transaction_terminal
      BEFORE UPDATE OR DELETE ON wager_transactions
      FOR EACH ROW EXECUTE FUNCTION fn_guard_wager_transaction_terminal();
    `);

    this.addSql(`
      CREATE TABLE IF NOT EXISTS wallet_ledger_entries (
        id UUID PRIMARY KEY,
        sequence_number BIGSERIAL NOT NULL,
        wallet_id UUID NOT NULL REFERENCES wallets(id) ON DELETE RESTRICT,
        transaction_id UUID NOT NULL REFERENCES wager_transactions(id) ON DELETE RESTRICT,
        direction VARCHAR(16) NOT NULL,
        money_amount NUMERIC(20, 2) NOT NULL,
        money_currency CHAR(3) NOT NULL,
        balance_before_amount NUMERIC(20, 2) NOT NULL,
        balance_after_amount NUMERIC(20, 2) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_ledger_wallet_tx UNIQUE (wallet_id, transaction_id),
        CONSTRAINT chk_ledger_direction CHECK (direction IN ('DEBIT', 'CREDIT')),
        CONSTRAINT chk_ledger_money_positive CHECK (money_amount > 0),
        CONSTRAINT chk_ledger_balances_non_negative CHECK (balance_before_amount >= 0 AND balance_after_amount >= 0),
        CONSTRAINT chk_ledger_arithmetic_balanced CHECK (
          (direction = 'CREDIT' AND balance_before_amount + money_amount = balance_after_amount)
          OR
          (direction = 'DEBIT' AND balance_before_amount - money_amount = balance_after_amount)
        )
      );
    `);

    this.addSql(`
      CREATE INDEX IF NOT EXISTS idx_ledger_wallet_sequence
      ON wallet_ledger_entries (wallet_id, sequence_number ASC);
    `);

    this.addSql(`
      CREATE OR REPLACE FUNCTION fn_prevent_ledger_mutation()
      RETURNS TRIGGER AS $$
      BEGIN
        RAISE EXCEPTION 'wallet_ledger_entries is strictly append-only and immutable: % operation forbidden on entry %', TG_OP, OLD.id;
      END;
      $$ LANGUAGE plpgsql;
    `);

    this.addSql(`
      DROP TRIGGER IF EXISTS trg_wallet_ledger_immutable ON wallet_ledger_entries;
      CREATE TRIGGER trg_wallet_ledger_immutable
      BEFORE UPDATE OR DELETE ON wallet_ledger_entries
      FOR EACH ROW EXECUTE FUNCTION fn_prevent_ledger_mutation();
    `);

    this.addSql(`
      CREATE TABLE IF NOT EXISTS inbox_messages (
        consumer_name VARCHAR(128) NOT NULL,
        message_id VARCHAR(255) NOT NULL,
        payload_hash CHAR(64) NOT NULL,
        received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        processed_at TIMESTAMPTZ NULL,
        PRIMARY KEY (consumer_name, message_id)
      );
    `);

    this.addSql(`
      CREATE TABLE IF NOT EXISTS outbox_messages (
        id UUID PRIMARY KEY,
        aggregate_id VARCHAR(128) NOT NULL,
        event_type VARCHAR(128) NOT NULL,
        payload JSONB NOT NULL,
        occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at TIMESTAMPTZ NULL,
        published_at TIMESTAMPTZ NULL,
        CONSTRAINT chk_outbox_attempts_non_negative CHECK (attempts >= 0)
      );
    `);

    this.addSql(`
      CREATE INDEX IF NOT EXISTS idx_outbox_unpublished_due
      ON outbox_messages (next_attempt_at ASC, occurred_at ASC)
      WHERE published_at IS NULL;
    `);
  }

  override async down(): Promise<void> {
    this.addSql(`DROP TABLE IF EXISTS outbox_messages CASCADE;`);
    this.addSql(`DROP TABLE IF EXISTS inbox_messages CASCADE;`);
    this.addSql(`DROP TRIGGER IF EXISTS trg_wallet_ledger_immutable ON wallet_ledger_entries;`);
    this.addSql(`DROP FUNCTION IF EXISTS fn_prevent_ledger_mutation();`);
    this.addSql(`DROP TABLE IF EXISTS wallet_ledger_entries CASCADE;`);
    this.addSql(`DROP TRIGGER IF EXISTS trg_guard_wager_transaction_terminal ON wager_transactions;`);
    this.addSql(`DROP FUNCTION IF EXISTS fn_guard_wager_transaction_terminal();`);
    this.addSql(`DROP TABLE IF EXISTS wager_transactions CASCADE;`);
    this.addSql(`DROP TABLE IF EXISTS wallets CASCADE;`);
  }
}
