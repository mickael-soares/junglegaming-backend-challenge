import { createHash } from "node:crypto";
import { MoneyProps } from "../value-objects/money";

export interface CanonicalWagerPayloadInput {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: MoneyProps;
  referenceExternalTransactionId?: string | null;
}

/**
 * Recursively serializes a value into canonical JSON (RFC 8785 style sorted keys,
 * omitting undefined values so optional omitted fields match across HTTP and SQS).
 */
export function toCanonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }

  if (Array.isArray(value)) {
    return `[${value.map((item) => toCanonicalJson(item)).join(",")}]`;
  }

  const record = value as Record<string, unknown>;
  const sortedKeys = Object.keys(record)
    .filter((key) => record[key] !== undefined && record[key] !== null)
    .sort();

  const entries = sortedKeys.map(
    (key) => `${JSON.stringify(key)}:${toCanonicalJson(record[key])}`,
  );

  return `{${entries.join(",")}}`;
}

/**
 * Computes a deterministic SHA-256 hex digest of the canonical business payload.
 * Transport metadata (headers, SQS messageId, occurredAt) are excluded.
 */
export function computeWagerPayloadHash(input: CanonicalWagerPayloadInput): string {
  const normalized = {
    externalTransactionId: input.externalTransactionId,
    gameId: input.gameId,
    kind: input.kind,
    money: {
      amount: input.money.amount,
      currency: input.money.currency,
    },
    playerId: input.playerId,
    providerId: input.providerId,
    ...(input.referenceExternalTransactionId
      ? { referenceExternalTransactionId: input.referenceExternalTransactionId }
      : {}),
    roundId: input.roundId,
    walletId: input.walletId,
  };

  const canonical = toCanonicalJson(normalized);
  return createHash("sha256").update(canonical, "utf8").digest("hex");
}
