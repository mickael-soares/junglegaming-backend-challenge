import { Injectable } from "@nestjs/common";

export interface LogContext {
  correlationId?: string;
  messageId?: string;
  transactionId?: string;
  walletId?: string;
  providerId?: string;
  playerId?: string;
  roundId?: string;
  kind?: string;
  status?: string;
  failureCode?: string;
  durationMs?: number;
  [key: string]: unknown;
}

const REDACTED_KEYS = new Set([
  "password",
  "secret",
  "token",
  "authorization",
  "rawPayload",
  "fullPayload",
]);

@Injectable()
export class StructuredLogger {
  private readonly instanceId = process.env.INSTANCE_ID ?? "instance-1";
  private readonly silent = process.env.LOG_SILENT === "true";

  info(message: string, context: LogContext = {}): void {
    this.write("INFO", message, context);
  }

  warn(message: string, context: LogContext = {}): void {
    this.write("WARN", message, context);
  }

  error(message: string, context: LogContext = {}, error?: unknown): void {
    const errInfo =
      error instanceof Error
        ? { errorName: error.name, errorMessage: error.message }
        : error !== undefined
          ? { errorMessage: String(error) }
          : {};
    this.write("ERROR", message, { ...context, ...errInfo });
  }

  private write(level: "INFO" | "WARN" | "ERROR", message: string, context: LogContext): void {
    if (this.silent) {
      return;
    }

    const sanitizedContext: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(context)) {
      if (value === undefined) continue;
      if (REDACTED_KEYS.has(key)) {
        sanitizedContext[key] = "[REDACTED]";
      } else {
        sanitizedContext[key] = value;
      }
    }

    const entry = {
      timestamp: new Date().toISOString(),
      level,
      instanceId: this.instanceId,
      message,
      ...sanitizedContext,
    };

    const serialized = JSON.stringify(entry);
    if (level === "ERROR") {
      process.stderr.write(serialized + "\n");
    } else {
      process.stdout.write(serialized + "\n");
    }
  }
}
