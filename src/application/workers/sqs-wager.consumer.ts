import { Message } from "@aws-sdk/client-sqs";
import {
  Inject,
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from "@nestjs/common";
import { createHash, randomUUID } from "node:crypto";
import {
  DomainError,
  EntityNotFoundError,
  IdempotencyConflictError,
  InvalidMoneyError,
  InvalidPayloadError,
} from "../../domain/errors/domain-errors";
import { toCanonicalJson } from "../../domain/utils/canonical-json";
import { MoneyProps } from "../../domain/value-objects/money";
import { SqsClientService } from "../../infrastructure/messaging/sqs-client.service";
import { StructuredLogger } from "../../infrastructure/observability/logger.service";
import { MetricsService } from "../../infrastructure/observability/metrics.service";
import {
  ProcessWagerTransactionOutput,
  ProcessWagerTransactionUseCase,
} from "../use-cases/process-wager-transaction.use-case";

export interface SqsWagerEnvelope {
  messageId: string;
  type: "WagerTransactionRequested" | string;
  occurredAt: string;
  data: {
    providerId: string;
    externalTransactionId: string;
    idempotencyKey?: string;
    playerId: string;
    walletId: string;
    roundId: string;
    gameId: string;
    kind: string;
    money: MoneyProps;
    referenceExternalTransactionId?: string;
  };
}

export interface HandleSqsMessageOptions {
  /** Test hook to simulate process crash right after DB commit and before SQS ack */
  crashBeforeAck?: boolean;
}

@Injectable()
export class SqsWagerConsumer implements OnModuleInit, OnModuleDestroy {
  static readonly CONSUMER_NAME = "sqs-wager-transactions-consumer";

  private isPolling = false;
  private isShuttingDown = false;
  private pollLoopPromise: Promise<void> | null = null;
  private readonly inFlightMessages = new Map<string, string>(); // messageId -> receiptHandle

  private readonly maxReceiveCount = Number(
    process.env.SQS_MAX_RECEIVE_COUNT ?? 5,
  );

  constructor(
    @Inject(SqsClientService) private readonly sqsClient: SqsClientService,
    @Inject(ProcessWagerTransactionUseCase)
    private readonly processWagerUseCase: ProcessWagerTransactionUseCase,
    @Inject(StructuredLogger) private readonly logger: StructuredLogger,
    @Inject(MetricsService) private readonly metrics: MetricsService,
  ) {}

  async onModuleInit(): Promise<void> {
    if (process.env.SQS_POLL_ENABLED === "false") {
      return;
    }
    try {
      await this.sqsClient.ensureQueuesExist();
    } catch (err) {
      this.logger.warn(
        "SQS queues not reachable at startup; consumer will retry in background",
        {},
      );
    }
    this.startPolling();
  }

  async onModuleDestroy(): Promise<void> {
    await this.gracefulShutdown();
  }

  startPolling(): void {
    if (this.isPolling) return;
    this.isPolling = true;
    this.isShuttingDown = false;
    this.pollLoopPromise = this.pollLoop();
  }

  /**
   * Graceful shutdown on SIGTERM / module destroy:
   * Waits briefly for in-flight messages to finish; if any remain unfinished,
   * releases their SQS visibility timeout back to 0 so another instance can process them immediately.
   */
  async gracefulShutdown(timeoutMs = 3_000): Promise<void> {
    this.isShuttingDown = true;
    this.isPolling = false;

    if (this.pollLoopPromise) {
      await Promise.race([
        this.pollLoopPromise,
        new Promise((resolve) => setTimeout(resolve, timeoutMs)),
      ]);
    }

    for (const [msgId, receiptHandle] of this.inFlightMessages.entries()) {
      try {
        await this.sqsClient.changeMessageVisibility(
          this.sqsClient.wagerQueueName,
          receiptHandle,
          0,
        );
        this.logger.info("Released SQS visibility for in-flight message on SIGTERM", {
          messageId: msgId,
        });
      } catch {
        // Ignore if already acked
      }
    }
    this.inFlightMessages.clear();
  }

  private async pollLoop(): Promise<void> {
    while (this.isPolling && !this.isShuttingDown) {
      try {
        const messages = await this.sqsClient.receiveMessages(
          this.sqsClient.wagerQueueName,
          10,
          1,
          30,
        );

        if (messages.length === 0) {
          continue;
        }

        for (const message of messages) {
          if (this.isShuttingDown) {
            if (message.ReceiptHandle) {
              await this.sqsClient.changeMessageVisibility(
                this.sqsClient.wagerQueueName,
                message.ReceiptHandle,
                0,
              );
            }
            break;
          }
          await this.handleRawSqsMessage(message);
        }
      } catch (err) {
        if (!this.isShuttingDown) {
          await new Promise((resolve) => setTimeout(resolve, 500));
        }
      }
    }
  }

  /**
   * Processes a single raw SQS message.
   * Exposed publicly so integration & concurrency tests can also invoke or inspect behavior deterministically.
   */
  async handleRawSqsMessage(
    message: Message,
    options?: HandleSqsMessageOptions,
  ): Promise<ProcessWagerTransactionOutput | null> {
    const receiptHandle = message.ReceiptHandle;
    const sqsMessageId = message.MessageId ?? randomUUID();
    const receiveCount = Number(
      message.Attributes?.ApproximateReceiveCount ?? "1",
    );

    if (receiptHandle) {
      this.inFlightMessages.set(sqsMessageId, receiptHandle);
    }

    try {
      if (!message.Body) {
        await this.routeToDlqAndAck(
          message,
          sqsMessageId,
          "EMPTY_MESSAGE_BODY",
        );
        return null;
      }

      let envelope: SqsWagerEnvelope;
      try {
        envelope = JSON.parse(message.Body) as SqsWagerEnvelope;
      } catch {
        await this.routeToDlqAndAck(message, sqsMessageId, "INVALID_JSON_BODY");
        return null;
      }

      const logicalMessageId = envelope.messageId || sqsMessageId;
      const correlationId = logicalMessageId;

      if (
        !envelope ||
        envelope.type !== "WagerTransactionRequested" ||
        !envelope.data ||
        typeof envelope.data !== "object"
      ) {
        await this.routeToDlqAndAck(
          message,
          logicalMessageId,
          "UNSUPPORTED_ENVELOPE_TYPE",
        );
        return null;
      }

      const { data } = envelope;
      const idempotencyKey =
        data.idempotencyKey ??
        `${data.providerId}:${data.externalTransactionId}`;

      const messagePayloadHash = createHash("sha256")
        .update(toCanonicalJson(data), "utf8")
        .digest("hex");

      // Execute the shared application use case (Inbox + Wallet + Transaction + Ledger + Outbox in 1 SQL TX)
      const output = await this.processWagerUseCase.execute({
        idempotencyKey,
        providerId: data.providerId,
        externalTransactionId: data.externalTransactionId,
        playerId: data.playerId,
        walletId: data.walletId,
        roundId: data.roundId,
        gameId: data.gameId,
        kind: data.kind,
        money: data.money,
        referenceExternalTransactionId: data.referenceExternalTransactionId,
        correlationId,
        inboxContext: {
          consumerName: SqsWagerConsumer.CONSUMER_NAME,
          messageId: logicalMessageId,
          messagePayloadHash,
        },
      });

      if (options?.crashBeforeAck) {
        throw new Error("SIMULATED_WORKER_CRASH_AFTER_COMMIT_BEFORE_ACK");
      }

      if (receiptHandle) {
        await this.sqsClient.deleteMessage(
          this.sqsClient.wagerQueueName,
          receiptHandle,
        );
      }

      return output;
    } catch (err: unknown) {
      if (
        err instanceof Error &&
        err.message === "SIMULATED_WORKER_CRASH_AFTER_COMMIT_BEFORE_ACK"
      ) {
        // Release visibility so redelivery can happen immediately in tests
        if (receiptHandle) {
          await this.sqsClient.changeMessageVisibility(
            this.sqsClient.wagerQueueName,
            receiptHandle,
            0,
          );
        }
        throw err;
      }

      // Distinguish Permanent Contract/Domain Conflict Errors vs Transient Errors
      if (this.isPermanentError(err)) {
        await this.routeToDlqAndAck(
          message,
          sqsMessageId,
          err instanceof DomainError ? err.code : "PERMANENT_CONTRACT_ERROR",
        );
        return null;
      }

      // Transient error or missing wallet not yet created -> retry with exponential backoff up to maxReceiveCount
      this.metrics.incrementRetry("sqs");

      if (receiveCount >= this.maxReceiveCount) {
        await this.routeToDlqAndAck(
          message,
          sqsMessageId,
          "MAX_RECEIVE_COUNT_EXCEEDED",
        );
        return null;
      }

      if (receiptHandle) {
        const backoffSeconds = Math.min(30, Math.pow(2, receiveCount - 1));
        try {
          await this.sqsClient.changeMessageVisibility(
            this.sqsClient.wagerQueueName,
            receiptHandle,
            backoffSeconds,
          );
        } catch {
          // Ignore visibility update failure
        }
      }

      this.logger.warn("Transient failure processing SQS message; scheduled redelivery", {
        messageId: sqsMessageId,
        receiveCount,
      });

      return null;
    } finally {
      this.inFlightMessages.delete(sqsMessageId);
    }
  }

  private isPermanentError(err: unknown): boolean {
    return (
      err instanceof InvalidMoneyError ||
      err instanceof InvalidPayloadError ||
      err instanceof IdempotencyConflictError
    );
  }

  private async routeToDlqAndAck(
    message: Message,
    messageId: string,
    reason: string,
  ): Promise<void> {
    try {
      await this.sqsClient.sendMessage({
        queueName: this.sqsClient.wagerDlqName,
        body: JSON.stringify({
          originalMessageId: messageId,
          reason,
          failedAt: new Date().toISOString(),
          rawBody: message.Body ?? null,
        }),
        messageGroupId: "dlq",
        messageDeduplicationId: `${messageId}:${Date.now()}`,
      });

      if (message.ReceiptHandle) {
        await this.sqsClient.deleteMessage(
          this.sqsClient.wagerQueueName,
          message.ReceiptHandle,
        );
      }

      this.metrics.incrementDlqMessages();
      this.logger.warn("Routed SQS message to DLQ", {
        messageId,
        reason,
      });
    } catch (dlqErr) {
      this.logger.error("Failed to route message to DLQ", { messageId, reason }, dlqErr);
    }
  }
}
