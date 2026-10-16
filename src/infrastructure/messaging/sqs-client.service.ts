import {
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  Message,
  PurgeQueueCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { Inject, Injectable, OnModuleDestroy } from "@nestjs/common";
import { StructuredLogger } from "../observability/logger.service";

export interface SqsPublishParams {
  queueName: string;
  body: string;
  messageGroupId: string;
  messageDeduplicationId: string;
}

@Injectable()
export class SqsClientService implements OnModuleDestroy {
  private readonly client: SQSClient;
  private readonly queueUrlCache = new Map<string, string>();

  readonly wagerQueueName =
    process.env.SQS_WAGER_QUEUE_NAME ?? "wager-transactions.fifo";
  readonly wagerDlqName =
    process.env.SQS_WAGER_DLQ_NAME ?? "wager-transactions-dlq.fifo";
  readonly eventsQueueName =
    process.env.SQS_EVENTS_QUEUE_NAME ?? "wager-integration-events.fifo";

  constructor(@Inject(StructuredLogger) private readonly logger: StructuredLogger) {
    this.client = new SQSClient({
      region: process.env.AWS_REGION ?? "us-east-1",
      endpoint: process.env.AWS_ENDPOINT ?? "http://localhost:4566",
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? "test",
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? "test",
      },
    });
  }

  onModuleDestroy(): void {
    this.client.destroy();
  }

  async ensureQueuesExist(): Promise<void> {
    // 1. Ensure DLQ FIFO
    const dlqUrl = await this.getOrCreateFifoQueue(this.wagerDlqName);
    const dlqAttrs = await this.client.send(
      new GetQueueAttributesCommand({
        QueueUrl: dlqUrl,
        AttributeNames: ["QueueArn"],
      }),
    );
    const dlqArn = dlqAttrs.Attributes?.QueueArn;

    // 2. Ensure Main Wager FIFO Queue with RedrivePolicy
    const maxReceiveCount = process.env.SQS_MAX_RECEIVE_COUNT ?? "5";
    await this.getOrCreateFifoQueue(
      this.wagerQueueName,
      dlqArn
        ? {
            VisibilityTimeout: "30",
            RedrivePolicy: JSON.stringify({
              deadLetterTargetArn: dlqArn,
              maxReceiveCount,
            }),
          }
        : { VisibilityTimeout: "30" },
    );

    // 3. Ensure Integration Events Outbox FIFO Queue
    await this.getOrCreateFifoQueue(this.eventsQueueName);
  }

  async getQueueUrl(queueName: string): Promise<string> {
    const cached = this.queueUrlCache.get(queueName);
    if (cached) return cached;

    try {
      const res = await this.client.send(
        new GetQueueUrlCommand({ QueueName: queueName }),
      );
      if (res.QueueUrl) {
        this.queueUrlCache.set(queueName, res.QueueUrl);
        return res.QueueUrl;
      }
    } catch {
      // Queue might not exist yet in LocalStack; create it on demand
    }

    return await this.getOrCreateFifoQueue(queueName);
  }

  async sendMessage(params: SqsPublishParams): Promise<string | undefined> {
    const queueUrl = await this.getQueueUrl(params.queueName);
    const isFifo = params.queueName.endsWith(".fifo");

    const res = await this.client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: params.body,
        ...(isFifo
          ? {
              MessageGroupId: params.messageGroupId,
              MessageDeduplicationId: params.messageDeduplicationId,
            }
          : {}),
      }),
    );

    return res.MessageId;
  }

  async receiveMessages(
    queueName: string,
    maxMessages = 10,
    waitTimeSeconds = 1,
    visibilityTimeout = 30,
  ): Promise<Message[]> {
    const queueUrl = await this.getQueueUrl(queueName);
    const res = await this.client.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: maxMessages,
        WaitTimeSeconds: waitTimeSeconds,
        VisibilityTimeout: visibilityTimeout,
        AttributeNames: ["All"],
        MessageAttributeNames: ["All"],
      }),
    );
    return res.Messages ?? [];
  }

  async deleteMessage(queueName: string, receiptHandle: string): Promise<void> {
    const queueUrl = await this.getQueueUrl(queueName);
    await this.client.send(
      new DeleteMessageCommand({
        QueueUrl: queueUrl,
        ReceiptHandle: receiptHandle,
      }),
    );
  }

  async changeMessageVisibility(
    queueName: string,
    receiptHandle: string,
    visibilityTimeoutSeconds: number,
  ): Promise<void> {
    const queueUrl = await this.getQueueUrl(queueName);
    await this.client.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: queueUrl,
        ReceiptHandle: receiptHandle,
        VisibilityTimeout: Math.max(0, Math.floor(visibilityTimeoutSeconds)),
      }),
    );
  }

  async purgeQueue(queueName: string): Promise<void> {
    const queueUrl = await this.getQueueUrl(queueName);
    try {
      await this.client.send(new PurgeQueueCommand({ QueueUrl: queueUrl }));
    } catch {
      // Drain manually if PurgeQueue is throttled by LocalStack
      for (let i = 0; i < 10; i++) {
        const msgs = await this.receiveMessages(queueName, 10, 0, 1);
        if (msgs.length === 0) break;
        for (const m of msgs) {
          if (m.ReceiptHandle) {
            await this.deleteMessage(queueName, m.ReceiptHandle);
          }
        }
      }
    }
  }

  async checkHealth(): Promise<boolean> {
    try {
      await this.getQueueUrl(this.wagerQueueName);
      return true;
    } catch (err) {
      this.logger.error("SQS readiness check failed", {}, err);
      return false;
    }
  }

  private async getOrCreateFifoQueue(
    queueName: string,
    extraAttributes: Record<string, string> = {},
  ): Promise<string> {
    const isFifo = queueName.endsWith(".fifo");
    const res = await this.client.send(
      new CreateQueueCommand({
        QueueName: queueName,
        Attributes: {
          ...(isFifo
            ? {
                FifoQueue: "true",
                ContentBasedDeduplication: "false",
              }
            : {}),
          ...extraAttributes,
        },
      }),
    );

    const url = res.QueueUrl!;
    this.queueUrlCache.set(queueName, url);
    return url;
  }
}
