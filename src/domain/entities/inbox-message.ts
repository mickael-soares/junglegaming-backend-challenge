import { InvalidPayloadError } from "../errors/domain-errors";

export interface ReceiveInboxProps {
  messageId: string;
  consumerName: string;
  payloadHash: string;
  receivedAt?: Date;
}

export interface InboxMessageState {
  messageId: string;
  consumerName: string;
  payloadHash: string;
  receivedAt: Date;
  processedAt?: Date;
}

export class InboxMessage {
  private constructor(
    public readonly messageId: string,
    public readonly consumerName: string,
    public readonly payloadHash: string,
    public readonly receivedAt: Date,
    private _processedAt?: Date,
  ) {}

  static receive(props: ReceiveInboxProps): InboxMessage {
    if (!props.messageId || typeof props.messageId !== "string") {
      throw new InvalidPayloadError("InboxMessage messageId is required");
    }
    if (!props.consumerName || typeof props.consumerName !== "string") {
      throw new InvalidPayloadError("InboxMessage consumerName is required");
    }
    if (!props.payloadHash || typeof props.payloadHash !== "string") {
      throw new InvalidPayloadError("InboxMessage payloadHash is required");
    }

    return new InboxMessage(
      props.messageId,
      props.consumerName,
      props.payloadHash,
      props.receivedAt ?? new Date(),
      undefined,
    );
  }

  static rehydrate(state: InboxMessageState): InboxMessage {
    return new InboxMessage(
      state.messageId,
      state.consumerName,
      state.payloadHash,
      state.receivedAt,
      state.processedAt,
    );
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  isProcessed(): boolean {
    return this._processedAt !== undefined && this._processedAt !== null;
  }

  markProcessed(at: Date): void {
    this._processedAt = at;
  }
}
