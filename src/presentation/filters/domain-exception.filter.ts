import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
  Inject,
} from "@nestjs/common";
import { Response, Request } from "express";
import {
  CurrencyMismatchError,
  DomainError,
  DuplicateWalletError,
  EntityNotFoundError,
  FailureCode,
  IdempotencyConflictError,
  InsufficientFundsError,
  InvalidMoneyError,
  InvalidPayloadError,
  InvalidTransactionStateError,
  LedgerImbalanceError,
  NegativeBalanceReversalError,
} from "../../domain/errors/domain-errors";
import { StructuredLogger } from "../../infrastructure/observability/logger.service";

@Catch()
export class GlobalDomainExceptionFilter implements ExceptionFilter {
  constructor(
    @Inject(StructuredLogger) private readonly logger: StructuredLogger,
  ) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const ctx = host.switchToHttp();
    const response = ctx.getResponse<Response>();
    const request = ctx.getRequest<Request>();

    const correlationId =
      (request.headers?.["x-correlation-id"] as string | undefined) ?? undefined;

    if (exception instanceof DomainError) {
      const status = this.mapDomainErrorToHttpStatus(exception);
      response.status(status).json({
        error: exception.name,
        failureCode: exception.code,
        message: exception.message,
        ...(correlationId ? { correlationId } : {}),
      });
      return;
    }

    if (exception instanceof HttpException) {
      const status = exception.getStatus();
      const body = exception.getResponse();
      response.status(status).json(
        typeof body === "object" && body !== null
          ? {
              failureCode: FailureCode.INVALID_PAYLOAD,
              ...body,
            }
          : {
              failureCode: FailureCode.INVALID_PAYLOAD,
              message: String(body),
            },
      );
      return;
    }

    this.logger.error(
      "Unhandled infrastructure exception in HTTP layer",
      {
        correlationId,
        path: request?.url,
        method: request?.method,
      },
      exception,
    );

    response.status(HttpStatus.SERVICE_UNAVAILABLE).json({
      error: "InfrastructureError",
      failureCode: FailureCode.INFRASTRUCTURE_FAILURE,
      message: "Transient infrastructure failure; safe to retry with same Idempotency-Key",
      ...(correlationId ? { correlationId } : {}),
    });
  }

  private mapDomainErrorToHttpStatus(error: DomainError): number {
    if (
      error instanceof InvalidMoneyError ||
      error instanceof InvalidPayloadError
    ) {
      return HttpStatus.BAD_REQUEST; // 400
    }

    if (error instanceof EntityNotFoundError) {
      return HttpStatus.NOT_FOUND; // 404
    }

    if (
      error instanceof DuplicateWalletError ||
      error instanceof IdempotencyConflictError ||
      error instanceof InvalidTransactionStateError
    ) {
      return HttpStatus.CONFLICT; // 409
    }

    if (
      error instanceof InsufficientFundsError ||
      error instanceof NegativeBalanceReversalError ||
      error instanceof CurrencyMismatchError ||
      error instanceof LedgerImbalanceError
    ) {
      return HttpStatus.UNPROCESSABLE_ENTITY; // 422
    }

    return HttpStatus.BAD_REQUEST;
  }
}
