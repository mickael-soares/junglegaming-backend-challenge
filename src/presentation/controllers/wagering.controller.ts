import {
  Body,
  Controller,
  Get,
  Headers,
  HttpStatus,
  Inject,
  Param,
  Post,
  Res,
} from "@nestjs/common";
import type { Response } from "express";
import { randomUUID } from "node:crypto";
import {
  ProcessWagerTransactionUseCase,
} from "../../application/use-cases/process-wager-transaction.use-case";
import {
  QueryWageringUseCase,
  TransactionDetailsDto,
} from "../../application/use-cases/query-wagering.use-case";
import { WagerTransactionStatus } from "../../domain/entities/wager-transaction";
import { MoneyProps } from "../../domain/value-objects/money";

export interface SubmitWagerTransactionRequestBody {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
}

@Controller()
export class WageringController {
  constructor(
    @Inject(ProcessWagerTransactionUseCase)
    private readonly processWagerUseCase: ProcessWagerTransactionUseCase,
    @Inject(QueryWageringUseCase)
    private readonly queryUseCase: QueryWageringUseCase,
  ) {}

  @Post("wagering/transactions")
  async submitTransaction(
    @Headers("idempotency-key") idempotencyKey: string | undefined,
    @Headers("x-correlation-id") correlationId: string | undefined,
    @Body() body: SubmitWagerTransactionRequestBody,
    @Res({ passthrough: true }) res: Response,
  ) {
    const output = await this.processWagerUseCase.execute({
      idempotencyKey: idempotencyKey ?? "",
      providerId: body?.providerId,
      externalTransactionId: body?.externalTransactionId,
      playerId: body?.playerId,
      walletId: body?.walletId,
      roundId: body?.roundId,
      gameId: body?.gameId,
      kind: body?.kind,
      money: body?.money,
      referenceExternalTransactionId: body?.referenceExternalTransactionId,
      correlationId: correlationId ?? randomUUID(),
    });

    if (output.status === WagerTransactionStatus.Processed) {
      res.status(HttpStatus.OK);
    } else if (
      output.status === WagerTransactionStatus.PendingReference ||
      output.status === WagerTransactionStatus.Pending
    ) {
      res.status(HttpStatus.ACCEPTED);
    } else if (output.status === WagerTransactionStatus.Rejected) {
      res.status(HttpStatus.UNPROCESSABLE_ENTITY);
    } else {
      res.status(HttpStatus.SERVICE_UNAVAILABLE);
    }

    return output;
  }

  @Get("wagering/transactions/:transactionId")
  async getTransactionById(
    @Param("transactionId") transactionId: string,
  ): Promise<TransactionDetailsDto> {
    return await this.queryUseCase.getTransactionById(transactionId);
  }

  @Get("providers/:providerId/wagering/transactions/:externalTransactionId")
  async getTransactionByProviderAndExternalId(
    @Param("providerId") providerId: string,
    @Param("externalTransactionId") externalTransactionId: string,
  ): Promise<TransactionDetailsDto> {
    return await this.queryUseCase.getTransactionByProviderAndExternalId(
      providerId,
      externalTransactionId,
    );
  }
}
