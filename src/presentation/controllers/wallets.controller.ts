import {
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  HttpStatus,
  Inject,
  Param,
  Post,
  Query,
} from "@nestjs/common";
import { randomUUID } from "node:crypto";
import {
  CreateWalletOutput,
  CreateWalletUseCase,
} from "../../application/use-cases/create-wallet.use-case";
import {
  PaginatedLedgerDto,
  QueryWageringUseCase,
  WalletDetailsDto,
} from "../../application/use-cases/query-wagering.use-case";
import {
  ReconcileWalletOutput,
  ReconcileWalletUseCase,
} from "../../application/use-cases/reconcile-wallet.use-case";
import { MoneyProps } from "../../domain/value-objects/money";

export interface CreateWalletRequestBody {
  id?: string;
  playerId: string;
  initialBalance: MoneyProps;
}

@Controller("wallets")
export class WalletsController {
  constructor(
    @Inject(CreateWalletUseCase)
    private readonly createWalletUseCase: CreateWalletUseCase,
    @Inject(QueryWageringUseCase)
    private readonly queryUseCase: QueryWageringUseCase,
    @Inject(ReconcileWalletUseCase)
    private readonly reconcileUseCase: ReconcileWalletUseCase,
  ) {}

  @Post()
  @HttpCode(HttpStatus.CREATED)
  async createWallet(
    @Body() body: CreateWalletRequestBody,
    @Headers("x-correlation-id") correlationId?: string,
  ): Promise<CreateWalletOutput> {
    return await this.createWalletUseCase.execute({
      id: body?.id,
      playerId: body?.playerId,
      initialBalance: body?.initialBalance,
      correlationId: correlationId ?? randomUUID(),
    });
  }

  @Get(":walletId")
  async getWallet(@Param("walletId") walletId: string): Promise<WalletDetailsDto> {
    return await this.queryUseCase.getWalletById(walletId);
  }

  @Get(":walletId/ledger")
  async getWalletLedger(
    @Param("walletId") walletId: string,
    @Query("cursor") cursor?: string,
    @Query("limit") limit?: string,
  ): Promise<PaginatedLedgerDto> {
    return await this.queryUseCase.getWalletLedger({
      walletId,
      cursor,
      limit: limit !== undefined ? Number(limit) : 50,
    });
  }

  @Post(":walletId/reconciliation")
  @HttpCode(HttpStatus.OK)
  async reconcileWallet(
    @Param("walletId") walletId: string,
    @Headers("x-correlation-id") correlationId?: string,
  ): Promise<ReconcileWalletOutput> {
    return await this.reconcileUseCase.execute(
      walletId,
      correlationId ?? randomUUID(),
    );
  }
}
