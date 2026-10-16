import { MikroOrmModule } from "@mikro-orm/nestjs";
import { Options } from "@mikro-orm/postgresql";
import { DynamicModule, Module } from "@nestjs/common";
import { APP_FILTER, APP_GUARD } from "@nestjs/core";
import { CreateWalletUseCase } from "./application/use-cases/create-wallet.use-case";
import { ProcessWagerTransactionUseCase } from "./application/use-cases/process-wager-transaction.use-case";
import { QueryWageringUseCase } from "./application/use-cases/query-wagering.use-case";
import { ReconcileWalletUseCase } from "./application/use-cases/reconcile-wallet.use-case";
import { OutboxPublisherWorker } from "./application/workers/outbox-publisher.worker";
import { PendingReferenceRetryWorker } from "./application/workers/pending-reference-retry.worker";
import { SqsWagerConsumer } from "./application/workers/sqs-wager.consumer";
import { buildMikroOrmOptions } from "./infrastructure/database/mikro-orm.config";
import { SqsClientService } from "./infrastructure/messaging/sqs-client.service";
import { StructuredLogger } from "./infrastructure/observability/logger.service";
import { MetricsService } from "./infrastructure/observability/metrics.service";
import {
  NoopProviderIdentityAdapter,
  ProviderAuthGuard,
  ProviderIdentityPort,
} from "./presentation/auth/provider-identity.port";
import { HealthAndMetricsController } from "./presentation/controllers/health.controller";
import { WageringController } from "./presentation/controllers/wagering.controller";
import { WalletsController } from "./presentation/controllers/wallets.controller";
import { GlobalDomainExceptionFilter } from "./presentation/filters/domain-exception.filter";

@Module({})
export class AppModule {
  static register(ormOverrides: Partial<Options> = {}): DynamicModule {
    return {
      module: AppModule,
      imports: [MikroOrmModule.forRoot(buildMikroOrmOptions(ormOverrides))],
      controllers: [
        WalletsController,
        WageringController,
        HealthAndMetricsController,
      ],
      providers: [
        StructuredLogger,
        MetricsService,
        SqsClientService,
        CreateWalletUseCase,
        ProcessWagerTransactionUseCase,
        ReconcileWalletUseCase,
        QueryWageringUseCase,
        OutboxPublisherWorker,
        PendingReferenceRetryWorker,
        SqsWagerConsumer,
        {
          provide: ProviderIdentityPort,
          useClass: NoopProviderIdentityAdapter,
        },
        {
          provide: APP_GUARD,
          useClass: ProviderAuthGuard,
        },
        {
          provide: APP_FILTER,
          useClass: GlobalDomainExceptionFilter,
        },
      ],
      exports: [
        StructuredLogger,
        MetricsService,
        SqsClientService,
        CreateWalletUseCase,
        ProcessWagerTransactionUseCase,
        ReconcileWalletUseCase,
        QueryWageringUseCase,
        OutboxPublisherWorker,
        PendingReferenceRetryWorker,
        SqsWagerConsumer,
      ],
    };
  }
}
