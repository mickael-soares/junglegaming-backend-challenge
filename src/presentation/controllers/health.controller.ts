import { EntityManager } from "@mikro-orm/postgresql";
import {
  Controller,
  Get,
  Header,
  HttpStatus,
  Inject,
  Res,
} from "@nestjs/common";
import type { Response } from "express";
import { SqsClientService } from "../../infrastructure/messaging/sqs-client.service";
import {
  MetricsService,
  type MetricsSnapshot,
} from "../../infrastructure/observability/metrics.service";
import { Public } from "../auth/provider-identity.port";

@Controller()
@Public()
export class HealthAndMetricsController {
  constructor(
    @Inject(EntityManager) private readonly em: EntityManager,
    @Inject(SqsClientService) private readonly sqsClient: SqsClientService,
    @Inject(MetricsService) private readonly metrics: MetricsService,
  ) {}

  @Get("health/live")
  getLiveness() {
    return {
      status: "ok",
      timestamp: new Date().toISOString(),
    };
  }

  @Get("health/ready")
  async getReadiness(@Res({ passthrough: true }) res: Response) {
    let postgresUp = false;
    try {
      await this.em.getConnection().execute("SELECT 1");
      postgresUp = true;
    } catch {
      postgresUp = false;
    }

    const sqsUp = await this.sqsClient.checkHealth();
    const isReady = postgresUp && sqsUp;

    res.status(isReady ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE);
    return {
      status: isReady ? "ready" : "degraded",
      checks: {
        postgres: postgresUp ? "up" : "down",
        sqs: sqsUp ? "up" : "down",
      },
      timestamp: new Date().toISOString(),
    };
  }

  @Get("metrics")
  @Header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
  getPrometheusMetrics(): string {
    return this.metrics.toPrometheusText();
  }

  @Get("metrics/json")
  getJsonMetrics(): MetricsSnapshot {
    return this.metrics.getSnapshot();
  }
}
