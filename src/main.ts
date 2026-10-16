import "reflect-metadata";
import { MikroORM } from "@mikro-orm/postgresql";
import { NestFactory } from "@nestjs/core";
import { AppModule } from "./app.module";
import { StructuredLogger } from "./infrastructure/observability/logger.service";

async function bootstrap() {
  const app = await NestFactory.create(AppModule.register(), {
    logger: ["error", "warn"],
  });

  app.enableShutdownHooks();

  const logger = app.get(StructuredLogger);
  const orm = app.get(MikroORM);

  // Run migrations automatically on startup protected by a PostgreSQL advisory lock
  // so multiple instances booting simultaneously do not race on DDL.
  if (process.env.AUTO_MIGRATE !== "false") {
    const conn = orm.em.getConnection();
    await conn.execute("SELECT pg_advisory_lock(99887766)");
    try {
      await orm.getMigrator().up();
    } finally {
      await conn.execute("SELECT pg_advisory_unlock(99887766)");
    }
  }

  const port = Number(process.env.PORT ?? 3000);
  await app.listen(port);

  logger.info("Distributed Wagering Processor started", {
    port,
    instanceId: process.env.INSTANCE_ID ?? "instance-1",
  });

  const handleSignal = async (signal: string) => {
    logger.info(`Received ${signal}, initiating graceful shutdown...`);
    await app.close();
    process.exit(0);
  };

  process.once("SIGTERM", () => void handleSignal("SIGTERM"));
  process.once("SIGINT", () => void handleSignal("SIGINT"));
}

if (import.meta.main) {
  bootstrap().catch((err) => {
    console.error("Failed to bootstrap application:", err);
    process.exit(1);
  });
}
