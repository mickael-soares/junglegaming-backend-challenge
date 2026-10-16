import { PGlite } from "@electric-sql/pglite";
import { PGLiteSocketServer } from "@electric-sql/pglite-socket";
import { MikroORM } from "@mikro-orm/core";
import { INestApplication } from "@nestjs/common";
import { NestFactory } from "@nestjs/core";
import { createConnection } from "node:net";
import { AppModule } from "../../src/app.module";
import { SqsClientService } from "../../src/infrastructure/messaging/sqs-client.service";
import { EmbeddedSqsServer } from "./local-sqs-server";

export interface TestInfrastructureHandle {
  usingExternalPostgres: boolean;
  usingExternalSqs: boolean;
  dbHost: string;
  dbPort: number;
  sqsEndpoint: string;
  stop: () => Promise<void>;
}

let sharedInfra: TestInfrastructureHandle | null = null;
let pgliteDb: PGlite | null = null;
let pgliteSocketServer: PGLiteSocketServer | null = null;
let embeddedSqsServer: EmbeddedSqsServer | null = null;

async function isTcpPortOpen(host: string, port: number, timeoutMs = 600): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    const socket = createConnection({ host, port });
    const timer = setTimeout(() => {
      socket.destroy();
      resolve(false);
    }, timeoutMs);

    socket.once("connect", () => {
      clearTimeout(timer);
      socket.end();
      resolve(true);
    });

    socket.once("error", () => {
      clearTimeout(timer);
      socket.destroy();
      resolve(false);
    });
  });
}

/**
 * Ensures PostgreSQL and LocalStack SQS are reachable.
 * - If Docker Compose containers are running on DB_PORT (5432) and AWS_ENDPOINT (4566),
 *   connects directly to those containers.
 * - If not running, automatically boots a real PostgreSQL 16 wire-protocol server
 *   (PGlite + PGLiteSocketServer) and an AWS SQS wire-protocol server so all tests
 *   execute real SQL migrations, triggers, constraints, and SQS HTTP requests.
 */
export async function ensureTestInfrastructure(): Promise<TestInfrastructureHandle> {
  if (sharedInfra) {
    return sharedInfra;
  }

  process.env.LOG_SILENT = process.env.LOG_SILENT ?? "true";

  const configuredDbHost = process.env.DB_HOST ?? "127.0.0.1";
  const configuredDbPort = Number(process.env.DB_PORT ?? 5432);
  const configuredSqsUrl = new URL(
    process.env.AWS_ENDPOINT ?? "http://127.0.0.1:4566",
  );
  const configuredSqsPort = Number(configuredSqsUrl.port || 4566);

  const externalPgOpen = await isTcpPortOpen(configuredDbHost, configuredDbPort);
  const externalSqsOpen = await isTcpPortOpen(
    configuredSqsUrl.hostname,
    configuredSqsPort,
  );

  let dbHost = configuredDbHost;
  let dbPort = configuredDbPort;
  let sqsEndpoint = process.env.AWS_ENDPOINT ?? "http://127.0.0.1:4566";

  if (!externalPgOpen) {
    pgliteDb = await PGlite.create();
    pgliteSocketServer = new PGLiteSocketServer({
      db: pgliteDb,
      host: "127.0.0.1",
      port: 0, // bind ephemeral port to avoid conflicts
      maxConnections: 250,
    });
    await pgliteSocketServer.start();
    const connStr = pgliteSocketServer.getServerConn();
    const parts = connStr.split(":");
    dbHost = parts[0];
    dbPort = Number(parts[1]);

    process.env.DB_HOST = dbHost;
    process.env.DB_PORT = String(dbPort);
    process.env.DB_USER = "postgres";
    process.env.DB_PASSWORD = "";
    process.env.DB_NAME = "postgres";
  }

  if (!externalSqsOpen) {
    embeddedSqsServer = new EmbeddedSqsServer(0, "127.0.0.1");
    const sqsPort = await embeddedSqsServer.start();
    sqsEndpoint = `http://127.0.0.1:${sqsPort}`;
    process.env.AWS_ENDPOINT = sqsEndpoint;
  }

  sharedInfra = {
    usingExternalPostgres: externalPgOpen,
    usingExternalSqs: externalSqsOpen,
    dbHost,
    dbPort,
    sqsEndpoint,
    stop: async () => {
      if (embeddedSqsServer) {
        await embeddedSqsServer.stop();
        embeddedSqsServer = null;
      }
      if (pgliteSocketServer) {
        await pgliteSocketServer.stop();
        pgliteSocketServer = null;
      }
      if (pgliteDb) {
        await pgliteDb.close();
        pgliteDb = null;
      }
      migrationCompleted = false;
      sharedInfra = null;
    },
  };

  return sharedInfra;
}

export interface CreateAppInstanceOptions {
  instanceId?: string;
  enableSqsPolling?: boolean;
  enableOutboxWorker?: boolean;
  enablePendingRefWorker?: boolean;
  listenHttp?: boolean;
}

export interface RunningAppInstance {
  app: INestApplication;
  orm: MikroORM;
  sqsClient: SqsClientService;
  baseUrl: string;
  close: () => Promise<void>;
}

let migrationCompleted = false;
let migrationPromise: Promise<void> | null = null;

export async function createAppInstance(
  options: CreateAppInstanceOptions = {},
): Promise<RunningAppInstance> {
  await ensureTestInfrastructure();

  process.env.INSTANCE_ID = options.instanceId ?? "test-instance-1";
  process.env.SQS_POLL_ENABLED = options.enableSqsPolling ? "true" : "false";
  process.env.OUTBOX_WORKER_DISABLED = options.enableOutboxWorker
    ? "false"
    : "true";
  process.env.PENDING_REF_WORKER_DISABLED = options.enablePendingRefWorker
    ? "false"
    : "true";

  const app = await NestFactory.create(
    AppModule.register({
      pool: { min: 1, max: 10 },
    }),
    { logger: false },
  );

  const orm = app.get(MikroORM);
  if (!migrationCompleted) {
    if (!migrationPromise) {
      migrationPromise = orm
        .getMigrator()
        .up()
        .then(() => {
          migrationCompleted = true;
        })
        .finally(() => {
          migrationPromise = null;
        });
    }
    await migrationPromise;
  }

  const sqsClient = app.get(SqsClientService);
  await sqsClient.ensureQueuesExist();

  let baseUrl = "";
  if (options.listenHttp !== false) {
    await app.listen(0, "127.0.0.1");
    const url = await app.getUrl();
    baseUrl = url.replace("[::1]", "127.0.0.1").replace("localhost", "127.0.0.1");
  } else {
    await app.init();
  }

  return {
    app,
    orm,
    sqsClient,
    baseUrl,
    close: async () => {
      await app.close();
    },
  };
}

export async function resetDatabaseAndQueues(instance: RunningAppInstance): Promise<void> {
  const conn = instance.orm.em.getConnection();
  await conn.execute(`
    TRUNCATE TABLE
      outbox_messages,
      inbox_messages,
      wallet_ledger_entries,
      wager_transactions,
      wallets
    RESTART IDENTITY CASCADE;
  `);

  await instance.sqsClient.purgeQueue(instance.sqsClient.wagerQueueName);
  await instance.sqsClient.purgeQueue(instance.sqsClient.wagerDlqName);
  await instance.sqsClient.purgeQueue(instance.sqsClient.eventsQueueName);
}
