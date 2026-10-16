import { Migrator } from "@mikro-orm/migrations";
import { defineConfig, Options, PostgreSqlDriver } from "@mikro-orm/postgresql";
import { ALL_ENTITY_SCHEMAS } from "./entities/persistence-entities";
import { Migration20261015200843_InitialSchema } from "./migrations/Migration20261015200843_InitialSchema";

export function buildMikroOrmOptions(overrides: Partial<Options> = {}): Options {
  return defineConfig({
    driver: PostgreSqlDriver,
    host: process.env.DB_HOST ?? "localhost",
    port: Number(process.env.DB_PORT ?? 5432),
    user: process.env.DB_USER ?? "jungle",
    password: process.env.DB_PASSWORD ?? "jungle_secret",
    dbName: process.env.DB_NAME ?? "jungle_wagering",
    entities: ALL_ENTITY_SCHEMAS,
    extensions: [Migrator],
    migrations: {
      migrationsList: [
        {
          name: "Migration20261015200843_InitialSchema",
          class: Migration20261015200843_InitialSchema,
        },
      ],
      transactional: true,
      allOrNothing: true,
    },
    pool: {
      min: 2,
      max: Number(process.env.DB_POOL_MAX ?? 25),
    },
    allowGlobalContext: true,
    debug: process.env.DB_DEBUG === "true",
    ...overrides,
  });
}

export default buildMikroOrmOptions();
