import { MikroORM } from "@mikro-orm/postgresql";
import { buildMikroOrmOptions } from "./mikro-orm.config";

async function run() {
  const direction = process.argv[2] ?? "up";
  const orm = await MikroORM.init(buildMikroOrmOptions());
  try {
    const migrator = orm.getMigrator();
    if (direction === "down") {
      await migrator.down();
      console.log("Migration DOWN executed successfully.");
    } else {
      await migrator.up();
      console.log("Migration UP executed successfully.");
    }
  } finally {
    await orm.close(true);
  }
}

run().catch((err) => {
  console.error("Migration CLI failed:", err);
  process.exit(1);
});
