import 'reflect-metadata';
import { AppDataSource } from './database/data-source';

async function migrate() {
  await AppDataSource.initialize();
  // TypeORM synchronize is controlled by ORACLE_SYNCHRONIZE. This script intentionally
  // does not read from MongoDB; MongoDB is no longer part of the runtime path.
  await AppDataSource.destroy();
  // eslint-disable-next-line no-console
  console.log('Oracle schema check completed. MongoDB migration is disabled/not used.');
}

migrate().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('Oracle schema check failed', err);
  process.exit(1);
});
