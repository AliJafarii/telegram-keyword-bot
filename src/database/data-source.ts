import 'reflect-metadata';
import { DataSource } from 'typeorm';
import { config as loadEnv } from 'dotenv';
import { UserEntity } from '../entities/user.entity';
import { SearchEntity } from '../entities/search.entity';
import { ChannelMatchEntity } from '../entities/channel-match.entity';
import { CrawlStepEntity } from '../entities/crawl-step.entity';
import { SearchLinkEntity } from '../entities/search-link.entity';

loadEnv({ path: process.env.ENV_FILE || '.env.production' });

const entities = [UserEntity, SearchEntity, ChannelMatchEntity, CrawlStepEntity, SearchLinkEntity];
const databaseType = process.env.DATABASE_TYPE || 'sqlite';

const buildDataSource = (): DataSource => {
  if (databaseType === 'oracle') {
    return new DataSource({
      type: 'oracle',
      username: process.env.ORACLE_USER,
      password: process.env.ORACLE_PASSWORD,
      connectString: process.env.ORACLE_CONNECT_STRING,
      synchronize: process.env.ORACLE_SYNCHRONIZE !== 'false',
      logging: false,
      entities
    });
  }

  if (databaseType === 'postgres') {
    return new DataSource({
      type: 'postgres',
      host: process.env.POSTGRES_HOST || '127.0.0.1',
      port: Number(process.env.POSTGRES_PORT || 5432),
      database: process.env.POSTGRES_DATABASE || 'telegram_keyword_bot',
      username: process.env.POSTGRES_USER || 'telegram_keyword_bot',
      password: process.env.POSTGRES_PASSWORD || '',
      ssl: process.env.POSTGRES_SSL === 'true' ? { rejectUnauthorized: false } : false,
      synchronize: process.env.POSTGRES_SYNCHRONIZE !== 'false',
      logging: false,
      entities
    });
  }

  return new DataSource({
    type: 'sqlite',
    database: process.env.SQLITE_DATABASE || '/root/telegram-keyword-bot/data/telegram-keyword-bot.sqlite',
    synchronize: process.env.DATABASE_SYNCHRONIZE !== 'false',
    logging: false,
    entities
  });
};

export const AppDataSource = buildDataSource();
