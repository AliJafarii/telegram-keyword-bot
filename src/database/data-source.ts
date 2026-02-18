import 'reflect-metadata';
import { DataSource } from 'typeorm';
import { config as loadEnv } from 'dotenv';
import { UserEntity } from '../entities/user.entity';
import { SearchEntity } from '../entities/search.entity';
import { ChannelMatchEntity } from '../entities/channel-match.entity';
import { CrawlStepEntity } from '../entities/crawl-step.entity';
import { SearchLinkEntity } from '../entities/search-link.entity';

loadEnv({ path: process.env.ENV_FILE || '.env.production' });

export const AppDataSource = new DataSource({
  type: 'oracle',
  username: process.env.ORACLE_USER,
  password: process.env.ORACLE_PASSWORD,
  connectString: process.env.ORACLE_CONNECT_STRING,
  synchronize: process.env.ORACLE_SYNCHRONIZE !== 'false',
  logging: false,
  entities: [UserEntity, SearchEntity, ChannelMatchEntity, CrawlStepEntity, SearchLinkEntity]
});
