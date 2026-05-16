import { ConfigService } from '@nestjs/config';
import { TypeOrmModuleOptions } from '@nestjs/typeorm';
import { UserEntity } from '../entities/user.entity';
import { SearchEntity } from '../entities/search.entity';
import { ChannelMatchEntity } from '../entities/channel-match.entity';
import { CrawlStepEntity } from '../entities/crawl-step.entity';
import { SearchLinkEntity } from '../entities/search-link.entity';

const entities = [UserEntity, SearchEntity, ChannelMatchEntity, CrawlStepEntity, SearchLinkEntity];

export const typeOrmConfig = (config: ConfigService): TypeOrmModuleOptions => {
  const databaseType = config.get<string>('databaseType') || 'sqlite';

  if (databaseType === 'oracle') {
    return {
      type: 'oracle',
      username: config.get<string>('oracleUser'),
      password: config.get<string>('oraclePassword'),
      connectString: config.get<string>('oracleConnectString'),
      synchronize: config.get<boolean>('oracleSynchronize'),
      logging: false,
      entities
    };
  }

  return {
    type: 'sqlite',
    database: config.get<string>('sqliteDatabase') || '/root/telegram-keyword-bot/data/telegram-keyword-bot.sqlite',
    synchronize: config.get<boolean>('databaseSynchronize') !== false,
    logging: false,
    entities
  };
};
