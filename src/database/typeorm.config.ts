import { ConfigService } from '@nestjs/config';
import { TypeOrmModuleOptions } from '@nestjs/typeorm';
import { UserEntity } from '../entities/user.entity';
import { SearchEntity } from '../entities/search.entity';
import { ChannelMatchEntity } from '../entities/channel-match.entity';
import { CrawlStepEntity } from '../entities/crawl-step.entity';
import { SearchLinkEntity } from '../entities/search-link.entity';
import { TelegramChatEntity } from '../entities/telegram-chat.entity';
import { TelegramInputLinkEntity } from '../entities/telegram-input-link.entity';
import { TelegramMessageLinkEntity } from '../entities/telegram-message-link.entity';

const entities = [
  UserEntity,
  SearchEntity,
  ChannelMatchEntity,
  CrawlStepEntity,
  SearchLinkEntity,
  TelegramChatEntity,
  TelegramInputLinkEntity,
  TelegramMessageLinkEntity
];

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

  if (databaseType === 'postgres') {
    return {
      type: 'postgres',
      host: config.get<string>('postgresHost') || '127.0.0.1',
      port: Number(config.get<number>('postgresPort') || 5432),
      database: config.get<string>('postgresDatabase') || 'telegram_keyword_bot',
      username: config.get<string>('postgresUser') || 'telegram_keyword_bot',
      password: config.get<string>('postgresPassword') || '',
      ssl: config.get<boolean>('postgresSsl') ? { rejectUnauthorized: false } : false,
      synchronize: config.get<boolean>('postgresSynchronize') !== false,
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
