import { ConfigService } from '@nestjs/config';
import { TypeOrmModuleOptions } from '@nestjs/typeorm';
import { UserEntity } from '../entities/user.entity';
import { SearchEntity } from '../entities/search.entity';
import { ChannelMatchEntity } from '../entities/channel-match.entity';
import { CrawlStepEntity } from '../entities/crawl-step.entity';
import { SearchLinkEntity } from '../entities/search-link.entity';

export const typeOrmConfig = (config: ConfigService): TypeOrmModuleOptions => ({
  type: 'oracle',
  username: config.get<string>('oracleUser'),
  password: config.get<string>('oraclePassword'),
  connectString: config.get<string>('oracleConnectString'),
  synchronize: config.get<boolean>('oracleSynchronize'),
  logging: false,
  entities: [UserEntity, SearchEntity, ChannelMatchEntity, CrawlStepEntity, SearchLinkEntity]
});
