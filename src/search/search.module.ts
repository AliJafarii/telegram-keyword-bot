import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { SearchService } from './search.service';
import { LoggerService } from '../common/logger.service';
import { CrawlStepEntity } from '../entities/crawl-step.entity';
import { ChannelMatchEntity } from '../entities/channel-match.entity';
import { SearchLinkEntity } from '../entities/search-link.entity';

@Module({
  imports: [TypeOrmModule.forFeature([CrawlStepEntity, ChannelMatchEntity, SearchLinkEntity])],
  providers: [SearchService, LoggerService],
  exports: [SearchService]
})
export class SearchModule {}
