import { Module } from '@nestjs/common';
import { BotService } from './bot.service';
import { SearchModule } from '../search/search.module';
import { LoggerService } from '../common/logger.service';
import { TypeOrmModule } from '@nestjs/typeorm';
import { UserEntity } from '../entities/user.entity';
import { SearchEntity } from '../entities/search.entity';

@Module({
  imports: [SearchModule, TypeOrmModule.forFeature([UserEntity, SearchEntity])],
  providers: [BotService, LoggerService]
})
export class BotModule {}
