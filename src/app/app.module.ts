import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import configuration from '../config/configuration';
import { LoggerService } from '../common/logger.service';
import { SearchModule } from '../search/search.module';
import { BotModule } from '../bot/bot.module';
import { typeOrmConfig } from '../database/typeorm.config';
import { StorageModule } from '../storage/storage.module';
import { HealthController } from './health.controller';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: process.env.ENV_FILE || '.env.production',
      load: [configuration]
    }),
    TypeOrmModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (config: ConfigService) => typeOrmConfig(config)
    }),
    StorageModule,
    SearchModule,
    BotModule
  ],
  controllers: [HealthController],
  providers: [LoggerService],
  exports: [LoggerService]
})
export class AppModule {}
