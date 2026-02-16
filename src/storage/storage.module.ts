import { Global, Module } from '@nestjs/common';
import { LoggerService } from '../common/logger.service';
import { RedisStoreService } from './redis-store.service';

@Global()
@Module({
  providers: [RedisStoreService, LoggerService],
  exports: [RedisStoreService]
})
export class StorageModule {}
