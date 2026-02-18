import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app/app.module';
import { LoggerService } from './common/logger.service';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { bufferLogs: true });
  const logger = app.get(LoggerService);
  app.useLogger(logger);
  logger.log('App bootstrap starting');
  app.enableShutdownHooks();
  const port = Number(process.env.PORT || 3000);
  await app.listen(port, '0.0.0.0');
  logger.log('Bot application initialized', { port });
}

bootstrap().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('Bootstrap failed', err);
  process.exit(1);
});
