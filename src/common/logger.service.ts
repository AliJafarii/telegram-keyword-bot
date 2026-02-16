import { Injectable, LoggerService as NestLoggerService } from '@nestjs/common';
import { createLogger, format, transports, Logger as WinstonLogger } from 'winston';
import * as fs from 'fs';
import * as path from 'path';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class LoggerService implements NestLoggerService {
  private readonly logger: WinstonLogger;

  constructor(private readonly config: ConfigService) {
    const logDir = path.join(process.cwd(), 'logs');
    if (!fs.existsSync(logDir)) {
      fs.mkdirSync(logDir);
    }

    this.logger = createLogger({
      level: this.config.get<string>('logLevel') || 'info',
      format: format.combine(format.timestamp(), format.json()),
      transports: [
        new transports.Console(),
        new transports.File({ filename: path.join(logDir, 'app.log') })
      ]
    });
  }

  log(message: string, ...meta: unknown[]) {
    this.logger.info(message, meta.length ? { meta } : undefined);
  }

  error(message: string, trace?: string, ...meta: unknown[]) {
    this.logger.error(message, { trace, ...(meta.length ? { meta } : {}) });
  }

  warn(message: string, ...meta: unknown[]) {
    this.logger.warn(message, meta.length ? { meta } : undefined);
  }

  debug(message: string, ...meta: unknown[]) {
    this.logger.debug(message, meta.length ? { meta } : undefined);
  }

  verbose(message: string, ...meta: unknown[]) {
    this.logger.verbose(message, meta.length ? { meta } : undefined);
  }
}
