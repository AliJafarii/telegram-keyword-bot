import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createClient, RedisClientType } from 'redis';
import { LoggerService } from '../common/logger.service';

export type StoredChannelType = 'channel' | 'group' | 'bot' | 'unknown';

export interface StoreMatchInput {
  searchRef: string;
  searchId?: number;
  keyword: string;
  channelKey: string;
  channelType: StoredChannelType;
  channelLink?: string;
  messageId: number;
  messageLink?: string;
  messageText: string;
  messageDate?: number;
  relatedLinks: string[];
}

export interface StoredMatch extends StoreMatchInput {}

@Injectable()
export class RedisStoreService implements OnModuleInit, OnModuleDestroy {
  private readonly redis: RedisClientType;
  private readonly prefix: string;
  private connected = false;
  private readonly ttlSeconds: number;

  constructor(
    private readonly config: ConfigService,
    private readonly logger: LoggerService
  ) {
    const url = this.config.get<string>('redisUrl') || 'redis://127.0.0.1:6379';
    this.prefix = this.config.get<string>('redisPrefix') || 'tkb';
    this.ttlSeconds = Number(this.config.get<number>('redisTtlSeconds') || 7 * 24 * 60 * 60);
    this.redis = createClient({ url });
    this.redis.on('error', (err) => {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.warn('Redis client error', { error: message });
    });
  }

  async onModuleInit() {
    try {
      await this.redis.connect();
      this.connected = true;
      this.logger.log('Redis connected');
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.warn('Redis connect failed', { error: message });
      this.connected = false;
    }
  }

  async onModuleDestroy() {
    if (!this.connected) return;
    try {
      await this.redis.quit();
    } catch {
      // ignore quit failures
    }
  }

  private keyRun(searchRef: string): string {
    return `${this.prefix}:run:${searchRef}`;
  }

  private keyMatches(searchRef: string): string {
    return `${this.prefix}:matches:${searchRef}`;
  }

  private fieldMatch(channelKey: string, messageId: number): string {
    return `${channelKey}:${messageId}`;
  }

  private async ensureConnected(): Promise<boolean> {
    if (this.connected) return true;
    try {
      await this.redis.connect();
      this.connected = true;
      return true;
    } catch {
      return false;
    }
  }

  async upsertSearchRun(searchRef: string, keyword: string) {
    if (!(await this.ensureConnected())) return;
    const key = this.keyRun(searchRef);
    await this.redis.hSet(key, {
      searchRef,
      keyword,
      status: 'running',
      startedAt: String(Date.now())
    });
    await this.redis.expire(key, this.ttlSeconds);
  }

  async completeSearchRun(searchRef: string, summary: Record<string, unknown>) {
    if (!(await this.ensureConnected())) return;
    const key = this.keyRun(searchRef);
    await this.redis.hSet(key, {
      status: 'done',
      finishedAt: String(Date.now()),
      summaryJson: JSON.stringify(summary)
    });
    await this.redis.expire(key, this.ttlSeconds);
  }

  async failSearchRun(searchRef: string, error: string) {
    if (!(await this.ensureConnected())) return;
    const key = this.keyRun(searchRef);
    await this.redis.hSet(key, {
      status: 'failed',
      finishedAt: String(Date.now()),
      summaryJson: JSON.stringify({ error })
    });
    await this.redis.expire(key, this.ttlSeconds);
  }

  async upsertMatch(input: StoreMatchInput) {
    if (!(await this.ensureConnected())) return;
    const key = this.keyMatches(input.searchRef);
    const field = this.fieldMatch(input.channelKey, input.messageId);
    await this.redis.hSet(key, field, JSON.stringify(input));
    await this.redis.expire(key, this.ttlSeconds);
  }

  async getClientLinks(searchRef: string): Promise<string[]> {
    if (!(await this.ensureConnected())) return [];
    const key = this.keyMatches(searchRef);
    const rows = await this.redis.hVals(key);
    const out = new Set<string>();

    for (const raw of rows) {
      let row: StoredMatch | null = null;
      try {
        row = JSON.parse(raw) as StoredMatch;
      } catch {
        row = null;
      }
      if (!row) continue;

      if (row.channelType === 'bot') {
        if (row.channelLink) out.add(row.channelLink);
        continue;
      }
      if (row.messageLink) out.add(row.messageLink);
      for (const related of row.relatedLinks || []) {
        const match = related.match(/^https:\/\/t\.me\/([A-Za-z0-9_]+)$/i);
        if (match?.[1] && /bot$/i.test(match[1])) out.add(related);
      }
    }
    return Array.from(out);
  }

  async getAllMatches(searchRef: string): Promise<StoredMatch[]> {
    if (!(await this.ensureConnected())) return [];
    const key = this.keyMatches(searchRef);
    const rows = await this.redis.hVals(key);
    const out: StoredMatch[] = [];
    for (const raw of rows) {
      try {
        out.push(JSON.parse(raw) as StoredMatch);
      } catch {
        // ignore corrupted rows
      }
    }
    return out;
  }
}
