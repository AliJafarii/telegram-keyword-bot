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
  matchReason: 'keyword_hyperlink' | 'keyword_video';
  iterationNo: number;
  discoveredViaLink?: string;
  discoveredFromMessageLink?: string;
  discoveredFromChannel?: string;
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

  private normalizeTelegramLink(link: string): string {
    let normalized = link.trim();
    normalized = normalized.replace(/[)\],.;!?،؛]+$/g, '');
    normalized = normalized.replace(/^http:\/\//i, 'https://');
    if (/^(?:t|telegram)\.me\//i.test(normalized)) {
      normalized = `https://${normalized}`;
    }
    if (/^https?:\/\/(?:www\.)?(?:t|telegram)\.me\//i.test(normalized)) {
      normalized = normalized.replace(/^https?:\/\/(?:www\.)?(?:telegram\.me|t\.me)\//i, 'https://t.me/');
    }
    return normalized;
  }

  private parseMessageLink(link: string): string | null {
    const normalized = this.normalizeTelegramLink(link);
    const privateMatch = normalized.match(/^https:\/\/t\.me\/c\/(\d+)\/(\d+)(?:[/?#].*)?$/i);
    if (privateMatch) {
      const messageId = Number.parseInt(privateMatch[2], 10);
      if (!Number.isFinite(messageId) || messageId <= 0) return null;
      return `https://t.me/c/${privateMatch[1]}/${messageId}`;
    }

    const publicMatch = normalized.match(/^https:\/\/t\.me\/(?:s\/)?([A-Za-z0-9_]+)\/(\d+)(?:[/?#].*)?$/i);
    if (publicMatch) {
      const username = publicMatch[1];
      if (username.toLowerCase() === 'c') return null;
      const messageId = Number.parseInt(publicMatch[2], 10);
      if (!Number.isFinite(messageId) || messageId <= 0) return null;
      return `https://t.me/${username}/${messageId}`;
    }

    return null;
  }

  private parseBotLink(link: string): string | null {
    const normalized = this.normalizeTelegramLink(link);
    if (/^https:\/\/t\.me\/c\/\d+(?:\/\d+)?/i.test(normalized)) return null;
    if (/^https:\/\/t\.me\/(?:s\/)?[A-Za-z0-9_]+\/\d+/i.test(normalized)) return null;

    const usernameMatch = normalized.match(/^https:\/\/t\.me\/([A-Za-z0-9_]+)(?:[/?#].*)?$/i);
    if (!usernameMatch?.[1] || !/bot$/i.test(usernameMatch[1])) return null;
    if (!/[?#]/.test(normalized)) {
      return normalized.replace(/\/+$/, '');
    }
    return normalized;
  }

  private parseInviteOrRootLink(link: string): string | null {
    const normalized = this.normalizeTelegramLink(link);

    const plusMatch = normalized.match(/^https:\/\/t\.me\/\+([A-Za-z0-9_-]+)(?:[/?#].*)?$/i);
    if (plusMatch?.[1]) return `https://t.me/+${plusMatch[1]}`;

    const joinchatMatch = normalized.match(/^https:\/\/t\.me\/joinchat\/([A-Za-z0-9_-]+)(?:[/?#].*)?$/i);
    if (joinchatMatch?.[1]) return `https://t.me/joinchat/${joinchatMatch[1]}`;

    const privateRoot = normalized.match(/^https:\/\/t\.me\/c\/(\d+)\/?(?:[?#].*)?$/i);
    if (privateRoot?.[1]) return `https://t.me/c/${privateRoot[1]}`;

    if (/^https:\/\/t\.me\/(?:s\/)?[A-Za-z0-9_]+\/\d+/i.test(normalized)) return null;

    const usernameMatch = normalized.match(/^https:\/\/t\.me\/([A-Za-z0-9_]+)(?:[/?#].*)?$/i);
    if (!usernameMatch?.[1]) return null;
    if (/bot$/i.test(usernameMatch[1])) return null;
    if (usernameMatch[1].toLowerCase() === 'c') return null;
    return `https://t.me/${usernameMatch[1]}`;
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
        const botRoot = row.channelLink ? this.parseBotLink(row.channelLink) : null;
        if (botRoot) out.add(botRoot);
      } else {
        const messageLink = row.messageLink ? this.parseMessageLink(row.messageLink) : null;
        if (messageLink) out.add(messageLink);
      }

      const discoveredVia = row.discoveredViaLink ? this.parseInviteOrRootLink(row.discoveredViaLink) : null;
      if (discoveredVia) out.add(discoveredVia);

      for (const related of row.relatedLinks || []) {
        const botLink = this.parseBotLink(related);
        if (botLink) out.add(botLink);
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

  async assignSearchId(searchRef: string, searchId: number): Promise<number> {
    if (!(await this.ensureConnected())) return 0;
    if (!Number.isFinite(searchId) || searchId <= 0) return 0;

    const key = this.keyMatches(searchRef);
    const rows = await this.redis.hGetAll(key);
    const updates: Record<string, string> = {};
    let patched = 0;

    for (const [field, raw] of Object.entries(rows)) {
      try {
        const parsed = JSON.parse(raw) as StoredMatch;
        if (parsed.searchId === searchId) continue;
        parsed.searchId = searchId;
        updates[field] = JSON.stringify(parsed);
        patched += 1;
      } catch {
        // ignore corrupted rows
      }
    }

    if (patched > 0) {
      await this.redis.hSet(key, updates);
      await this.redis.expire(key, this.ttlSeconds);
    }
    return patched;
  }
}
