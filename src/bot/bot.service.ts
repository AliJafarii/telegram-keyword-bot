import { Injectable, OnModuleInit } from '@nestjs/common';
import { Telegraf } from 'telegraf';
import { SocksProxyAgent } from 'socks-proxy-agent';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { LoggerService } from '../common/logger.service';
import { SearchService } from '../search/search.service';
import { UserEntity } from '../entities/user.entity';
import { SearchEntity } from '../entities/search.entity';
import { RedisStoreService } from '../storage/redis-store.service';

type ResultSource = 'web' | 'telegram' | 'links';

interface InMemorySearchResult {
  links: string[];
  createdAt: number;
}

@Injectable()
export class BotService implements OnModuleInit {
  private readonly bot: Telegraf;
  private readonly maxMessageLen = 4000;
  private readonly runningChats = new Set<number>();
  private readonly inMemoryResults = new Map<string, InMemorySearchResult>();
  private readonly inMemoryTtlMs = 12 * 60 * 60 * 1000;

  constructor(
    private readonly config: ConfigService,
    private readonly logger: LoggerService,
    private readonly searchService: SearchService,
    private readonly redisStore: RedisStoreService,
    @InjectRepository(UserEntity) private readonly userRepo: Repository<UserEntity>,
    @InjectRepository(SearchEntity) private readonly searchRepo: Repository<SearchEntity>
  ) {
    const token = this.config.get<string>('botToken') || '';
    const proxyUrl = process.env.SOCKS_PROXY;
    const agent = proxyUrl ? new SocksProxyAgent(proxyUrl) : undefined;
    this.bot = new Telegraf(token, agent ? { telegram: { agent } } : undefined);
  }

  async onModuleInit() {
    this.logger.log('Bot initializing...');
    this.registerHandlers();
    try {
      const me = await this.bot.telegram.getMe();
      this.logger.log(`Bot identity: @${me.username || 'unknown'}`);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.error('Bot getMe failed', message);
      throw err;
    }

    try {
      await this.bot.telegram.deleteWebhook();
      this.logger.log('Bot webhook cleared');
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.error('Bot deleteWebhook failed', message);
    }

    this.bot.catch((err) => {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.error('Bot error', message);
    });

    await this.bot.launch({ dropPendingUpdates: true });
    this.logger.log('Bot launched');
  }

  private registerHandlers() {
    this.bot.start(async (ctx) => {
      const tgId = String(ctx.from?.id || '');
      const username = ctx.from?.username || undefined;
      const existing = await this.userRepo.findOne({ where: { telegram_id: tgId } });
      if (!existing) {
        await this.userRepo.save({ telegram_id: tgId, username });
      }
      return ctx.reply('👋 Welcome! Use /seed <keyword> [iterations], /history, or just send a keyword.');
    });

    this.bot.command('search', async (ctx) => {
      const text = (ctx.message as any)?.text || '';
      return this.handleKeywordSearch(ctx as any, text, true);
    });

    this.bot.command('crawl', async (ctx) => {
      const text = (ctx.message as any)?.text || '';
      return this.handleKeywordSearch(ctx as any, text, true);
    });

    this.bot.command('seed', async (ctx) => {
      const text = (ctx.message as any)?.text || '';
      return this.handleKeywordSearch(ctx as any, text, true);
    });

    this.bot.command('history', async (ctx) => {
      return this.handleHistory(ctx as any);
    });

    this.bot.on('text', async (ctx) => {
      const text = ((ctx.message as any)?.text || '').trim();
      if (!text || text.startsWith('/')) return;
      return this.handleKeywordSearch(ctx as any, text, false);
    });

    this.bot.on('callback_query', async (ctx) => {
      const data = (ctx.callbackQuery as any).data as string;
      if (!data?.startsWith('pg:')) return ctx.answerCbQuery();
      await ctx.answerCbQuery();
      const [, searchId, source, pageStr] = data.split(':');
      const pageIdx = Number.parseInt(pageStr, 10);
      const chatId = ctx.callbackQuery.message?.chat.id as number;
      const msgId = ctx.callbackQuery.message?.message_id as number;
      if (!['web', 'telegram', 'links'].includes(source)) return;
      await this.sendPage(
        searchId,
        source as ResultSource,
        Number.isNaN(pageIdx) ? 0 : pageIdx,
        chatId,
        msgId
      );
    });
  }

  private parseKeywordAndIterations(rawInput: string, fromCommand: boolean): { keyword: string; iterations?: number } {
    let input = rawInput.trim();
    if (fromCommand) {
      input = input.replace(/^\/[a-z0-9_]+(?:@[a-z0-9_]+)?\s*/i, '').trim();
    }
    if (!input) return { keyword: '' };

    let keyword = input;
    let iterations: number | undefined;

    const flagMatch = input.match(/\s--?(?:iter|iterations?)(?:=|\s+)(\d+)\s*$/i);
    if (flagMatch && flagMatch.index !== undefined) {
      keyword = input.slice(0, flagMatch.index).trim();
      iterations = Number.parseInt(flagMatch[1], 10);
    } else if (fromCommand) {
      const parts = input.split(/\s+/).filter(Boolean);
      const last = parts[parts.length - 1];
      if (parts.length > 1 && /^\d+$/.test(last)) {
        iterations = Number.parseInt(last, 10);
        keyword = parts.slice(0, -1).join(' ');
      }
    }

    if (iterations !== undefined && Number.isNaN(iterations)) {
      iterations = undefined;
    }
    return { keyword: keyword.trim(), iterations };
  }

  private parseJsonStringArray(raw?: string | null): string[] {
    if (!raw) return [];
    try {
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      return parsed
        .filter((item): item is string => typeof item === 'string')
        .map((item) => item.trim())
        .filter(Boolean);
    } catch {
      return [];
    }
  }

  private setInMemoryLinks(searchRef: string, links: string[]) {
    const now = Date.now();
    this.inMemoryResults.set(searchRef, { links, createdAt: now });
    for (const [key, entry] of this.inMemoryResults) {
      if (now - entry.createdAt > this.inMemoryTtlMs) {
        this.inMemoryResults.delete(key);
      }
    }
  }

  private getInMemoryLinks(searchRef: string): string[] {
    const entry = this.inMemoryResults.get(searchRef);
    if (!entry) return [];
    if (Date.now() - entry.createdAt > this.inMemoryTtlMs) {
      this.inMemoryResults.delete(searchRef);
      return [];
    }
    return entry.links;
  }

  private toNumericSearchId(searchRef: string): number | null {
    if (!/^\d+$/.test(searchRef)) return null;
    const id = Number.parseInt(searchRef, 10);
    return Number.isNaN(id) ? null : id;
  }

  private async handleKeywordSearch(ctx: any, rawInput: string, fromCommand: boolean) {
    const chatId = ctx.chat?.id as number | undefined;
    if (!chatId) return;

    const { keyword, iterations } = this.parseKeywordAndIterations(rawInput, fromCommand);
    if (!keyword) {
      return ctx.reply('❗ Usage: /seed <keyword> [iterations]\nExample: /seed tasian 5');
    }

    const defaultIterations = Math.max(1, Number(this.config.get<number>('crawlIterations') || 5));
    const maxIterations = Math.max(1, Math.min(50, iterations ?? defaultIterations));

    if (this.runningChats.has(chatId)) {
      return ctx.reply('⏳ A crawl job is already running in this chat. Please wait.');
    }

    const tgId = String(ctx.from?.id || '');
    const user = await this.userRepo.findOne({ where: { telegram_id: tgId } });
    if (!user) return ctx.reply('❗ Please /start first.');

    this.runningChats.add(chatId);
    await ctx.reply(
      `🔎 Starting crawl for “${keyword}” (max iterations: ${maxIterations}).\nI will send paginated links when done.`
    );

    setImmediate(async () => {
      let searchRef = `tmp_${Date.now()}_${chatId}`;
      try {
        let searchDbId: number | null = null;
        try {
          const search = await this.searchRepo.save({
            user_id: user.id,
            keyword
          });
          searchDbId = search.id;
          searchRef = String(search.id);
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          this.logger.warn('Search row create failed; continuing without DB row', { error: message });
        }

        const crawl = await this.searchService.crawlKeywordIterative(
          keyword,
          searchDbId ?? 0,
          maxIterations,
          searchRef
        );
        const resultLinks = crawl.clientLinks.length ? crawl.clientLinks : crawl.links;
        this.setInMemoryLinks(searchRef, resultLinks);

        if (searchDbId !== null) {
          try {
            await this.searchRepo.update(searchDbId, {
              results_links: JSON.stringify(resultLinks),
              results_invites: JSON.stringify(crawl.invites)
            });
          } catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            this.logger.warn('Search row update failed; using in-memory results', { searchDbId, error: message });
          }
        }

        try {
          const persisted = await this.searchService.persistMatchesToOracle(searchRef);
          this.logger.log('Oracle channel matches persisted', {
            searchRef,
            total: persisted.total,
            inserted: persisted.inserted,
            updated: persisted.updated,
            failed: persisted.failed
          });
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          this.logger.warn('Oracle channel match persistence failed', { searchRef, error: message });
        }

        await this.bot.telegram.sendMessage(
          chatId,
          `✅ Crawl completed for “${keyword}”.\nIterations: ${crawl.iterations}\nSeed publics: ${crawl.seedPublics.length}\nChats processed: ${crawl.chatsProcessed}\nMessages stored: ${crawl.messagesStored}\nLinks found: ${resultLinks.length}`
        );

        if (!resultLinks.length) {
          await this.bot.telegram.sendMessage(chatId, '😕 No Telegram links were found for this keyword.');
          return;
        }

        await this.sendPage(searchRef, 'links', 0, chatId);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        this.logger.error('Keyword crawl failed', message);
        try {
          await this.redisStore.failSearchRun(searchRef, message);
        } catch {
          // ignore redis-store failure in user path
        }
        await this.bot.telegram.sendMessage(chatId, '⚠️ Crawl failed. Check logs and try again.');
      } finally {
        this.runningChats.delete(chatId);
      }
    });
  }

  private async handleHistory(ctx: any) {
    const chatId = ctx.chat?.id as number | undefined;
    if (!chatId) return;

    const tgId = String(ctx.from?.id || '');
    const user = await this.userRepo.findOne({ where: { telegram_id: tgId } });
    if (!user) return ctx.reply('❗ Please /start first.');

    const rows = await this.searchRepo.find({
      where: { user_id: user.id },
      order: { created_at: 'DESC' },
      take: 15
    });

    if (!rows.length) {
      return ctx.reply('No history yet. Use /seed <keyword> first.');
    }

    const lines: string[] = ['🗂 Your recent searches:\n'];
    for (const row of rows) {
      let linkCount = this.parseJsonStringArray(row.results_links).length;
      if (!linkCount) {
        linkCount = await this.searchService.countSearchLinks(row.id);
      }
      lines.push(
        `#${row.id} | ${row.keyword}\n${row.created_at.toISOString()} | links: ${linkCount}`
      );
    }

    await this.sendInChunks(chatId, lines.join('\n\n'));
  }

  private async sendInChunks(chatId: number, text: string, opts: Record<string, unknown> = {}) {
    for (let i = 0; i < text.length; i += this.maxMessageLen) {
      await this.bot.telegram.sendMessage(chatId, text.substring(i, i + this.maxMessageLen), opts);
    }
  }

  private safeSlice(str: string, n: number) {
    return Array.from(str).slice(0, n).join('');
  }

  private async sendPage(
    searchId: string,
    source: ResultSource,
    pageIdx: number,
    chatId: number,
    messageId?: number
  ) {
    const cachedLinks = this.getInMemoryLinks(searchId);
    let doc: SearchEntity | null = null;
    const numericId = this.toNumericSearchId(searchId);
    if (numericId !== null) {
      try {
        doc = await this.searchRepo.findOne({ where: { id: numericId } });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        this.logger.warn('Search row read failed; trying in-memory cache', { searchId, error: message });
      }
    }

    const dbLinks = this.parseJsonStringArray(doc?.results_links);
    let storedLinks: string[] = [];
    if (!cachedLinks.length) {
      try {
        storedLinks = await this.redisStore.getClientLinks(searchId);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        this.logger.warn('Redis store getClientLinks failed', { searchId, error: message });
      }
      if (!storedLinks.length && numericId !== null) {
        try {
          storedLinks = await this.searchService.getSearchLinksBySearchId(numericId);
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          this.logger.warn('Search link table fallback failed', { searchId, error: message });
        }
      }
    }
    const bestLinks = cachedLinks.length ? cachedLinks : (storedLinks.length ? storedLinks : dbLinks);
    if (!cachedLinks.length && bestLinks.length) {
      this.setInMemoryLinks(searchId, bestLinks);
    }

    const items = source === 'web'
      ? doc?.results_web || []
      : source === 'telegram'
        ? doc?.results_telegram || []
        : bestLinks;
    const pageSize = source === 'web'
      ? Number(this.config.get<number>('webPageSize') || 10)
      : source === 'telegram'
        ? Number(this.config.get<number>('tgPageSize') || 5)
        : Number(this.config.get<number>('linksPageSize') || 15);
    if (!items.length) {
      if (!messageId) {
        await this.bot.telegram.sendMessage(chatId, 'No cached results available for this page.');
      }
      return;
    }
    const totalPages = Math.ceil(items.length / pageSize);
    if (totalPages <= 0) return;
    const safePageIdx = Math.min(Math.max(pageIdx, 0), totalPages - 1);

    const slice = items.slice(safePageIdx * pageSize, (safePageIdx + 1) * pageSize);
    let text = source === 'web'
      ? `🌐 Web Results (Page ${safePageIdx + 1}/${totalPages}):\n\n`
      : source === 'telegram'
        ? `🤖 Telegram Results (Page ${safePageIdx + 1}/${totalPages}):\n\n`
        : `🔗 Discovered Telegram Links (Page ${safePageIdx + 1}/${totalPages}):\n\n`;

    if (source === 'web') {
      text += slice
        .map((r: any, i: number) => `${safePageIdx * pageSize + i + 1}. ${r.title}\n${r.url}`)
        .join('\n\n');
    } else if (source === 'telegram') {
      for (const chatEntry of slice as any[]) {
        text += `[${chatEntry.category.toUpperCase()}] @${chatEntry.username}\n`;
        text += `Title: ${chatEntry.title}\n`;
        if (chatEntry.messages.length) {
          for (const m of chatEntry.messages) {
            text += `• ${this.safeSlice(m.text, 40)}…\n  ${m.url}\n`;
          }
        } else {
          text += `🔗 https://t.me/${chatEntry.username}\n`;
        }
        text += '\n';
      }
    } else {
      text += (slice as string[])
        .map((link, i) => `${safePageIdx * pageSize + i + 1}. ${link}`)
        .join('\n');
    }

    const prevBtn = safePageIdx > 0
      ? { text: '◀️ Prev', callback_data: `pg:${searchId}:${source}:${safePageIdx - 1}` }
      : { text: ' ', callback_data: 'nop' };
    const nextBtn = safePageIdx < totalPages - 1
      ? { text: 'Next ▶️', callback_data: `pg:${searchId}:${source}:${safePageIdx + 1}` }
      : { text: ' ', callback_data: 'nop' };

    const opts = { reply_markup: { inline_keyboard: [[prevBtn, nextBtn]] } };

    if (messageId) {
      try {
        await this.bot.telegram.editMessageText(chatId, messageId, undefined, text, opts);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        this.logger.warn('Failed to edit pagination message', { searchId, source, message });
        await this.bot.telegram.sendMessage(chatId, text, opts);
      }
    } else if (text.length <= this.maxMessageLen) {
      await this.bot.telegram.sendMessage(chatId, text, opts);
    } else {
      await this.sendInChunks(chatId, text);
    }
  }
}
