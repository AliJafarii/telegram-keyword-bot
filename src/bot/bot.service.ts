import { Injectable, OnModuleInit } from '@nestjs/common';
import { Markup, Telegraf } from 'telegraf';
import { SocksProxyAgent } from 'socks-proxy-agent';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { LoggerService } from '../common/logger.service';
import { KeywordCrawlResultDto, SearchService } from '../search/search.service';
import { UserEntity } from '../entities/user.entity';
import { SearchEntity } from '../entities/search.entity';
import { RedisStoreService } from '../storage/redis-store.service';
import * as XLSX from 'xlsx';

type ResultSource = 'web' | 'telegram' | 'links';

interface InMemorySearchResult {
  links: string[];
  createdAt: number;
}

interface ParsedMessageLink {
  link: string;
  channelId: string;
  uid: number;
}

interface GroupedChannelLinks {
  channelId: string;
  links: string[];
}

interface CrawlRunResult {
  searchRef: string;
  crawl: KeywordCrawlResultDto;
  resultLinks: string[];
  displayLinks: ParsedMessageLink[];
}

interface ExportLinkRow {
  keyword?: string;
  type: string;
  channel: string;
  uid: string;
  link: string;
}

interface KeywordSheetBatch {
  inputSheetName: string;
  keywords: string[];
}

type ChatInputMode = 'keyword' | 'excel';

@Injectable()
export class BotService implements OnModuleInit {
  private readonly bot: Telegraf;
  private readonly maxMessageLen = 4000;
  private readonly runningChats = new Set<number>();
  private readonly inMemoryResults = new Map<string, InMemorySearchResult>();
  private readonly inMemoryTtlMs = 12 * 60 * 60 * 1000;
  private readonly pendingInputMode = new Map<number, ChatInputMode>();
  private readonly menuSearchOneKeyword = 'Search One Keyword';
  private readonly menuImportExcel = 'Import Excel File';
  private readonly mainMenuKeyboard = Markup.keyboard(
    [[this.menuSearchOneKeyword], [this.menuImportExcel]]
  ).resize();

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
    this.logger.log('Bot runtime profile', {
      envFile: process.env.ENV_FILE || '.env.production',
      proxyEnabled: Boolean(process.env.SOCKS_PROXY)
    });
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

    try {
      await this.bot.telegram.setMyCommands([
        { command: 'start', description: 'Open main menu' }
      ]);
      this.logger.log('Bot commands updated');
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.warn('Bot setMyCommands failed', { error: message });
    }

    this.bot.catch((err) => {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.error('Bot error', message);
    });

    this.bot.launch({ dropPendingUpdates: true })
      .then(() => this.logger.log('Bot launched'))
      .catch((err) => {
        const message = err instanceof Error ? err.message : String(err);
        this.logger.error('Bot launch failed', message);
      });

    this.logger.log('Bot launch requested');
  }

  private registerHandlers() {
    this.bot.start(async (ctx) => {
      const tgId = String(ctx.from?.id || '');
      const username = ctx.from?.username || undefined;
      const existing = await this.userRepo.findOne({ where: { telegram_id: tgId } });
      if (!existing) {
        await this.userRepo.save({ telegram_id: tgId, username });
      }
      if (ctx.chat?.id) {
        this.pendingInputMode.delete(ctx.chat.id);
      }
      return ctx.reply(
        '👋 Welcome! Choose one option from menu:',
        this.mainMenuKeyboard
      );
    });

    this.bot.on('text', async (ctx) => {
      const text = ((ctx.message as any)?.text || '').trim();
      if (!text || text.startsWith('/')) return;

      const chatId = ctx.chat?.id as number | undefined;
      if (!chatId) return;

      if (text === this.menuSearchOneKeyword) {
        this.pendingInputMode.set(chatId, 'keyword');
        return ctx.reply('🔎 Send one keyword to start crawling.', this.mainMenuKeyboard);
      }

      if (text === this.menuImportExcel) {
        this.pendingInputMode.set(chatId, 'excel');
        return ctx.reply('📄 Send an Excel file (.xlsx/.xls) with one keyword per row.', this.mainMenuKeyboard);
      }

      const mode = this.pendingInputMode.get(chatId);
      if (mode === 'excel') {
        return ctx.reply('📄 You selected Excel mode. Please upload .xlsx/.xls file.', this.mainMenuKeyboard);
      }
      if (mode === 'keyword') {
        this.pendingInputMode.delete(chatId);
      } else {
        return ctx.reply(
          'Choose one option from menu first.',
          this.mainMenuKeyboard
        );
      }

      return this.handleKeywordSearch(ctx as any, text, false);
    });

    this.bot.on('document', async (ctx) => {
      return this.handleKeywordFile(ctx as any);
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

  private parseKeywordRowsFromWorksheet(sheet: XLSX.WorkSheet): string[] {
    const rows = XLSX.utils.sheet_to_json(sheet, { header: 1, raw: false, blankrows: false }) as Array<Array<unknown>>;
    const out: string[] = [];
    for (const row of rows) {
      if (!Array.isArray(row) || !row.length) continue;
      const firstCell = row.find((cell) =>
        typeof cell === 'string' || typeof cell === 'number'
      );
      if (firstCell === undefined || firstCell === null) continue;
      const keyword = String(firstCell).trim();
      if (!keyword) continue;
      out.push(keyword);
    }
    return out;
  }

  private extractKeywordBatchesFromWorkbook(workbook: XLSX.WorkBook): KeywordSheetBatch[] {
    const out: KeywordSheetBatch[] = [];
    for (const sheetName of workbook.SheetNames || []) {
      const sheet = workbook.Sheets[sheetName];
      if (!sheet) continue;
      const sheetKeywords = this.parseKeywordRowsFromWorksheet(sheet);
      if (!sheetKeywords.length) continue;
      const dedupe = new Set<string>();
      for (const keyword of sheetKeywords) dedupe.add(keyword);
      const keywords = Array.from(dedupe);
      if (!keywords.length) continue;
      out.push({
        inputSheetName: sheetName,
        keywords
      });
    }
    return out;
  }

  private sanitizeSheetName(name: string, fallbackIndex: number): string {
    const original = String(name || '');
    const hasInvalidChars = /[:\\/?*\[\]]/.test(original);
    if (original && !hasInvalidChars && original.length <= 31) {
      // Keep exact input sheet title when it is valid in Excel.
      return original;
    }

    const base = original
      .trim()
      .replace(/[:\\/?*\[\]]/g, ' ')
      .replace(/\s+/g, ' ')
      .slice(0, 31)
      .trim();
    if (base) return base;
    return `sheet_${fallbackIndex + 1}`;
  }

  private parseMessageLink(link: string): ParsedMessageLink | null {
    const trimmed = this.normalizeTelegramLink(link);

    const privateMatch = trimmed.match(/^https:\/\/t\.me\/c\/(\d+)\/(\d+)(?:[/?#].*)?$/i);
    if (privateMatch) {
      const uid = Number.parseInt(privateMatch[2], 10);
      if (!Number.isFinite(uid)) return null;
      return {
        link: `https://t.me/c/${privateMatch[1]}/${uid}`,
        channelId: privateMatch[1],
        uid
      };
    }

    const publicMatch = trimmed.match(/^https:\/\/t\.me\/(?:s\/)?([A-Za-z0-9_]+)\/(\d+)(?:[/?#].*)?$/i);
    if (publicMatch) {
      if (publicMatch[1].toLowerCase() === 'c') return null;
      const uid = Number.parseInt(publicMatch[2], 10);
      if (!Number.isFinite(uid)) return null;
      return {
        link: `https://t.me/${publicMatch[1]}/${uid}`,
        channelId: publicMatch[1],
        uid
      };
    }

    return null;
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

  private parseBotLink(link: string): string | null {
    const normalized = this.normalizeTelegramLink(link);
    if (!/^https:\/\/t\.me\//i.test(normalized)) return null;
    if (/^https:\/\/t\.me\/c\/\d+\/\d+/i.test(normalized)) return null;
    if (/^https:\/\/t\.me\/(?:s\/)?[A-Za-z0-9_]+\/\d+/i.test(normalized)) return null;

    const usernameMatch = normalized.match(/^https:\/\/t\.me\/([A-Za-z0-9_]+)(?:[/?#].*)?$/i);
    if (!usernameMatch?.[1]) return null;
    if (!/bot$/i.test(usernameMatch[1])) return null;

    if (!/[?#]/.test(normalized)) {
      return normalized.replace(/\/+$/, '');
    }
    return normalized;
  }

  private parsePlainRootLink(link: string): string | null {
    const normalized = this.normalizeTelegramLink(link);
    if (!/^https:\/\/t\.me\//i.test(normalized)) return null;
    if (/^https:\/\/t\.me\/c\/\d+\/?(?:[?#].*)?$/i.test(normalized)) {
      const m = normalized.match(/^https:\/\/t\.me\/c\/(\d+)/i);
      return m ? `https://t.me/c/${m[1]}` : normalized;
    }
    if (/^https:\/\/t\.me\/c\/\d+\/\d+/i.test(normalized)) return null;
    if (/^https:\/\/t\.me\/(?:s\/)?[A-Za-z0-9_]+\/\d+/i.test(normalized)) return null;

    if (/^https:\/\/t\.me\/\+[A-Za-z0-9_-]+(?:[/?#].*)?$/i.test(normalized)) {
      const m = normalized.match(/^https:\/\/t\.me\/\+([A-Za-z0-9_-]+)/i);
      return m ? `https://t.me/+${m[1]}` : normalized;
    }
    if (/^https:\/\/t\.me\/joinchat\/[A-Za-z0-9_-]+(?:[/?#].*)?$/i.test(normalized)) {
      const m = normalized.match(/^https:\/\/t\.me\/joinchat\/([A-Za-z0-9_-]+)/i);
      return m ? `https://t.me/joinchat/${m[1]}` : normalized;
    }

    const usernameMatch = normalized.match(/^https:\/\/t\.me\/([A-Za-z0-9_]+)(?:[/?#].*)?$/i);
    if (!usernameMatch?.[1]) return null;
    if (/bot$/i.test(usernameMatch[1])) return null;
    if (usernameMatch[1].toLowerCase() === 'c') return null;
    return `https://t.me/${usernameMatch[1]}`;
  }

  private filterClientMessageLinks(rawLinks: string[]): ParsedMessageLink[] {
    const dedupe = new Map<string, ParsedMessageLink>();
    for (const link of rawLinks) {
      const parsed = this.parseMessageLink(link);
      if (!parsed) continue;
      dedupe.set(parsed.link, parsed);
    }
    return Array.from(dedupe.values()).sort((a, b) => {
      const channelCmp = a.channelId.localeCompare(b.channelId, undefined, { sensitivity: 'base' });
      if (channelCmp !== 0) return channelCmp;
      if (a.uid !== b.uid) return a.uid - b.uid;
      return a.link.localeCompare(b.link, undefined, { sensitivity: 'base' });
    });
  }

  private filterClientBotLinks(rawLinks: string[]): string[] {
    const dedupe = new Map<string, string>();
    for (const raw of rawLinks) {
      const parsed = this.parseBotLink(raw);
      if (!parsed) continue;
      dedupe.set(parsed.toLowerCase(), parsed);
    }
    return Array.from(dedupe.values()).sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
  }

  private filterClientPlainRootLinks(rawLinks: string[]): string[] {
    const dedupe = new Map<string, string>();
    for (const raw of rawLinks) {
      const parsed = this.parsePlainRootLink(raw);
      if (!parsed) continue;
      dedupe.set(parsed.toLowerCase(), parsed);
    }
    return Array.from(dedupe.values()).sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
  }

  private splitInviteAndRootLinks(links: string[]): { inviteLinks: string[]; rootLinks: string[] } {
    const inviteLinks: string[] = [];
    const rootLinks: string[] = [];
    for (const link of links) {
      if (/^https:\/\/t\.me\/\+/i.test(link) || /^https:\/\/t\.me\/joinchat\//i.test(link)) {
        inviteLinks.push(link);
      } else {
        rootLinks.push(link);
      }
    }
    return { inviteLinks, rootLinks };
  }

  private groupClientMessageLinks(items: ParsedMessageLink[]): GroupedChannelLinks[] {
    const grouped = new Map<string, ParsedMessageLink[]>();
    for (const item of items) {
      if (!grouped.has(item.channelId)) grouped.set(item.channelId, []);
      grouped.get(item.channelId)!.push(item);
    }

    return Array.from(grouped.entries())
      .map(([channelId, entries]) => ({
        channelId,
        links: entries
          .sort((a, b) => a.uid - b.uid)
          .map((entry) => entry.link)
      }))
      .sort((a, b) => a.channelId.localeCompare(b.channelId, undefined, { sensitivity: 'base' }));
  }

  private buildExportRows(links: string[], sourceKeyword?: string): ExportLinkRow[] {
    const rows = new Map<string, ExportLinkRow>();

    for (const raw of links) {
      const message = this.parseMessageLink(raw);
      if (message) {
        rows.set(message.link, {
          keyword: sourceKeyword,
          type: 'message',
          channel: message.channelId,
          uid: String(message.uid),
          link: message.link
        });
        continue;
      }

      const bot = this.parseBotLink(raw);
      if (bot) {
        const usernameMatch = bot.match(/^https:\/\/t\.me\/([A-Za-z0-9_]+)/i);
        rows.set(bot, {
          keyword: sourceKeyword,
          type: 'bot',
          channel: usernameMatch?.[1] || '',
          uid: '',
          link: bot
        });
        continue;
      }

      const root = this.parsePlainRootLink(raw);
      if (!root) continue;

      if (/^https:\/\/t\.me\/c\/\d+$/i.test(root)) {
        const match = root.match(/^https:\/\/t\.me\/c\/(\d+)$/i);
        rows.set(root, {
          keyword: sourceKeyword,
          type: 'private_root',
          channel: match?.[1] || '',
          uid: '',
          link: root
        });
        continue;
      }
      if (/^https:\/\/t\.me\/\+/i.test(root) || /^https:\/\/t\.me\/joinchat\//i.test(root)) {
        rows.set(root, {
          keyword: sourceKeyword,
          type: 'invite',
          channel: '',
          uid: '',
          link: root
        });
        continue;
      }

      const usernameMatch = root.match(/^https:\/\/t\.me\/([A-Za-z0-9_]+)$/i);
      rows.set(root, {
        keyword: sourceKeyword,
        type: 'chat_root',
        channel: usernameMatch?.[1] || '',
        uid: '',
        link: root
      });
    }

    return Array.from(rows.values()).sort((a, b) => {
      const t = a.type.localeCompare(b.type, undefined, { sensitivity: 'base' });
      if (t !== 0) return t;
      const c = a.channel.localeCompare(b.channel, undefined, { sensitivity: 'base' });
      if (c !== 0) return c;
      const k = (a.keyword || '').localeCompare(b.keyword || '', undefined, { sensitivity: 'base' });
      if (k !== 0) return k;
      const u = a.uid.localeCompare(b.uid, undefined, { sensitivity: 'base' });
      if (u !== 0) return u;
      return a.link.localeCompare(b.link, undefined, { sensitivity: 'base' });
    });
  }

  private async withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
    return Promise.race([
      promise,
      new Promise<T>((_, reject) =>
        setTimeout(() => reject(new Error(`${label} timeout after ${timeoutMs}ms`)), timeoutMs)
      )
    ]);
  }

  private async createSearchRowOrThrow(params: {
    userId: number;
    keyword: string;
    chatId: number;
  }): Promise<SearchEntity> {
    const { userId, keyword, chatId } = params;
    const timeoutMs = Math.max(1000, Number(this.config.get<number>('searchCreateTimeoutMs') || 5000));
    const maxAttempts = Math.max(1, Number(this.config.get<number>('searchCreateMaxAttempts') || 3));
    let lastError: string | null = null;

    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
      try {
        const row = await this.withTimeout(
          this.searchRepo.save({
            user_id: userId,
            keyword
          }),
          timeoutMs,
          `search_row_create_attempt_${attempt}`
        );
        if (attempt > 1) {
          this.logger.log('Search row create succeeded after retry', { chatId, keyword, attempt, searchId: row.id });
        }
        return row;
      } catch (err) {
        lastError = err instanceof Error ? err.message : String(err);
        this.logger.warn('Search row create attempt failed', {
          chatId,
          keyword,
          attempt,
          maxAttempts,
          error: lastError
        });
      }
    }

    throw new Error(
      `search_row_create failed after ${maxAttempts} attempt(s): ${lastError || 'unknown error'}`
    );
  }

  private async runCrawlAndPersist(params: {
    chatId: number;
    userId: number;
    keyword: string;
    maxIterations: number;
  }): Promise<CrawlRunResult> {
    const { chatId, userId, keyword, maxIterations } = params;
    const dbOpTimeoutMs = Math.max(10000, Number(this.config.get<number>('dbOpTimeoutMs') || 120000));
    const crawlTimeoutMs = Math.max(
      120000,
      Number(this.config.get<number>('crawlMaxRuntimeMs') || 300000) + 60000
    );

    let searchRef = `tmp_${Date.now()}_${chatId}`;
    this.logger.log('Crawl job started', { chatId, userId, keyword, maxIterations, searchRef });
    try {
      const search = await this.createSearchRowOrThrow({ userId, keyword, chatId });
      const searchDbId = search.id;
      searchRef = String(searchDbId);
      this.logger.log('Search row created', { searchDbId, keyword, chatId });

      const crawl = await this.withTimeout(
        this.searchService.crawlKeywordIterative(
          keyword,
          searchDbId,
          maxIterations,
          searchRef
        ),
        crawlTimeoutMs,
        'crawlKeywordIterative'
      );
      const resultLinks = crawl.clientLinks.length ? crawl.clientLinks : crawl.links;
      const displayLinks = this.filterClientMessageLinks(resultLinks);
      this.setInMemoryLinks(searchRef, resultLinks);
      this.logger.log('Crawl job completed', {
        searchRef,
        chatId,
        keyword,
        iterations: crawl.iterations,
        chatsProcessed: crawl.chatsProcessed,
        messagesStored: crawl.messagesStored,
        linksFound: resultLinks.length,
        displayLinks: displayLinks.length
      });

      try {
        await this.withTimeout(
          this.searchRepo.update(searchDbId, {
            results_links: JSON.stringify(resultLinks),
            results_invites: JSON.stringify(crawl.invites)
          }),
          dbOpTimeoutMs,
          'search_row_update'
        );
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        this.logger.warn('Search row update failed; using in-memory results', { searchDbId, error: message });
      }

      try {
        const persisted = await this.withTimeout(
          this.searchService.persistMatchesToOracle(searchRef),
          dbOpTimeoutMs,
          'persistMatchesToOracle'
        );
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
      return {
        searchRef,
        crawl,
        resultLinks,
        displayLinks
      };
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.error('Keyword crawl failed', message);
      try {
        await this.redisStore.failSearchRun(searchRef, message);
      } catch {
        // ignore redis-store failure in user path
      }
      throw err;
    }
  }

  private async executeCrawlJob(params: {
    chatId: number;
    userId: number;
    keyword: string;
    maxIterations: number;
  }) {
    const { chatId, userId, keyword, maxIterations } = params;
    let searchRef = `tmp_${Date.now()}_${chatId}`;
    try {
      const run = await this.runCrawlAndPersist({
        chatId,
        userId,
        keyword,
        maxIterations
      });
      searchRef = run.searchRef;
      await this.bot.telegram.sendMessage(
        chatId,
        `✅ Crawl completed for “${keyword}”.\nIterations: ${run.crawl.iterations}\nSeed publics: ${run.crawl.seedPublics.length}\nChats processed: ${run.crawl.chatsProcessed}\nMessages stored: ${run.crawl.messagesStored}\nMessage links (channel+uid): ${run.displayLinks.length}\nTotal links: ${run.resultLinks.length}`,
        this.mainMenuKeyboard
      );

      if (!run.resultLinks.length) {
        await this.bot.telegram.sendMessage(
          chatId,
          '😕 No Telegram links were found for this keyword.',
          this.mainMenuKeyboard
        );
        return;
      }

      await this.sendPage(run.searchRef, 'links', 0, chatId);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await this.bot.telegram.sendMessage(
        chatId,
        `⚠️ Crawl failed: ${message.length > 300 ? `${message.slice(0, 300)}...` : message}`,
        this.mainMenuKeyboard
      );
    } finally {
      this.runningChats.delete(chatId);
      this.logger.log('Crawl job finished', { chatId, keyword, searchRef });
    }
  }

  private async handleKeywordSearch(ctx: any, rawInput: string, fromCommand: boolean) {
    const chatId = ctx.chat?.id as number | undefined;
    if (!chatId) return;

    const { keyword, iterations } = this.parseKeywordAndIterations(rawInput, fromCommand);
    if (!keyword) {
      return ctx.reply('❗ Please send a keyword.', this.mainMenuKeyboard);
    }

    const defaultIterations = Math.max(1, Number(this.config.get<number>('crawlIterations') || 5));
    const maxIterations = Math.max(1, Math.min(50, iterations ?? defaultIterations));
    this.logger.log('Keyword request received', { chatId, keyword, maxIterations, fromCommand });

    if (this.runningChats.has(chatId)) {
      return ctx.reply('⏳ A crawl job is already running in this chat. Please wait.', this.mainMenuKeyboard);
    }

    const tgId = String(ctx.from?.id || '');
    const user = await this.userRepo.findOne({ where: { telegram_id: tgId } });
    if (!user) return ctx.reply('❗ Please /start first.', this.mainMenuKeyboard);

    this.runningChats.add(chatId);
    await ctx.reply(
      `🔎 Starting crawl for “${keyword}” (max iterations: ${maxIterations}).\nI will send paginated links when done.`,
      this.mainMenuKeyboard
    );

    void this.executeCrawlJob({
      chatId,
      userId: user.id,
      keyword,
      maxIterations
    });
  }

  private async handleKeywordFile(ctx: any) {
    const chatId = ctx.chat?.id as number | undefined;
    if (!chatId) return;

    if (this.runningChats.has(chatId)) {
      return ctx.reply('⏳ A crawl job is already running in this chat. Please wait.', this.mainMenuKeyboard);
    }

    const doc = (ctx.message as any)?.document;
    if (!doc?.file_id) {
      return ctx.reply('❗ Please upload a valid Excel file (.xlsx or .xls).', this.mainMenuKeyboard);
    }
    const fileName = String(doc.file_name || '').trim().toLowerCase();
    if (!fileName.endsWith('.xlsx') && !fileName.endsWith('.xls')) {
      return ctx.reply('❗ Unsupported file type. Please upload .xlsx or .xls.', this.mainMenuKeyboard);
    }

    const tgId = String(ctx.from?.id || '');
    const user = await this.userRepo.findOne({ where: { telegram_id: tgId } });
    if (!user) return ctx.reply('❗ Please /start first.', this.mainMenuKeyboard);

    const caption = String((ctx.message as any)?.caption || '');
    const iterMatch = caption.match(/--?(?:iter|iterations?)(?:=|\s+)(\d+)/i);
    const defaultIterations = Math.max(1, Number(this.config.get<number>('crawlIterations') || 5));
    const maxIterations = Math.max(
      1,
      Math.min(50, iterMatch ? Number.parseInt(iterMatch[1], 10) || defaultIterations : defaultIterations)
    );

    this.runningChats.add(chatId);
    try {
      const fileLink = await this.bot.telegram.getFileLink(doc.file_id);
      const response = await fetch(fileLink.toString());
      if (!response.ok) {
        throw new Error(`failed to download file (${response.status})`);
      }
      const content = Buffer.from(await response.arrayBuffer());
      const workbook = XLSX.read(content, { type: 'buffer' });
      const batches = this.extractKeywordBatchesFromWorkbook(workbook);
      if (!batches.length) {
        await ctx.reply('❗ No keywords were found in the uploaded file.', this.mainMenuKeyboard);
        return;
      }

      const maxKeywordsPerFile = 200;
      let remaining = maxKeywordsPerFile;
      const limitedBatches: KeywordSheetBatch[] = [];
      for (const batch of batches) {
        if (remaining <= 0) break;
        const keywords = batch.keywords.slice(0, remaining);
        if (!keywords.length) continue;
        limitedBatches.push({
          inputSheetName: batch.inputSheetName,
          keywords
        });
        remaining -= keywords.length;
      }

      const originalKeywordsCount = batches.reduce((sum, b) => sum + b.keywords.length, 0);
      const limitedKeywordsCount = limitedBatches.reduce((sum, b) => sum + b.keywords.length, 0);
      const excelConcurrency = Math.max(
        1,
        Number(this.config.get<number>('excelCrawlConcurrency') || 2)
      );
      await ctx.reply(
        `📄 Received ${batches.length} sheet(s), ${originalKeywordsCount} keyword(s). Starting batch crawl for ${limitedBatches.length} sheet(s), ${limitedKeywordsCount} keyword(s), max iterations ${maxIterations}, concurrency ${excelConcurrency}.`,
        this.mainMenuKeyboard
      );

      const sheetResults: Array<{
        inputSheetName: string;
        rows: ExportLinkRow[];
        errors: string[];
      }> = limitedBatches.map((batch) => ({
        inputSheetName: batch.inputSheetName,
        rows: [],
        errors: []
      }));

      const tasks: Array<{ sheetIndex: number; sheetName: string; keyword: string }> = [];
      for (let sheetIndex = 0; sheetIndex < limitedBatches.length; sheetIndex += 1) {
        const batch = limitedBatches[sheetIndex];
        for (const keyword of batch.keywords) {
          tasks.push({
            sheetIndex,
            sheetName: batch.inputSheetName,
            keyword
          });
        }
      }

      const keywordPromiseCache = new Map<string, Promise<{ rows: ExportLinkRow[]; error?: string }>>();
      const executeKeyword = (keyword: string): Promise<{ rows: ExportLinkRow[]; error?: string }> => {
        const cacheKey = keyword.trim();
        const existing = keywordPromiseCache.get(cacheKey);
        if (existing) return existing;

        const promise = (async () => {
          try {
            const run = await this.runCrawlAndPersist({
              chatId,
              userId: user.id,
              keyword,
              maxIterations
            });
            return { rows: this.buildExportRows(run.resultLinks, keyword) };
          } catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            return { rows: [], error: message };
          }
        })();
        keywordPromiseCache.set(cacheKey, promise);
        return promise;
      };

      let cursor = 0;
      let completed = 0;
      const totalTasks = tasks.length;
      const workerCount = Math.min(totalTasks, excelConcurrency);
      const shouldReportProgress = (done: number, total: number): boolean =>
        done === 1 || done === total || done % 10 === 0;

      const workers: Array<Promise<void>> = [];
      for (let w = 0; w < workerCount; w += 1) {
        workers.push((async () => {
          while (true) {
            const idx = cursor;
            cursor += 1;
            if (idx >= totalTasks) break;

            const task = tasks[idx];
            const result = await executeKeyword(task.keyword);
            if (result.error) {
              sheetResults[task.sheetIndex].errors.push(`${task.keyword}: ${result.error}`);
              this.logger.warn('Batch keyword crawl failed', {
                chatId,
                sheet: task.sheetName,
                keyword: task.keyword,
                error: result.error
              });
            } else {
              sheetResults[task.sheetIndex].rows.push(...result.rows);
            }

            completed += 1;
            if (shouldReportProgress(completed, totalTasks)) {
              await this.bot.telegram.sendMessage(
                chatId,
                `⏱ Progress: ${completed}/${totalTasks} keyword(s) processed.`
              );
            }
          }
        })());
      }

      await Promise.all(workers);

      const outputWorkbook = XLSX.utils.book_new();
      const usedSheetNames = new Set<string>();
      let exportedRowsCount = 0;
      for (let i = 0; i < sheetResults.length; i += 1) {
        const result = sheetResults[i];
        const rawName = this.sanitizeSheetName(result.inputSheetName, i);
        let sheetName = rawName;
        let suffix = 2;
        while (usedSheetNames.has(sheetName.toLowerCase())) {
          const maxBaseLen = Math.max(1, 31 - String(suffix).length - 1);
          sheetName = `${rawName.slice(0, maxBaseLen)}_${suffix}`;
          suffix += 1;
        }
        usedSheetNames.add(sheetName.toLowerCase());

        const dedupe = new Map<string, ExportLinkRow>();
        for (const row of result.rows) {
          // Merge by link identity at sheet-level, not per-keyword.
          const key = `${row.type}|||${row.channel}|||${row.uid}|||${row.link}`;
          if (!dedupe.has(key)) dedupe.set(key, row);
        }
        const mergedRows = Array.from(dedupe.values());
        exportedRowsCount += mergedRows.length;

        const rows: Array<Array<string>> = [['type', 'channel', 'uid', 'link']];
        if (!mergedRows.length && !result.errors.length) {
          rows.push(['info', '', '', 'No links found']);
        } else {
          for (const row of mergedRows) {
            rows.push([row.type, row.channel, row.uid, row.link]);
          }
          for (const err of result.errors) {
            rows.push(['error', '', '', err]);
          }
        }
        const worksheet = XLSX.utils.aoa_to_sheet(rows);
        XLSX.utils.book_append_sheet(outputWorkbook, worksheet, sheetName);
      }

      const outputBuffer = XLSX.write(outputWorkbook, { bookType: 'xlsx', type: 'buffer' }) as Buffer;
      const failedCount = sheetResults.reduce((sum, s) => sum + s.errors.length, 0);
      await this.bot.telegram.sendDocument(chatId, {
        source: outputBuffer,
        filename: `keyword-results-${Date.now()}.xlsx`
      } as any);
      await this.bot.telegram.sendMessage(
        chatId,
        `✅ Batch crawl completed.\nSheets: ${sheetResults.length}\nKeywords: ${limitedKeywordsCount}\nFailed keywords: ${failedCount}\nExported links: ${exportedRowsCount}`,
        this.mainMenuKeyboard
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await this.bot.telegram.sendMessage(
        chatId,
        `⚠️ Excel import failed: ${message.length > 300 ? `${message.slice(0, 300)}...` : message}`,
        this.mainMenuKeyboard
      );
    } finally {
      this.pendingInputMode.delete(chatId);
      this.runningChats.delete(chatId);
    }
  }

  private splitTextIntoChunks(text: string): string[] {
    const lines = text.split('\n');
    const chunks: string[] = [];
    let current = '';

    const pushCurrent = () => {
      if (current) chunks.push(current);
      current = '';
    };

    for (const line of lines) {
      const candidate = current ? `${current}\n${line}` : line;
      if (candidate.length <= this.maxMessageLen) {
        current = candidate;
        continue;
      }

      if (current) pushCurrent();

      if (line.length <= this.maxMessageLen) {
        current = line;
        continue;
      }

      for (let i = 0; i < line.length; i += this.maxMessageLen) {
        const part = line.slice(i, i + this.maxMessageLen);
        if (part.length === this.maxMessageLen && i + this.maxMessageLen < line.length) {
          chunks.push(part);
        } else {
          current = part;
        }
      }
    }

    if (current) pushCurrent();
    return chunks.length ? chunks : [text];
  }

  private async sendInChunks(
    chatId: number,
    text: string,
    opts: Record<string, unknown> = {},
    attachOptionsToLastChunk = false
  ) {
    const chunks = this.splitTextIntoChunks(text);
    for (let i = 0; i < chunks.length; i += 1) {
      const shouldAttachOpts = !attachOptionsToLastChunk || i === chunks.length - 1;
      await this.bot.telegram.sendMessage(chatId, chunks[i], shouldAttachOpts ? opts : undefined);
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
    const linkItems = source === 'links'
      ? this.filterClientMessageLinks(items as string[])
      : [];
    const botItems = source === 'links'
      ? this.filterClientBotLinks(items as string[])
      : [];
    const plainRootItems = source === 'links'
      ? this.filterClientPlainRootLinks(items as string[])
      : [];
    const { inviteLinks, rootLinks } = source === 'links'
      ? this.splitInviteAndRootLinks(plainRootItems)
      : { inviteLinks: [], rootLinks: [] };
    const groupedLinkItems = source === 'links'
      ? this.groupClientMessageLinks(linkItems)
      : [];
    const pageSize = source === 'web'
      ? Number(this.config.get<number>('webPageSize') || 10)
      : source === 'telegram'
        ? Number(this.config.get<number>('tgPageSize') || 5)
        : Number(this.config.get<number>('linksPageSize') || 50);
    const itemCount = source === 'links'
      ? Math.max(groupedLinkItems.length, (botItems.length || inviteLinks.length || rootLinks.length) ? 1 : 0)
      : items.length;
    if (!itemCount) {
      if (!messageId) {
        await this.bot.telegram.sendMessage(
          chatId,
          source === 'links'
            ? 'No message links with channel id and uid were found for this search.'
            : 'No cached results available for this page.'
        );
      }
      return;
    }
    const totalPages = Math.ceil(itemCount / pageSize);
    if (totalPages <= 0) return;
    const safePageIdx = Math.min(Math.max(pageIdx, 0), totalPages - 1);

    const sourceItems = source === 'links' ? groupedLinkItems : items;
    const slice = sourceItems.slice(safePageIdx * pageSize, (safePageIdx + 1) * pageSize);
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
      const chunks: string[] = [];
      if (safePageIdx === 0 && inviteLinks.length) {
        chunks.push(
          `Join these invite links first (manual if needed), then open message links:\n${inviteLinks.join('\n')}`
        );
      }
      if (safePageIdx === 0 && botItems.length) {
        chunks.push(`Bot Links:\n${botItems.join('\n')}`);
      }
      if (safePageIdx === 0 && rootLinks.length) {
        chunks.push(`Channel/Group Root Links:\n${rootLinks.join('\n')}`);
      }
      if ((slice as GroupedChannelLinks[]).length) {
        chunks.push(
          (slice as GroupedChannelLinks[])
            .map(
              (entry) =>
                `Telegram UID: ${entry.channelId}\n\nMessage Links:\n${entry.links.join('\n')}`
            )
            .join('\n---------------------------------\n')
        );
      }
      text += chunks.join('\n\n');
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
      await this.sendInChunks(chatId, text, opts, true);
    }
  }
}
