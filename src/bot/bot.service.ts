Total output lines: 1522

import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Markup, Telegraf } from 'telegraf';
import { SocksProxyAgent } from 'socks-proxy-agent';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { execFile } from 'child_process';
import { mkdtemp, readFile, rm, writeFile } from 'fs/promises';
import { basename, extname, join } from 'path';
import { tmpdir } from 'os';
import { promisify } from 'util';
import ExcelJS from 'exceljs';
import { LoggerService } from '../common/logger.service';
import { KeywordCrawlResultDto, SearchService } from '../search/search.service';
import { UserEntity } from '../entities/user.entity';
import { SearchEntity } from '../entities/search.entity';
import { RedisStoreService } from '../storage/redis-store.service';

const execFileAsync = promisify(execFile);

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
export class BotService implements OnModuleInit, OnModuleDestroy {
  private readonly bot: Telegraf;
  private readonly maxMessageLen = 4000;
  private readonly runningChats = new Set<number>();
  private readonly inMemoryResults = new Map<string, InMemorySearchResult>();
  private readonly inMemoryTtlMs = 12 * 60 * 60 * 1000;
  private readonly cachedSearchTtlMs = 7 * 24 * 60 * 60 * 1000;
  private readonly pendingInputMode = new Map<number, ChatInputMode>();
  private readonly menuSearchOneKeyword = 'جستجوی یک کلمه';
  private readonly menuImportExcel = 'ارسال فایل اکسل';
  private botLaunchLoop?: Promise<void>;
  private botLaunchAttempts = 0;
  private botShutdownRequested = false;
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
        { command: 'start', description: 'نمایش منوی اصلی' }
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

    this.ensureBotLaunchLoop();
  }

  async onModuleDestroy() {
    this.botShutdownRequested = true;
    try {
      this.bot.stop('shutdown');
      this.logger.log('Bot stopped');
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.warn('Bot stop failed', { error: message });
    }
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
        'سلام 👋 یکی از گزینه‌های منو را انتخاب کن.',
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
        return ctx.reply('لطفاً یک کلمه یا عبارت بفرست تا سریع جستجو کنم.', this.mainMenuKeyboard);
      }

      if (text === this.menuImportExcel) {
        this.pendingInputMode.set(chatId, 'excel');
        return ctx.reply('لطفاً فایل اکسل را بفرست؛ هر ردیف باید یک کلمه داشته باشد.', this.mainMenuKeyboard);
      }

      const mode = this.pendingInputMode.get(chatId);
      if (mode === 'excel') {
        return ctx.reply('حالت اکسل فعال است؛ لطفاً فایل با پسوند xlsx یا xls بفرست.', this.mainMenuKeyboard);
      }
      if (mode === 'keyword') {
        this.pendingInputMode.delete(chatId);
      } else {
        return ctx.reply(
          'لطفاً اول از منو مشخص کن می‌خواهی جستجوی تکی انجام بدی یا فایل اکسل بفرستی.',
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

  private ensureBotLaunchLoop() {
    if (this.botLaunchLoop) return;
    this.botLaunchLoop = this.runBotLaunchLoop()
      .catch((err) => {
        const message = err instanceof Error ? err.message : String(err);
        this.logger.error('Bot launch loop crashed', message);
      })
      .finally(() => {
        this.botLaunchLoop = undefined;
      });
  }

  private async runBotLaunchLoop() {
    while (!this.botShutdownRequested) {
      const attempt = this.botLaunchAttempts + 1;
      const dropPendingUpdates = this.botLaunchAttempts === 0;
      this.logger.log('Bot launch requested', { attempt, dropPendingUpdates });

      try {
        await this.bot.launch({ dropPendingUpdates });
        this.logger.log('Bot launched');
        this.botLaunchAttempts = 0;
        return;
      } catch (err) {
        if (this.botShutdownRequested) return;

        const message = err instanceof Error ? err.message : String(err);
        const retryMs = this.getBotLaunchRetryDelayMs(message, this.botLaunchAttempts);
        this.botLaunchAttempts += 1;

        this.logger.error('Bot launch failed', message);
        this.logger.warn('Bot relaunch scheduled', {
          attempt: this.botLaunchAttempts + 1,
          retryMs
        });

        await new Promise((resolve) => setTimeout(resolve, retryMs));
      }
    }
  }

  private getBotLaunchRetryDelayMs(message: string, attempts: number) {
    if (/terminated by other getUpdates request/i.test(message) || /409:\s*Conflict/i.test(message)) {
      return 15000;
    }
    return Math.min(60000, 5000 * (attempts + 1));
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

  private getCellText(cell: ExcelJS.Cell): string {
    const value = cell.value;
    if (value === null || value === undefined) return '';
    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
      return String(value).trim();
    }
    if (value instanceof Date) return value.toISOString();
    if (typeof value === 'object') {
      const textValue = (value as { text?: unknown }).text;
      if (typeof textValue === 'string') return textValue.trim();

      const resultValue = (value as { result?: unknown }).result;
      if (
        typeof resultValue === 'string' ||
        typeof resultValue === 'number' ||
        typeof resultValue === 'boolean'
      ) {
        return String(resultValue).trim();
      }

      const richText = (value as { richText?: Array<{ text?: string }> }).richText;
      if (Array.isArray(richText)) {
        return richText.map((part) => part.text || '').join('').trim();
      }
    }
    return cell.text.trim();
  }

  private parseKeywordRowsFromWorksheet(sheet: ExcelJS.Worksheet): string[] {
    const out: string[] = [];
    sheet.eachRow({ includeEmpty: false }, (row) => {
      let keyword = '';
      row.eachCell({ includeEmpty: false }, (cell) => {
        if (keyword) return;
        keyword = this.getCellText(cell);
      });
      if (keyword) out.push(keyword);
    });
    return out;
  }

  private extractKeywordBatchesFromWorkbook(workbook: ExcelJS.Workbook): KeywordSheetBatch[] {
    const out: KeywordSheetBatch[] = [];
    for (const sheet of workbook.worksheets || []) {
      const sheetKeywords = this.parseKeywordRowsFromWorksheet(sheet);
      if (!sheetKeywords.length) continue;
      const dedupe = new Set<string>();
      for (const keyword of sheetKeywords) dedupe.add(keyword);
      const keywords = Array.from(dedupe);
      if (!keywords.length) continue;
      out.push({
        inputSheetName: sheet.name,
        keywords
      });
    }
    return out;
  }

  private tempExcelFileName(fileName: string): string {
    const ext = extname(fileName).toLowerCase() === '.xls' ? '.xls' : '.xlsx';
    const base = basename(fileName, extname(fileName))
      .replace(/[^A-Za-z0-9._-]+/g, '_')
      .replace(/^_+|_+$/g, '')
      .slice(0, 64);
    return `${base || 'input'}${ext}`;
  }

  private async convertLegacyExcelToXlsx(content: Buffer, fileName: string): Promise<Buffer> {
    const tmpDir = await mkdtemp(join(tmpdir(), 'telegram-keyword-bot-excel-'));
    const inputName = this.tempExcelFileName(fileName);
    const inputPath = join(tmpDir, inputName);
    const outputPath = join(tmpDir, `${basename(inputName, extname(inputName))}.xlsx`);

    try {
      await writeFile(inputPath, content);
      await execFileAsync(
        'libreoffice',
        ['--headless', '--convert-to', 'xlsx', '--outdir', tmpDir, inputPath],
        { timeout: 30000, maxBuffer: 1024 * 1024 }
      );
      return await readFile(outputPath);
    } catch (err) {
      const errWithStderr = err as Error & { code?: string; stderr?: string };
      if (errWithStderr.code === 'ENOENT') {
        throw new Error('LibreOffice is required to read legacy .xls files but was not found on this server.');
      }
      const stderr = errWithStderr.stderr?.trim();
      throw new Error(`Failed to convert .xls file to .xlsx${stderr ? `: ${stderr}` : ''}`);
    } finally {
      await rm(tmpDir, { recursive: true, force: true }).catch(() => undefined);
    }
  }

  private async loadExcelWorkbook(content: Buffer, fileName: string): Promise<ExcelJS.Workbook> {
    const workbook = new ExcelJS.Workbook();
    const workbookContent = fileName.endsWith('.xls')
      ? await this.convertLegacyExcelToXlsx(content, fileName)
      : content;
    await workbook.xlsx.load(workbookContent as unknown as Parameters<ExcelJS.Xlsx['load']>[0]);
    return workbook;
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
    return `https://t.me/${usernameMatch[1]}`;
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
    for (const raw of rawLinks…4301 tokens truncated… (cachedSearch) {
      const cachedLinks = await this.getCachedSearchLinks(cachedSearch);
      this.logger.log('Serving keyword from cached search results', {
        chatId,
        keyword,
        cachedSearchId: cachedSearch.id,
        cachedLinks: cachedLinks.length
      });
      await ctx.reply(
        `نتیجه موجود آماده است ✅\nعبارت: «${keyword}»\n${this.summarizeClientLinks(cachedLinks)}\n\nاین نتیجه از کش ۷ روزه آمده؛ برای سرعت، تا یک هفته از دیتابیس جواب می‌دهم و بعد از آن سرچ تازه انجام می‌شود.`,
        this.mainMenuKeyboard
      );
      await this.sendPage(String(cachedSearch.id), 'links', 0, chatId);
      return;
    }

    if (this.runningChats.has(chatId)) {
      return ctx.reply('یک جستجو هنوز در حال اجراست؛ لطفاً چند لحظه صبر کن.', this.mainMenuKeyboard);
    }

    this.runningChats.add(chatId);
    await ctx.reply(
      `جستجو برای «${keyword}» شروع شد 🔎\nدر سریع‌ترین زمان ممکن کامل‌ترین لینک‌های مرتبط را می‌فرستم.`,
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
      return ctx.reply('یک جستجو هنوز در حال اجراست؛ لطفاً چند لحظه صبر کن.', this.mainMenuKeyboard);
    }

    const doc = (ctx.message as any)?.document;
    if (!doc?.file_id) {
      return ctx.reply('لطفاً یک فایل اکسل معتبر با پسوند xlsx یا xls بفرست.', this.mainMenuKeyboard);
    }
    const fileName = String(doc.file_name || '').trim().toLowerCase();
    if (!fileName.endsWith('.xlsx') && !fileName.endsWith('.xls')) {
      return ctx.reply('فرمت فایل پشتیبانی نمی‌شود؛ فقط فایل xlsx یا xls قابل قبول است.', this.mainMenuKeyboard);
    }

    const tgId = String(ctx.from?.id || '');
    const user = await this.userRepo.findOne({ where: { telegram_id: tgId } });
    if (!user) return ctx.reply('لطفاً اول دستور start را بزن تا منوی اصلی فعال شود.', this.mainMenuKeyboard);

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
      const workbook = await this.loadExcelWorkbook(content, fileName);
      const batches = this.extractKeywordBatchesFromWorkbook(workbook);
      if (!batches.length) {
        await ctx.reply('داخل فایل اکسل هیچ کلمه‌ای پیدا نکردم.', this.mainMenuKeyboard);
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
        `فایل اکسل دریافت شد 📄\nتعداد شیت‌ها: ${batches.length}\nتعداد کلمه‌ها: ${originalKeywordsCount}\nجستجو برای ${limitedKeywordsCount} کلمه شروع شد.`,
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
                `پیشرفت جستجو: ${completed} از ${totalTasks} کلمه پردازش شد.`
              );
            }
          }
        })());
      }

      await Promise.all(workers);

      const outputWorkbook = new ExcelJS.Workbook();
      outputWorkbook.creator = 'telegram-keyword-bot';
      outputWorkbook.created = new Date();
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

        const rows: Array<Array<string>> = [['نوع', 'کانال', 'شناسه', 'لینک']];
        if (!mergedRows.length && !result.errors.length) {
          rows.push(['اطلاعات', '', '', 'لینکی پیدا نشد']);
        } else {
          for (const row of mergedRows) {
            rows.push([row.type, row.channel, row.uid, row.link]);
          }
          for (const err of result.errors) {
            rows.push(['خطا', '', '', err]);
          }
        }
        const worksheet = outputWorkbook.addWorksheet(sheetName);
        worksheet.columns = [
          { key: 'type', width: 14 },
          { key: 'channel', width: 28 },
          { key: 'uid', width: 16 },
          { key: 'link', width: 72 }
        ];
        for (let rowIndex = 0; rowIndex < rows.length; rowIndex += 1) {
          const worksheetRow = worksheet.addRow(rows[rowIndex]);
          if (rowIndex === 0) {
            worksheetRow.font = { bold: true };
          } else {
            const link = rows[rowIndex][3];
            if (/^https?:\/\//i.test(link)) {
              worksheetRow.getCell(4).value = { text: link, hyperlink: link };
            }
          }
        }
      }

      const outputData = await outputWorkbook.xlsx.writeBuffer();
      const outputBuffer = Buffer.from(outputData);
      const failedCount = sheetResults.reduce((sum, s) => sum + s.errors.length, 0);
      await this.bot.telegram.sendDocument(chatId, {
        source: outputBuffer,
        filename: `keyword-results-${Date.now()}.xlsx`
      } as any);
      await this.bot.telegram.sendMessage(
        chatId,
        `پردازش فایل اکسل تمام شد ✅\nتعداد شیت‌ها: ${sheetResults.length}\nتعداد کلمه‌ها: ${limitedKeywordsCount}\nکلمه‌های ناموفق: ${failedCount}\nلینک‌های خروجی: ${exportedRowsCount}`,
        this.mainMenuKeyboard
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await this.bot.telegram.sendMessage(
        chatId,
        `پردازش فایل اکسل ناموفق بود. لطفاً فایل را بررسی کن و دوباره بفرست.\nجزئیات فنی: ${message.length > 250 ? `${message.slice(0, 250)}...` : message}`,
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
            ? 'برای این جستجو لینک پیام قابل نمایش پیدا نشد.'
            : 'نتیجه‌ای برای این صفحه در حافظه نیست.'
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
      ? `نتایج وب، صفحه ${safePageIdx + 1} از ${totalPages}:\n\n`
      : source === 'telegram'
        ? `نتایج تلگرام، صفحه ${safePageIdx + 1} از ${totalPages}:\n\n`
        : `لینک‌های پیدا شده، صفحه ${safePageIdx + 1} از ${totalPages}:\n\n`;

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
      const publicRootLinks = rootLinks.filter((link) => this.isPublicTelegramLink(link));
      const privateRootLinks = rootLinks.filter((link) => this.isPrivateTelegramLink(link));
      const publicGroups = (slice as GroupedChannelLinks[]).filter((entry) =>
        entry.links.some((link) => !this.isPrivateTelegramLink(link))
      );
      const privateGroups = (slice as GroupedChannelLinks[]).filter((entry) =>
        entry.links.every((link) => this.isPrivateTelegramLink(link))
      );

      if (safePageIdx === 0 && inviteLinks.length) {
        chunks.push(this.formatLinkList('گروه‌ها یا کانال‌های خصوصی - لینک دعوت', inviteLinks));
      }
      if (safePageIdx === 0 && botItems.length) {
        chunks.push(this.formatLinkList('بات‌ها', botItems));
      }
      if (safePageIdx === 0 && publicRootLinks.length) {
        chunks.push(this.formatLinkList('کانال‌ها یا گروه‌های عمومی - لینک اصلی', publicRootLinks));
      }
      if (safePageIdx === 0 && privateRootLinks.length) {
        chunks.push(this.formatLinkList('کانال‌ها یا گروه‌های خصوصی - لینک اصلی', privateRootLinks));
      }
      if (publicGroups.length) {
        chunks.push(this.formatGroupedMessages('پیام‌های کانال‌ها یا گروه‌های عمومی', publicGroups));
      }
      if (privateGroups.length) {
        chunks.push(this.formatGroupedMessages('پیام‌های کانال‌ها یا گروه‌های خصوصی', privateGroups));
      }
      text += chunks.join('\n\n');
    }

    const prevBtn = safePageIdx > 0
      ? { text: 'قبلی ◀️', callback_data: `pg:${searchId}:${source}:${safePageIdx - 1}` }
      : { text: ' ', callback_data: 'nop' };
    const nextBtn = safePageIdx < totalPages - 1
      ? { text: 'بعدی ▶️', callback_data: `pg:${searchId}:${source}:${safePageIdx + 1}` }
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
