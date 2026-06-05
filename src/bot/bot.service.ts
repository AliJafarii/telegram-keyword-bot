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

interface SearchMessageGroup {
  uid: string;
  title: string;
  inviteLink: string;
  messageLinks: string[];
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

interface ExportGroupRow {
  keyword?: string;
  type: string;
  uid: string;
  title: string;
  inviteLink: string;
  messageLinks: string[];
}

interface KeywordSheetBatch {
  inputSheetName: string;
  keywords: string[];
}

interface LinkImportResult {
  link?: string;
  type?: string;
  status?: string;
  chatId?: number;
  chatKey?: string;
  title?: string;
  username?: string;
  inviteLink?: string;
  extractedLinks?: string[];
  error?: string;
}

interface LinkImportPayload {
  generatedAt?: string;
  dryRun?: boolean;
  inputLinks?: number;
  results?: LinkImportResult[];
}

type ChatInputMode = 'keyword' | 'excel' | 'links';

@Injectable()
export class BotService implements OnModuleInit, OnModuleDestroy {
  private readonly bot: Telegraf;
  private readonly maxMessageLen = 4000;
  private readonly runningChats = new Set<number>();
  private readonly inMemoryResults = new Map<string, InMemorySearchResult>();
  private readonly inMemoryTtlMs = 12 * 60 * 60 * 1000;
  private readonly pendingInputMode = new Map<number, ChatInputMode>();
  private readonly linkImportTextBuffers = new Map<number, string[]>();
  private readonly menuSearchOneKeyword = 'جستجوی یک کلمه';
  private readonly menuImportExcel = 'ارسال فایل اکسل';
  private readonly menuImportLinks = 'اضافه کردن لینک‌ها';
  private botLaunchLoop?: Promise<void>;
  private botLaunchAttempts = 0;
  private botShutdownRequested = false;
  private readonly mainMenuKeyboard = Markup.keyboard(
    [[this.menuSearchOneKeyword], [this.menuImportExcel], [this.menuImportLinks]]
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
        this.linkImportTextBuffers.delete(ctx.chat.id);
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
        this.linkImportTextBuffers.delete(chatId);
        return ctx.reply('لطفاً یک کلمه یا عبارت بفرست تا سریع جستجو کنم.', this.mainMenuKeyboard);
      }

      if (text === this.menuImportExcel) {
        this.pendingInputMode.set(chatId, 'excel');
        this.linkImportTextBuffers.delete(chatId);
        return ctx.reply('لطفاً فایل اکسل را بفرست؛ هر ردیف باید یک کلمه داشته باشد.', this.mainMenuKeyboard);
      }

      if (text === this.menuImportLinks) {
        this.pendingInputMode.set(chatId, 'links');
        this.linkImportTextBuffers.set(chatId, []);
        return ctx.reply(
          'لطفاً فایل اکسل لینک‌ها را بفرست، یا لینک‌ها را در چند پیام پشت‌سرهم بفرست و آخرش بنویس «تمام» یا «پردازش».',
          this.mainMenuKeyboard
        );
      }

      const mode = this.pendingInputMode.get(chatId);
      if (mode === 'excel') {
        return ctx.reply('حالت اکسل فعال است؛ لطفاً فایل با پسوند xlsx یا xls بفرست.', this.mainMenuKeyboard);
      }
      if (mode === 'links') {
        return this.handleLinkImportText(ctx as any, text);
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
      const chatId = ctx.chat?.id as number | undefined;
      if (chatId && this.pendingInputMode.get(chatId) === 'links') {
        return this.handleLinkImportFile(ctx as any);
      }
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
      row.eachCell({ includeEmpty: false }, (cell) => {
        const keyword = this.getCellText(cell);
        if (this.isKeywordCellValue(keyword)) out.push(keyword);
      });
    });
    return out;
  }

  private isKeywordCellValue(value: string): boolean {
    const normalized = value.trim().replace(/\s+/g, ' ');
    if (!normalized) return false;

    const header = normalized.toLowerCase();
    const headerValues = new Set([
      'keyword',
      'keywords',
      'final keyword',
      'final keywords',
      'کلمه',
      'کلمات',
      'کلیدواژه',
      'کلید واژه',
      'عبارت',
      'عبارات'
    ]);
    return !headerValues.has(header);
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

  private isPrivateTelegramLink(link: string): boolean {
    return /^https:\/\/t\.me\/c\/\d+(?:\/\d+)?$/i.test(link)
      || /^https:\/\/t\.me\/(?:\+|joinchat\/)/i.test(link);
  }

  private isPublicTelegramLink(link: string): boolean {
    return /^https:\/\/t\.me\/[A-Za-z0-9_]+(?:\/\d+)?$/i.test(link);
  }

  private formatLinkList(title: string, links: string[]): string {
    if (!links.length) return '';
    return [title, ...links.map((link) => 'ـ ' + link)].join('\n');
  }

  private formatPlainLinks(links: string[]): string {
    return links.join('\n');
  }

  private formatGroupedMessages(title: string, groups: GroupedChannelLinks[]): string {
    if (!groups.length) return '';
    const body = groups
      .map((entry) => [
        'شناسه: ' + entry.channelId,
        ...entry.links.map((link) => 'ـ ' + link)
      ].join('\n'))
      .join('\n\n');
    return title + '\n' + body;
  }

  private formatSearchMessageGroups(title: string, groups: SearchMessageGroup[]): string {
    if (!groups.length) return '';
    const body = groups
      .map((entry) => [
        '---------------------------------',
        'Telegram UID: ' + (entry.uid || ''),
        'Title: ' + (entry.title || ''),
        'Invite Link: ' + (entry.inviteLink || ''),
        ...entry.messageLinks
      ].join('\n'))
      .join('\n');
    return body + '\n---------------------------------';
  }

  private summarizeClientLinks(links: string[]): string {
    const messageLinks = this.filterClientMessageLinks(links);
    const groupedMessages = this.groupClientMessageLinks(messageLinks);
    const botLinks = this.filterClientBotLinks(links);
    const plainRootLinks = this.filterClientPlainRootLinks(links);
    const { inviteLinks, rootLinks } = this.splitInviteAndRootLinks(plainRootLinks);
    const publicRootLinks = rootLinks.filter((link) => this.isPublicTelegramLink(link));
    const privateRootLinks = rootLinks.filter((link) => this.isPrivateTelegramLink(link));
    const publicMessageGroups = groupedMessages.filter((entry) =>
      entry.links.some((link) => !this.isPrivateTelegramLink(link))
    );
    const privateMessageGroups = groupedMessages.filter((entry) =>
      entry.links.every((link) => this.isPrivateTelegramLink(link))
    );

    return [
      'خلاصه نتیجه موجود:',
      'ـ بات‌ها: ' + botLinks.length,
      'ـ لینک دعوت خصوصی: ' + inviteLinks.length,
      'ـ کانال/گروه عمومی: ' + (publicRootLinks.length + publicMessageGroups.length),
      'ـ کانال/گروه خصوصی: ' + (privateRootLinks.length + privateMessageGroups.length),
      'ـ پیام‌های قابل نمایش: ' + messageLinks.length,
      'ـ کل لینک‌ها: ' + links.length
    ].join('\n');
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

  private buildGroupedExportRows(rows: ExportLinkRow[]): ExportGroupRow[] {
    const groups = new Map<string, ExportGroupRow>();
    const ensureGroup = (key: string, initial: ExportGroupRow): ExportGroupRow => {
      const existing = groups.get(key);
      if (existing) {
        if (!existing.keyword && initial.keyword) existing.keyword = initial.keyword;
        if (!existing.title && initial.title) existing.title = initial.title;
        if (!existing.inviteLink && initial.inviteLink) existing.inviteLink = initial.inviteLink;
        return existing;
      }
      groups.set(key, initial);
      return initial;
    };

    for (const row of rows) {
      if (row.type === 'message') {
        const group = ensureGroup(`${row.keyword || ''}|channel|${row.channel}`, {
          keyword: row.keyword,
          type: 'channel_messages',
          uid: row.channel,
          title: '',
          inviteLink: '',
          messageLinks: []
        });
        if (!group.messageLinks.includes(row.link)) group.messageLinks.push(row.link);
        continue;
      }

      if (row.type === 'chat_root' || row.type === 'private_root' || row.type === 'invite') {
        const groupKey = row.type === 'invite'
          ? `${row.keyword || ''}|invite|${row.link}`
          : `${row.keyword || ''}|channel|${row.channel || row.link}`;
        const group = ensureGroup(groupKey, {
          keyword: row.keyword,
          type: row.type === 'invite' ? 'invite' : 'channel_root',
          uid: row.channel,
          title: row.channel,
          inviteLink: row.link,
          messageLinks: []
        });
        if (!group.inviteLink) group.inviteLink = row.link;
        continue;
      }

      const botKey = `${row.keyword || ''}|bot|${row.link}`;
      ensureGroup(botKey, {
        keyword: row.keyword,
        type: 'bot',
        uid: '',
        title: row.channel,
        inviteLink: row.link,
        messageLinks: []
      });
    }

    return Array.from(groups.values()).sort((a, b) => {
      const k = (a.keyword || '').localeCompare(b.keyword || '', undefined, { sensitivity: 'base' });
      if (k !== 0) return k;
      const t = a.type.localeCompare(b.type, undefined, { sensitivity: 'base' });
      if (t !== 0) return t;
      const u = a.uid.localeCompare(b.uid, undefined, { sensitivity: 'base' });
      if (u !== 0) return u;
      return (a.inviteLink || a.messageLinks[0] || '').localeCompare(
        b.inviteLink || b.messageLinks[0] || '',
        undefined,
        { sensitivity: 'base' }
      );
    });
  }

  private async withTimeout<T>(
    promise: Promise<T>,
    timeoutMs: number,
    label: string,
    onTimeout?: () => void
  ): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      let settled = false;
      const finish = (handler: () => void) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        handler();
      };
      const timer = setTimeout(() => {
        if (onTimeout) {
          try {
            onTimeout();
          } catch {
            // ignore timeout hook failures
          }
        }
        finish(() => reject(new Error(`${label} timeout after ${timeoutMs}ms`)));
      }, timeoutMs);

      promise.then(
        (value) => finish(() => resolve(value)),
        (err) => finish(() => reject(err))
      );
    });
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
    crawlRuntimeMs?: number;
  }): Promise<CrawlRunResult> {
    const { chatId, userId, keyword, maxIterations } = params;
    const dbOpTimeoutMs = Math.max(10000, Number(this.config.get<number>('dbOpTimeoutMs') || 120000));
    const configuredCrawlRuntimeMs = Math.max(
      60000,
      Number(params.crawlRuntimeMs || this.config.get<number>('crawlMaxRuntimeMs') || 300000)
    );
    // Allow a grace window after crawl runtime for in-flight Telegram calls to settle.
    const crawlTimeoutMs = Math.max(90000, configuredCrawlRuntimeMs + 60000);

    let searchRef = `tmp_${Date.now()}_${chatId}`;
    this.logger.log('Crawl job started', { chatId, userId, keyword, maxIterations, searchRef });
    try {
      const search = await this.createSearchRowOrThrow({ userId, keyword, chatId });
      const searchDbId = search.id;
      searchRef = String(searchDbId);
      this.logger.log('Search row created', { searchDbId, keyword, chatId });
      const crawlAbortController = new AbortController();

      const crawl = await this.withTimeout(
        this.searchService.crawlKeywordIterative(
          keyword,
          searchDbId,
          maxIterations,
          searchRef,
          { abortSignal: crawlAbortController.signal }
        ),
        crawlTimeoutMs,
        'crawlKeywordIterative',
        () => {
          crawlAbortController.abort(new Error(`crawlKeywordIterative timeout after ${crawlTimeoutMs}ms`));
          this.logger.warn('Crawl timeout reached; abort requested', {
            chatId,
            keyword,
            searchRef,
            timeoutMs: crawlTimeoutMs
          });
        }
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
        const partialLinks = await this.redisStore.getClientLinks(searchRef);
        if (partialLinks.length) {
          const partialMatches = await this.redisStore.getAllMatches(searchRef);
          const chatsProcessed = new Set(partialMatches.map((row) => row.channelKey).filter(Boolean)).size;
          const iterations = Math.max(
            0,
            ...partialMatches.map((row) => Number(row.iterationNo || 0)).filter((n) => Number.isFinite(n))
          );
          const displayLinks = this.filterClientMessageLinks(partialLinks);
          this.setInMemoryLinks(searchRef, partialLinks);
          const numericSearchId = this.toNumericSearchId(searchRef);

          if (numericSearchId) {
            try {
              await this.withTimeout(
                this.searchRepo.update(numericSearchId, {
                  results_links: JSON.stringify(partialLinks)
                }),
                dbOpTimeoutMs,
                'partial_search_row_update'
              );
            } catch (updateErr) {
              const updateMessage = updateErr instanceof Error ? updateErr.message : String(updateErr);
              this.logger.warn('Partial search row update failed; using Redis results', {
                searchRef,
                error: updateMessage
              });
            }
          }

          try {
            await this.withTimeout(
              this.searchService.persistMatchesToOracle(searchRef),
              dbOpTimeoutMs,
              'partial_persistMatchesToOracle'
            );
          } catch (persistErr) {
            const persistMessage = persistErr instanceof Error ? persistErr.message : String(persistErr);
            this.logger.warn('Partial channel match persistence failed', {
              searchRef,
              error: persistMessage
            });
          }

          try {
            await this.redisStore.completeSearchRun(searchRef, {
              keyword,
              partial: true,
              error: message,
              iterations,
              chatsProcessed,
              messagesStored: partialMatches.length,
              clientLinks: partialLinks.length
            });
          } catch {
            // ignore redis-store failure in user path
          }

          this.logger.warn('Returning partial crawl results after failure', {
            searchRef,
            chatId,
            keyword,
            error: message,
            matches: partialMatches.length,
            linksFound: partialLinks.length,
            displayLinks: displayLinks.length
          });

          return {
            searchRef,
            crawl: {
              seedPublics: [],
              links: partialLinks,
              clientLinks: partialLinks,
              invites: [],
              chatsProcessed,
              messagesStored: partialMatches.length,
              iterations
            },
            resultLinks: partialLinks,
            displayLinks
          };
        }
      } catch (partialErr) {
        const partialMessage = partialErr instanceof Error ? partialErr.message : String(partialErr);
        this.logger.warn('Partial crawl recovery failed', { searchRef, error: partialMessage });
      }
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
        `نتیجه آماده شد ✅\nعبارت: «${keyword}»\nکانال‌های بررسی‌شده: ${run.crawl.chatsProcessed}\nپیام‌های مرتبط: ${run.crawl.messagesStored}\nلینک‌های قابل نمایش: ${run.displayLinks.length}`,
        this.mainMenuKeyboard
      );

      if (!run.resultLinks.length) {
        await this.bot.telegram.sendMessage(
          chatId,
          'متأسفانه برای این عبارت لینک تلگرامی قابل نمایش پیدا نشد.',
          this.mainMenuKeyboard
        );
        return;
      }

      await this.sendPage(run.searchRef, 'links', 0, chatId);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await this.bot.telegram.sendMessage(
        chatId,
        `متأسفانه جستجو کامل نشد. لطفاً کمی بعد دوباره امتحان کن.\nجزئیات فنی: ${message.length > 250 ? `${message.slice(0, 250)}...` : message}`,
        this.mainMenuKeyboard
      );
    } finally {
      this.runningChats.delete(chatId);
      this.logger.log('Crawl job finished', { chatId, keyword, searchRef });
    }
  }

  private async findLatestCachedSearch(keyword: string): Promise<SearchEntity | null> {
    try {
      const cacheTtlMs = Number(this.config.get<number>('searchCacheTtlMs') ?? 7 * 24 * 60 * 60 * 1000);
      if (cacheTtlMs <= 0) return null;
      const cacheCutoff = new Date(Date.now() - cacheTtlMs);
      const candidates = await this.searchRepo
        .createQueryBuilder('search')
        .where('search.keyword = :keyword', { keyword })
        .andWhere('search.created_at >= :cacheCutoff', { cacheCutoff })
        .orderBy('search.id', 'DESC')
        .limit(5)
        .getMany();

      for (const candidate of candidates) {
        const dbLinks = this.parseJsonStringArray(candidate.results_links);
        if (dbLinks.length) return candidate;
        try {
          const redisLinks = await this.redisStore.getClientLinks(String(candidate.id));
          if (redisLinks.length) return candidate;
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          this.logger.warn('Cached search redis lookup failed', {
            keyword,
            searchId: candidate.id,
            error: message
          });
        }
        try {
          const tableLinks = await this.searchService.getSearchLinksBySearchId(candidate.id);
          if (tableLinks.length) return candidate;
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          this.logger.warn('Cached search link lookup failed', {
            keyword,
            searchId: candidate.id,
            error: message
          });
        }
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.warn('Cached search lookup failed', { keyword, error: message });
    }
    return null;
  }

  private async getCachedSearchLinks(search: SearchEntity): Promise<string[]> {
    const dbLinks = this.parseJsonStringArray(search.results_links);
    if (dbLinks.length) return dbLinks;

    const cachedLinks = this.getInMemoryLinks(String(search.id));
    if (cachedLinks.length) return cachedLinks;

    try {
      const redisLinks = await this.redisStore.getClientLinks(String(search.id));
      if (redisLinks.length) return redisLinks;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.warn('Cached search redis link lookup failed', { searchId: search.id, error: message });
    }

    try {
      return await this.searchService.getSearchLinksBySearchId(search.id);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.warn('Cached search table link lookup failed', { searchId: search.id, error: message });
      return [];
    }
  }

  private async handleKeywordSearch(ctx: any, rawInput: string, fromCommand: boolean) {
    const chatId = ctx.chat?.id as number | undefined;
    if (!chatId) return;

    const { keyword, iterations } = this.parseKeywordAndIterations(rawInput, fromCommand);
    if (!keyword) {
      return ctx.reply('لطفاً یک کلمه یا عبارت برای جستجو بفرست.', this.mainMenuKeyboard);
    }

    const defaultIterations = Math.max(1, Number(this.config.get<number>('crawlIterations') || 5));
    const maxIterations = Math.max(1, Math.min(50, iterations ?? defaultIterations));
    this.logger.log('Keyword request received', { chatId, keyword, maxIterations, fromCommand });

    const tgId = String(ctx.from?.id || '');
    const user = await this.userRepo.findOne({ where: { telegram_id: tgId } });
    if (!user) return ctx.reply('لطفاً اول دستور start را بزن تا منوی اصلی فعال شود.', this.mainMenuKeyboard);

    const cachedSearch = await this.findLatestCachedSearch(keyword);
    if (cachedSearch) {
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
      const excelCrawlRuntimeMs = Math.max(
        60000,
        Number(this.config.get<number>('excelCrawlMaxRuntimeMs') || 180000)
      );
      const excelIterations = Math.max(
        1,
        Math.min(maxIterations, Number(this.config.get<number>('excelCrawlIterations') || 1))
      );
      await ctx.reply(
        `فایل اکسل دریافت شد 📄\nتعداد شیت‌ها: ${batches.length}\nتعداد کلمه‌ها: ${originalKeywordsCount}\nجستجو برای ${limitedKeywordsCount} کلمه شروع شد.`,
        this.mainMenuKeyboard
      );

      const sheetResults: Array<{
        inputSheetName: string;
        rows: Map<string, ExportLinkRow>;
        errors: string[];
      }> = limitedBatches.map((batch) => ({
        inputSheetName: batch.inputSheetName,
        rows: new Map<string, ExportLinkRow>(),
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
              maxIterations: excelIterations,
              crawlRuntimeMs: excelCrawlRuntimeMs
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
        done >= 1 || done === total;

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
              for (const row of result.rows) {
                const key = `${row.type}|||${row.channel}|||${row.uid}|||${row.link}`;
                if (!sheetResults[task.sheetIndex].rows.has(key)) {
                  sheetResults[task.sheetIndex].rows.set(key, row);
                }
              }
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
      const hyperlinkRowLimit = Math.max(
        0,
        Number(this.config.get<number>('excelHyperlinkRowLimit') || 5000)
      );
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

        const mergedRows = Array.from(result.rows.values());
        const groupedRows = this.buildGroupedExportRows(mergedRows);
        exportedRowsCount += mergedRows.length;
        const worksheet = outputWorkbook.addWorksheet(sheetName);
        worksheet.columns = [
          { key: 'keyword', width: 28 },
          { key: 'type', width: 18 },
          { key: 'uid', width: 18 },
          { key: 'title', width: 32 },
          { key: 'inviteLink', width: 72 },
          { key: 'messageCount', width: 14 },
          { key: 'messageLinks', width: 90 }
        ];
        const headerRow = worksheet.addRow([
          'کلمه',
          'نوع',
          'Telegram UID / Channel',
          'Title',
          'Invite / Root / Bot Link',
          'تعداد پیام',
          'Message Links'
        ]);
        headerRow.font = { bold: true };
        const useHyperlinks = hyperlinkRowLimit > 0 && groupedRows.length <= hyperlinkRowLimit;
        if (!mergedRows.length && !result.errors.length) {
          worksheet.addRow(['', 'اطلاعات', '', '', 'لینکی پیدا نشد', 0, '']);
        } else {
          for (const row of groupedRows) {
            const worksheetRow = worksheet.addRow([
              row.keyword || '',
              row.type,
              row.uid,
              row.title,
              row.inviteLink,
              row.messageLinks.length,
              row.messageLinks.join('\n')
            ]);
            worksheetRow.alignment = { vertical: 'top', wrapText: true };
            if (useHyperlinks && /^https?:\/\//i.test(row.inviteLink)) {
              worksheetRow.getCell(5).value = { text: row.inviteLink, hyperlink: row.inviteLink };
            }
          }
          for (const err of result.errors) {
            worksheet.addRow(['', 'خطا', '', '', err, 0, '']);
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
      this.searchService.clearRuntimeCaches();
    }
  }

  private parseImportLinksPayload(stdout: string): LinkImportPayload {
    const start = stdout.indexOf('{');
    const end = stdout.lastIndexOf('}');
    if (start < 0 || end <= start) return {};
    try {
      return JSON.parse(stdout.slice(start, end + 1));
    } catch {
      return {};
    }
  }

  private linkImportReason(item: LinkImportResult): string {
    if (item.error) return item.error;
    if (item.status === 'stored' && item.type === 'private_message') {
      return 'لینک پیام خصوصی t.me/c است؛ بدون invite یا عضویت قبلی فقط ذخیره می‌شود و join مستقیم ندارد.';
    }
    if (item.status === 'stored' && item.type === 'public_message') {
      return 'لینک پیام عمومی ذخیره شده؛ اگر join فعال باشد، عضویت همان گروه یا چنل هم امتحان می‌شود.';
    }
    if (item.status === 'started' && item.type === 'bot' && !item.extractedLinks?.length) {
      return 'بات start شده ولی در تاریخچه اخیر لینک تلگرام قابل استخراج پیدا نشده است.';
    }
    if (item.status === 'joined') return 'عضویت انجام شد.';
    if (item.status === 'join_requested') return 'درخواست عضویت ارسال شد و نیاز به تایید ادمین دارد.';
    if (item.status === 'already_joined') return 'اکانت از قبل عضو بوده است.';
    if (item.status === 'flood_wait') return 'تلگرام برای این عملیات محدودیت زمانی FloodWait داده؛ بعد از پایان زمان انتظار باید دوباره تست شود.';
    return '';
  }

  private compactImportCounts(results: LinkImportResult[], key: 'status' | 'type'): Array<{ name: string; count: number }> {
    const counts = new Map<string, number>();
    for (const item of results) {
      const value = String(item[key] || 'unknown');
      counts.set(value, (counts.get(value) || 0) + 1);
    }
    return Array.from(counts.entries())
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([name, count]) => ({ name, count }));
  }

  private async buildLinkImportWorkbook(payload: LinkImportPayload): Promise<Buffer> {
    const results = Array.isArray(payload.results) ? payload.results : [];
    const workbook = new ExcelJS.Workbook();
    workbook.creator = 'telegram-search-bot';
    workbook.created = new Date();

    const addRowsSheet = (name: string, columns: Partial<ExcelJS.Column>[], rows: Record<string, any>[]) => {
      const sheet = workbook.addWorksheet(name);
      sheet.columns = columns;
      sheet.addRows(rows);
      sheet.views = [{ state: 'frozen', ySplit: 1 }];
      sheet.getRow(1).font = { bold: true };
      sheet.autoFilter = {
        from: { row: 1, column: 1 },
        to: { row: Math.max(1, rows.length + 1), column: columns.length }
      };
      for (const column of sheet.columns) {
        let width = Number(column.width || 12);
        column.eachCell?.({ includeEmpty: false }, (cell) => {
          width = Math.min(Math.max(width, String(cell.value || '').length + 2), 80);
        });
        column.width = width;
      }
      return sheet;
    };

    const statusCounts = this.compactImportCounts(results, 'status');
    const typeCounts = this.compactImportCounts(results, 'type');
    const errorCounts = new Map<string, number>();
    for (const item of results) {
      if (!item.error) continue;
      errorCounts.set(item.error, (errorCounts.get(item.error) || 0) + 1);
    }

    const botRows = results
      .filter((item) => item.type === 'bot')
      .map((item) => ({
        section: 'bot_links',
        title: item.title || item.username || item.chatKey || '',
        chatKey: item.chatKey || item.username || '',
        type: item.type || '',
        inviteLink: item.link || item.inviteLink || '',
        messageLinksCount: '',
        messageLinks: ''
      }));

    const grouped = new Map<string, {
      title: string;
      chatKey: string;
      type: string;
      inviteLink: string;
      messageLinks: string[];
    }>();
    const ensureGroup = (item: LinkImportResult) => {
      const key = item.chatKey || item.inviteLink || item.username || item.link || 'unknown';
      const current = grouped.get(key) || {
        title: item.title || item.username || item.chatKey || '',
        chatKey: item.chatKey || item.username || '',
        type: item.type || '',
        inviteLink: item.inviteLink || '',
        messageLinks: []
      };
      if (!current.title && (item.title || item.username)) current.title = item.title || item.username || '';
      if (!current.chatKey && (item.chatKey || item.username)) current.chatKey = item.chatKey || item.username || '';
      if (!current.inviteLink && item.inviteLink) current.inviteLink = item.inviteLink;
      if (!current.type && item.type) current.type = item.type;
      grouped.set(key, current);
      return current;
    };
    for (const item of results) {
      if (item.type === 'bot') continue;
      const isMessage = item.type === 'public_message' || item.type === 'private_message';
      const isChat = item.type === 'public_chat' || item.type === 'private_chat' || item.type === 'invite';
      if (!isMessage && !isChat) continue;
      const group = ensureGroup(item);
      if (isMessage && item.link && !group.messageLinks.includes(item.link)) {
        group.messageLinks.push(item.link);
      }
    }
    const groupedRows = [
      ...botRows,
      ...Array.from(grouped.values())
        .sort((a, b) => (a.title || a.chatKey || a.inviteLink).localeCompare(b.title || b.chatKey || b.inviteLink))
        .map((item) => ({
          section: 'chat_or_group',
          title: item.title,
          chatKey: item.chatKey,
          type: item.type,
          inviteLink: item.inviteLink,
          messageLinksCount: item.messageLinks.length,
          messageLinks: item.messageLinks.join('\n')
        }))
    ];

    const groupedSheet = addRowsSheet('grouped_output', [
      { header: 'section', key: 'section', width: 16 },
      { header: 'title', key: 'title', width: 32 },
      { header: 'chatKey', key: 'chatKey', width: 28 },
      { header: 'type', key: 'type', width: 18 },
      { header: 'inviteLink', key: 'inviteLink', width: 60 },
      { header: 'messageLinksCount', key: 'messageLinksCount', width: 20 },
      { header: 'messageLinks', key: 'messageLinks', width: 90 }
    ], groupedRows);
    groupedSheet.getColumn('messageLinks').alignment = { wrapText: true, vertical: 'top' };

    addRowsSheet('summary', [
      { header: 'metric', key: 'metric', width: 28 },
      { header: 'value', key: 'value', width: 18 },
      { header: 'note', key: 'note', width: 70 }
    ], [
      { metric: 'generatedAt', value: payload.generatedAt || '', note: '' },
      { metric: 'inputLinks', value: payload.inputLinks ?? results.length, note: 'لینک‌های یکتای خوانده شده از فایل ورودی' },
      { metric: 'results', value: results.length, note: 'تعداد ردیف‌های پردازش شده' },
      { metric: 'extractedLinks', value: results.reduce((sum, item) => sum + (item.extractedLinks?.length || 0), 0), note: 'لینک‌های پیدا شده داخل پاسخ بات‌ها' },
      { metric: 'errors', value: results.filter((item) => item.error).length, note: 'ردیف‌هایی که خطای فنی یا تلگرامی گرفته‌اند' },
      ...statusCounts.map((item) => ({ metric: 'status:' + item.name, value: item.count, note: '' })),
      ...typeCounts.map((item) => ({ metric: 'type:' + item.name, value: item.count, note: '' }))
    ]);

    addRowsSheet('results', [
      { header: 'row', key: 'row', width: 8 },
      { header: 'link', key: 'link', width: 55 },
      { header: 'type', key: 'type', width: 18 },
      { header: 'status', key: 'status', width: 18 },
      { header: 'chatId', key: 'chatId', width: 12 },
      { header: 'chatKey', key: 'chatKey', width: 26 },
      { header: 'username', key: 'username', width: 24 },
      { header: 'title', key: 'title', width: 30 },
      { header: 'inviteLink', key: 'inviteLink', width: 55 },
      { header: 'extractedLinksCount', key: 'extractedLinksCount', width: 20 },
      { header: 'reason', key: 'reason', width: 70 },
      { header: 'error', key: 'error', width: 70 }
    ], results.map((item, index) => ({
      row: index + 1,
      link: item.link || '',
      type: item.type || '',
      status: item.status || '',
      chatId: item.chatId || '',
      chatKey: item.chatKey || '',
      username: item.username || '',
      title: item.title || '',
      inviteLink: item.inviteLink || '',
      extractedLinksCount: item.extractedLinks?.length || 0,
      reason: this.linkImportReason(item),
      error: item.error || ''
    })));

    addRowsSheet('errors', [
      { header: 'error', key: 'error', width: 90 },
      { header: 'count', key: 'count', width: 12 }
    ], Array.from(errorCounts.entries())
      .sort((a, b) => b[1] - a[1])
      .map(([error, count]) => ({ error, count })));

    addRowsSheet('extracted_links', [
      { header: 'sourceLink', key: 'sourceLink', width: 55 },
      { header: 'sourceType', key: 'sourceType', width: 18 },
      { header: 'sourceStatus', key: 'sourceStatus', width: 18 },
      { header: 'extractedLink', key: 'extractedLink', width: 55 }
    ], results.flatMap((item) => (item.extractedLinks || []).map((link) => ({
      sourceLink: item.link || '',
      sourceType: item.type || '',
      sourceStatus: item.status || '',
      extractedLink: link
    }))));

    return Buffer.from(await workbook.xlsx.writeBuffer());
  }

  private summarizeImportLinks(stdout: string): string {
    const payload = this.parseImportLinksPayload(stdout);
    const results = Array.isArray(payload.results) ? payload.results : [];
    const statusCounts = new Map<string, number>();
    const typeCounts = new Map<string, number>();
    let nestedLinks = 0;
    let errors = 0;
    for (const item of results) {
      const status = item.status || 'unknown';
      const type = item.type || 'unknown';
      statusCounts.set(status, (statusCounts.get(status) || 0) + 1);
      typeCounts.set(type, (typeCounts.get(type) || 0) + 1);
      nestedLinks += Array.isArray(item.extractedLinks) ? item.extractedLinks.length : 0;
      if (item.error) errors += 1;
    }

    const compactCounts = (counts: Map<string, number>) =>
      Array.from(counts.entries())
        .sort((a, b) => a[0].localeCompare(b[0]))
        .map(([key, value]) => `${key}: ${value}`)
        .join('\\n');

    return [
      'پردازش لینک‌ها تمام شد ✅',
      `لینک‌های ورودی: ${payload.inputLinks ?? results.length}`,
      `نتیجه‌های ذخیره‌شده: ${results.length}`,
      `لینک‌های استخراج‌شده از بات‌ها: ${nestedLinks}`,
      `خطاها: ${errors}`,
      '',
      'وضعیت‌ها:',
      compactCounts(statusCounts) || '-',
      '',
      'نوع لینک‌ها:',
      compactCounts(typeCounts) || '-'
    ].join('\\n');
  }

  private async handleLinkImportFile(ctx: any) {
    const chatId = ctx.chat?.id as number | undefined;
    if (!chatId) return;

    if (this.runningChats.has(chatId)) {
      return ctx.reply('یک پردازش هنوز در حال اجراست؛ لطفاً چند لحظه صبر کن.', this.mainMenuKeyboard);
    }

    const doc = (ctx.message as any)?.document;
    if (!doc?.file_id) {
      return ctx.reply('لطفاً یک فایل اکسل معتبر با پسوند xlsx بفرست.', this.mainMenuKeyboard);
    }
    const fileName = String(doc.file_name || '').trim().toLowerCase();
    if (!fileName.endsWith('.xlsx') && !fileName.endsWith('.xls')) {
      return ctx.reply('برای اضافه کردن لینک‌ها فقط فایل xlsx یا xls قابل قبول است.', this.mainMenuKeyboard);
    }

    this.runningChats.add(chatId);
    const tmpDir = await mkdtemp(join(tmpdir(), 'telegram-link-import-'));
    const inputPath = join(tmpDir, this.tempExcelFileName(fileName));
    try {
      const fileLink = await this.bot.telegram.getFileLink(doc.file_id);
      const response = await fetch(fileLink.toString());
      if (!response.ok) {
        throw new Error(`failed to download file (${response.status})`);
      }
      const content = Buffer.from(await response.arrayBuffer());
      const importContent = fileName.endsWith('.xls')
        ? await this.convertLegacyExcelToXlsx(content, fileName)
        : content;
      await writeFile(inputPath, importContent);
      await ctx.reply('فایل لینک‌ها دریافت شد. پردازش join/start و ذخیره‌سازی شروع شد.', this.mainMenuKeyboard);
      await this.runLinkImport(ctx, tmpDir, ['--input', inputPath]);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await this.bot.telegram.sendMessage(
        chatId,
        `پردازش فایل لینک‌ها ناموفق بود.\nجزئیات فنی: ${message.length > 500 ? message.slice(0, 500) + '...' : message}`,
        this.mainMenuKeyboard
      );
    } finally {
      this.pendingInputMode.delete(chatId);
      this.linkImportTextBuffers.delete(chatId);
      this.runningChats.delete(chatId);
      await rm(tmpDir, { recursive: true, force: true }).catch(() => undefined);
    }
  }

  private async handleLinkImportText(ctx: any, text: string) {
    const chatId = ctx.chat?.id as number | undefined;
    if (!chatId) return;

    if (this.runningChats.has(chatId)) {
      return ctx.reply('یک پردازش هنوز در حال اجراست؛ لطفاً چند لحظه صبر کن.', this.mainMenuKeyboard);
    }

    if (this.isLinkImportFinishText(text)) {
      const collectedText = (this.linkImportTextBuffers.get(chatId) || []).join('\n');
      if (!collectedText) {
        return ctx.reply('هنوز لینکی جمع نشده؛ لینک‌ها را بفرست یا فایل xlsx بده.', this.mainMenuKeyboard);
      }
      return this.processBufferedLinkImportText(ctx, collectedText);
    }

    const linkCount = this.countTelegramLinks(text);
    if (!linkCount) {
      return ctx.reply('این پیام لینک t.me نداشت. لینک‌ها را بفرست و وقتی تمام شد بنویس «تمام».', this.mainMenuKeyboard);
    }

    const buffer = this.linkImportTextBuffers.get(chatId) || [];
    buffer.push(text);
    this.linkImportTextBuffers.set(chatId, buffer);
    const totalLinks = this.countTelegramLinks(buffer.join('\n'));
    return ctx.reply(
      `دریافت شد؛ این پیام ${linkCount} لینک داشت و تا الان حدود ${totalLinks} لینک جمع شده. وقتی همه پیام‌ها را فرستادی بنویس «تمام» یا «پردازش».`,
      this.mainMenuKeyboard
    );
  }

  private isLinkImportFinishText(text: string): boolean {
    return /^(?:تمام|تموم|پایان|پردازش|شروع پردازش|انجام بده|done|process)$/i.test(text.trim());
  }

  private countTelegramLinks(text: string): number {
    return (text.match(/(?:https?:\/\/)?(?:t|telegram)\.me\/[^\s<>)\]]+/gi) || []).length;
  }

  private async processBufferedLinkImportText(ctx: any, text: string) {
    const chatId = ctx.chat?.id as number | undefined;
    if (!chatId) return;

    this.runningChats.add(chatId);
    const tmpDir = await mkdtemp(join(tmpdir(), 'telegram-link-import-'));
    const inputPath = join(tmpDir, 'telegram-link-import-input.txt');
    try {
      await writeFile(inputPath, text, 'utf8');
      await ctx.reply('همه متن‌های لینک دریافت شد. پردازش join/start و ذخیره‌سازی شروع شد.', this.mainMenuKeyboard);
      await this.runLinkImport(ctx, tmpDir, ['--input', inputPath]);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await this.bot.telegram.sendMessage(
        chatId,
        `پردازش متن لینک‌ها ناموفق بود.\nجزئیات فنی: ${message.length > 500 ? message.slice(0, 500) + '...' : message}`,
        this.mainMenuKeyboard
      );
    } finally {
      this.pendingInputMode.delete(chatId);
      this.linkImportTextBuffers.delete(chatId);
      this.runningChats.delete(chatId);
      await rm(tmpDir, { recursive: true, force: true }).catch(() => undefined);
    }
  }

  private async runLinkImport(ctx: any, tmpDir: string, inputArgs: string[]) {
    const chatId = ctx.chat?.id as number | undefined;
    if (!chatId) return;

    const outputPath = join(tmpDir, 'telegram-link-import-result.json');
    const timeout = Math.max(
      60000,
      Number(this.config.get<number>('linkImportTimeoutMs') || 600000)
    );
    const { stdout, stderr } = await execFileAsync(
      'npm',
      [
        'run',
        'telegram:import-links',
        '--',
        ...inputArgs,
        '--joinInvites',
        'true',
        '--joinPublic',
        'true',
        '--startBots',
        'true',
        '--out',
        outputPath
      ],
      {
        cwd: process.cwd(),
        timeout,
        maxBuffer: 20 * 1024 * 1024
      }
    );

    const resultJson = await readFile(outputPath, 'utf8').catch(() => stdout);
    const summary = this.summarizeImportLinks(resultJson || stdout);
    const workbookBuffer = await this.buildLinkImportWorkbook(this.parseImportLinksPayload(resultJson || stdout));
    await this.bot.telegram.sendDocument(chatId, {
      source: workbookBuffer,
      filename: `telegram-link-import-result-${Date.now()}.xlsx`
    } as any);
    await this.bot.telegram.sendMessage(
      chatId,
      stderr?.trim()
        ? `${summary}\n\nهشدار فنی:\n${stderr.trim().slice(0, 700)}`
        : summary,
      this.mainMenuKeyboard
    );
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
    const groupedLinkItems = source === 'links'
      ? this.groupClientMessageLinks(linkItems)
      : [];
    const dbMessageGroups = source === 'links' && numericId !== null
      ? await this.searchService.getMessageGroupsBySearchId(numericId)
      : [];
    const displayMessageGroups = dbMessageGroups.length ? dbMessageGroups : groupedLinkItems.map((entry) => ({
      uid: entry.channelId,
      title: entry.channelId,
      inviteLink: '',
      messageLinks: entry.links
    }));
    const pageSize = source === 'web'
      ? Number(this.config.get<number>('webPageSize') || 10)
      : source === 'telegram'
        ? Number(this.config.get<number>('tgPageSize') || 5)
        : Number(this.config.get<number>('linksPageSize') || 50);
    const itemCount = source === 'links'
      ? Math.max(displayMessageGroups.length, botItems.length ? 1 : 0)
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

    const sourceItems = source === 'links' ? displayMessageGroups : items;
    const slice = sourceItems.slice(safePageIdx * pageSize, (safePageIdx + 1) * pageSize);
    let text = source === 'web'
      ? `نتایج وب، صفحه ${safePageIdx + 1} از ${totalPages}:\n\n`
      : source === 'telegram'
        ? `نتایج تلگرام، صفحه ${safePageIdx + 1} از ${totalPages}:\n\n`
        : '';

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
      if (safePageIdx === 0 && botItems.length) {
        chunks.push(this.formatPlainLinks(botItems));
      }
      if ((slice as SearchMessageGroup[]).length) {
        chunks.push(this.formatSearchMessageGroups('', slice as SearchMessageGroup[]));
      }
      text += chunks.join('\n');
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
