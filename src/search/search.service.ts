import { Injectable } from '@nestjs/common';

// Simple rate limiter to avoid Telegram flood waits
class RateLimiter {
  private lastRequestAt = 0;
  constructor(private readonly minIntervalMs: number) {}
  async wait(): Promise<void> {
    const now = Date.now();
    const elapsed = now - this.lastRequestAt;
    if (elapsed < this.minIntervalMs) {
      await new Promise((resolve) => setTimeout(resolve, this.minIntervalMs - elapsed));
    }
    this.lastRequestAt = Date.now();
  }
}
import { TelegramClient, Api } from 'telegram';
import { StringSession } from 'telegram/sessions';
import { ConfigService } from '@nestjs/config';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { randomBytes } from 'crypto';
import { LoggerService } from '../common/logger.service';
import { CrawlStepEntity } from '../entities/crawl-step.entity';
import { ChannelMatchEntity } from '../entities/channel-match.entity';
import { SearchLinkEntity } from '../entities/search-link.entity';
import { RedisStoreService, StoredChannelType } from '../storage/redis-store.service';

export interface KeywordCrawlResultDto {
  seedPublics: string[];
  links: string[];
  clientLinks: string[];
  invites: string[];
  chatsProcessed: number;
  messagesStored: number;
  iterations: number;
}

interface ChannelMatchTableMeta {
  tableName: string;
  tableRef: string;
  columnsByLower: Map<string, string>;
}

interface SearchLinkTableMeta {
  tableName: string;
  tableRef: string;
  columnsByLower: Map<string, string>;
}

interface FrontierChatState {
  chat: Api.TypeChat;
  discoveredViaLink?: string;
  discoveredFromMessageLink?: string;
  discoveredFromChannel?: string;
}

type ParsedTargetKind = 'invite' | 'bot' | 'public_chat' | 'public_message' | 'private_message' | 'private_chat';

interface ParsedTargetLink {
  kind: ParsedTargetKind;
  canonical: string;
  username?: string;
  botStartParam?: string;
  privateChannelId?: string;
  messageId?: number;
}

interface CrawlRunOptions {
  abortSignal?: AbortSignal;
}

interface MessageMediaMetadata extends Record<string, unknown> {
  type?: string;
  mimeType?: string;
  size?: number;
  fileName?: string;
  duration?: number;
  width?: number;
  height?: number;
  title?: string;
  performer?: string;
  voice?: boolean;
  roundMessage?: boolean;
  supportsStreaming?: boolean;
}

@Injectable()
export class SearchService {
  private readonly client: TelegramClient;
  private readonly apiRateLimiter: RateLimiter;
  private readonly inputPeerCache = new Map<string, Promise<Api.TypeInputPeer>>();
  private readonly historyPageCache = new Map<string, { createdAt: number; messages: Api.Message[] }>();
  private readonly chatBioLinksCache = new Map<string, Promise<string[]>>();
  private readonly historyCacheMaxPages: number;
  private sessionLoaded = false;
  private sessionInitPromise: Promise<void> | null = null;
  private channelMatchSchemaMode: 'unknown' | 'extended' | 'legacy' = 'unknown';
  private channelMatchTableMeta: ChannelMatchTableMeta | null = null;
  private searchLinkTableMeta: SearchLinkTableMeta | null = null;

  constructor(
    private readonly config: ConfigService,
    private readonly logger: LoggerService,
    private readonly redisStore: RedisStoreService,
    @InjectRepository(CrawlStepEntity)
    private readonly crawlStepRepo: Repository<CrawlStepEntity>,
    @InjectRepository(ChannelMatchEntity)
    private readonly channelRepo: Repository<ChannelMatchEntity>,
    @InjectRepository(SearchLinkEntity)
    private readonly searchLinkRepo: Repository<SearchLinkEntity>
  ) {
    // Rate limiter: minimum interval between Telegram API calls to avoid flood waits
    const rateLimitIntervalMs = Math.max(200, Number(this.config.get<number>('crawlRateLimitMs') || 2000));
    this.apiRateLimiter = new RateLimiter(rateLimitIntervalMs);
    this.historyCacheMaxPages = Math.max(
      0,
      Number(this.config.get<number>('crawlHistoryCacheMaxPages') || 500)
    );

    const proxyUrl = process.env.SOCKS_PROXY;
    const proxy = proxyUrl
      ? { ip: '127.0.0.1', port: 10808, socksType: 5 as 5 }
      : undefined;

    this.client = new TelegramClient(
      new StringSession(this.config.get<string>('sessionString') || ''),
      Number(this.config.get<number>('apiId')),
      this.config.get<string>('apiHash') || '',
      {
        connectionRetries: 5,
        ...(proxy ? { proxy } : {})
      }
    );
  }

  private async initUserClient() {
    if (this.sessionLoaded) return;
    if (this.sessionInitPromise) {
      await this.sessionInitPromise;
      return;
    }

    this.sessionInitPromise = (async () => {
      const initTimeoutMs = Math.max(5000, Number(this.config.get<number>('mtprotoInitTimeoutMs') || 30000));
      this.logger.log('Initializing GramJS client', {
        timeoutMs: initTimeoutMs,
        proxyEnabled: Boolean(process.env.SOCKS_PROXY)
      });
      const apiId = Number(this.config.get<number>('apiId'));
      const apiHash = this.config.get<string>('apiHash') || '';
      const sessionString = this.config.get<string>('sessionString') || '';
      if (!apiId || !apiHash || !sessionString) {
        throw new Error(
          'Missing API_ID / API_HASH / SESSION_STRING in environment. Production must use a pre-generated SESSION_STRING.'
        );
      }

      try {
        await this.invokeWithTimeout(this.client.connect(), initTimeoutMs, 'mtproto_connect');
        const isAuthorized = await this.invokeWithTimeout(
          this.client.checkAuthorization(),
          initTimeoutMs,
          'mtproto_check_authorization'
        );
        if (!isAuthorized) {
          throw new Error(
            'SESSION_STRING is not authorized. Generate a valid user session string and set SESSION_STRING.'
          );
        }
      } catch (err) {
        const errAny = err as any;
        if (errAny?.code === 420 && typeof errAny?.seconds === 'number') {
          throw new Error(
            `Telegram flood wait (${errAny.seconds}s) during auth. Avoid repeated auth attempts and use a valid persistent SESSION_STRING.`
          );
        }
        if (err instanceof Error && /timeout after/i.test(err.message)) {
          throw new Error(
            `MTProto init timeout after ${initTimeoutMs}ms. If this host cannot reach Telegram MTProto directly, configure SOCKS_PROXY.`
          );
        }
        throw err;
      }

      this.sessionLoaded = true;
      this.logger.log('MTProto client started');
    })();

    try {
      await this.sessionInitPromise;
    } finally {
      this.sessionInitPromise = null;
    }
  }

  private extractLinksFromText(text: string): string[] {
    const tg = text.match(/(?:https?:\/\/)?(?:t|telegram)\.me\/[^\s)]+/gi) || [];
    const tgScheme = text.match(/tg:\/\/join\?invite=[^\s)]+/gi) || [];
    return [...tg, ...tgScheme].map((u) => (u.startsWith('http') || u.startsWith('tg://') ? u : `https://${u}`));
  }

  private extractLinksFromEntities(text: string, entities: any[]): string[] {
    const links: string[] = [];
    for (const ent of entities || []) {
      const type = ent._ || ent.className || ent.constructor?.name || '';
      if (ent.url && (type === 'messageEntityTextUrl' || type === 'MessageEntityTextUrl')) {
        if (this.isTelegramLink(ent.url)) links.push(ent.url);
      } else if (ent.url && this.isTelegramLink(ent.url)) {
        links.push(ent.url);
      } else if (type === 'messageEntityUrl' || type === 'MessageEntityUrl') {
        const slice = text.substring(ent.offset, ent.offset + ent.length);
        if (slice && this.isTelegramLink(slice)) links.push(slice);
      }
    }
    return links;
  }

  private extractLinksFromReplyMarkup(markup: any): string[] {
    const links: string[] = [];
    const rows = markup?.rows || markup?.inlineKeyboard?.rows || [];
    for (const row of rows) {
      const buttons = row?.buttons || row;
      for (const btn of buttons || []) {
        if (btn?.url && this.isTelegramLink(btn.url)) links.push(btn.url);
      }
    }
    return links;
  }

  private collectTelegramLinks(text: string, entities: any[], replyMarkup: any): string[] {
    const links = [
      ...this.extractLinksFromText(text),
      ...this.extractLinksFromEntities(text, entities || []),
      ...this.extractLinksFromReplyMarkup(replyMarkup)
    ];
    return Array.from(
      new Set(
        links
          .map((l) => {
            const parsed = this.parseTargetLink(l);
            if (!parsed) return null;
            if (parsed.kind === 'bot') return this.normalizeLink(l);
            return parsed.canonical;
          })
          .filter((l): l is string => Boolean(l))
      )
    );
  }

  private normalizeLink(link: string): string {
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

  private isInviteLink(link: string): boolean {
    return /(?:t|telegram)\.me\/\+|(?:t|telegram)\.me\/joinchat|tg:\/\/join\?invite=/i.test(link);
  }

  private isTelegramLink(link: string): boolean {
    return /(?:t|telegram)\.me\//i.test(link) || /^tg:\/\//i.test(link);
  }

  private parseInviteHash(link: string): string | null {
    const plus = link.match(/(?:t|telegram)\.me\/\+([A-Za-z0-9_-]+)/i);
    if (plus?.[1]) return plus[1];
    const joinchat = link.match(/(?:t|telegram)\.me\/joinchat\/([A-Za-z0-9_-]+)/i);
    if (joinchat?.[1]) return joinchat[1];
    const tg = link.match(/tg:\/\/join\?invite=([A-Za-z0-9_-]+)/i);
    if (tg?.[1]) return tg[1];
    return null;
  }

  private extractBotStartParam(link: string): string | undefined {
    try {
      const parsed = new URL(link);
      const start = parsed.searchParams.get('start');
      if (start) return start;
      const startGroup = parsed.searchParams.get('startgroup');
      if (startGroup) return startGroup;
      const startApp = parsed.searchParams.get('startapp');
      if (startApp) return startApp;
      return undefined;
    } catch {
      return undefined;
    }
  }

  private canonicalizeInviteLink(link: string): string | null {
    const hash = this.parseInviteHash(link);
    if (!hash) return null;
    return `https://t.me/+${hash}`;
  }

  private canonicalizeTelegramLink(link: string): string | null {
    const normalized = this.normalizeLink(link);
    if (!normalized || !this.isTelegramLink(normalized)) return null;

    if (this.isInviteLink(normalized)) {
      return this.canonicalizeInviteLink(normalized);
    }

    const username = this.extractUsernameFromLink(normalized);
    if (!username) return null;
    return this.normalizeLink(`https://t.me/${username}`);
  }

  private parseTargetLink(link: string): ParsedTargetLink | null {
    const normalized = this.normalizeLink(link);
    if (!normalized || !this.isTelegramLink(normalized)) return null;

    if (this.isInviteLink(normalized)) {
      const invite = this.canonicalizeInviteLink(normalized);
      return invite ? { kind: 'invite', canonical: invite } : null;
    }

    const privateMsgMatch = normalized.match(/^https:\/\/t\.me\/c\/(\d+)\/(\d+)(?:[/?#].*)?$/i);
    if (privateMsgMatch) {
      const messageId = Number.parseInt(privateMsgMatch[2], 10);
      if (!Number.isFinite(messageId) || messageId <= 0) return null;
      return {
        kind: 'private_message',
        canonical: `https://t.me/c/${privateMsgMatch[1]}/${messageId}`,
        privateChannelId: privateMsgMatch[1],
        messageId
      };
    }

    const privateChatMatch = normalized.match(/^https:\/\/t\.me\/c\/(\d+)\/?(?:[?#].*)?$/i);
    if (privateChatMatch) {
      return {
        kind: 'private_chat',
        canonical: `https://t.me/c/${privateChatMatch[1]}`,
        privateChannelId: privateChatMatch[1]
      };
    }

    const publicMsgMatch = normalized.match(/^https:\/\/t\.me\/(?:s\/)?([A-Za-z0-9_]+)\/(\d+)(?:[/?#].*)?$/i);
    if (publicMsgMatch) {
      const username = this.extractUsernameFromLink(`https://t.me/${publicMsgMatch[1]}`);
      if (!username) return null;
      const messageId = Number.parseInt(publicMsgMatch[2], 10);
      if (!Number.isFinite(messageId) || messageId <= 0) return null;
      return {
        kind: 'public_message',
        canonical: `https://t.me/${username}/${messageId}`,
        username,
        messageId
      };
    }

    const username = this.extractUsernameFromLink(normalized);
    if (!username) return null;
    const canonical = this.normalizeLink(`https://t.me/${username}`);
    if (/bot$/i.test(username)) {
      return {
        kind: 'bot',
        canonical,
        username,
        botStartParam: this.extractBotStartParam(normalized)
      };
    }
    return {
      kind: 'public_chat',
      canonical,
      username
    };
  }

  private getChatUsername(chat: Api.TypeChat): string | undefined {
    return 'username' in chat ? chat.username : undefined;
  }

  private getChatIdentityText(chat: Api.TypeChat): string {
    const chatAny = chat as any;
    const parts: string[] = [];
    const username = this.getChatUsername(chat);
    if (username) parts.push(username);
    if (typeof chatAny?.title === 'string' && chatAny.title.trim()) parts.push(chatAny.title.trim());
    if (typeof chatAny?.firstName === 'string' && chatAny.firstName.trim()) parts.push(chatAny.firstName.trim());
    if (typeof chatAny?.lastName === 'string' && chatAny.lastName.trim()) parts.push(chatAny.lastName.trim());

    const usernames = Array.isArray(chatAny?.usernames) ? chatAny.usernames : [];
    for (const item of usernames) {
      const uname = typeof item === 'string'
        ? item
        : typeof item?.username === 'string'
          ? item.username
          : '';
      if (uname.trim()) parts.push(uname.trim());
    }

    return parts.join(' ').trim();
  }

  private chatIdentityContainsKeyword(chat: Api.TypeChat, keywordTerms: string[]): boolean {
    const text = this.getChatIdentityText(chat);
    if (!text) return false;
    return this.messageContainsKeyword(text, keywordTerms);
  }

  private classifyChatType(chat: Api.TypeChat): StoredChannelType {
    const chatAny = chat as any;
    const kind = String(chatAny?._ || '').toLowerCase();
    if (kind.includes('user') && chatAny?.bot) return 'bot';
    if (kind.includes('channel')) {
      return chatAny?.broadcast ? 'channel' : 'group';
    }
    if (kind.includes('chat')) return 'group';
    return 'unknown';
  }

  private buildChannelLink(chat: Api.TypeChat): string | undefined {
    const username = this.getChatUsername(chat);
    if (!username) return undefined;
    return this.normalizeLink(`https://t.me/${username}`);
  }

  private buildMessageLink(chat: Api.TypeChat, messageId: number): string | undefined {
    const username = this.getChatUsername(chat);
    if (username) {
      return this.normalizeLink(`https://t.me/${username}/${messageId}`);
    }

    const chatAny = chat as any;
    const numericId = Number(chatAny?.id || 0);
    if (!numericId) return undefined;
    const abs = Math.abs(numericId);
    const absStr = String(abs);
    const internal = absStr.startsWith('100') ? absStr.slice(3) : absStr;
    if (!internal) return undefined;
    return `https://t.me/c/${internal}/${messageId}`;
  }

  private getChatKey(chat: Api.TypeChat): string {
    const username = this.getChatUsername(chat);
    return username || String((chat as any).id || '');
  }

  private normalizeForSearch(text: string): string {
    return text
      .normalize('NFKC')
      .toLowerCase()
      .replace(/\u0640/g, '')
      .replace(/\p{Cf}+/gu, '')
      .replace(/[أإٱآ]/g, 'ا')
      .replace(/ة/g, 'ه')
      .replace(/ؤ/g, 'و')
      .replace(/ئ/g, 'ی')
      .replace(/[يى]/g, 'ی')
      .replace(/ك/g, 'ک')
      .replace(/\p{M}+/gu, '');
  }

  private levenshteinWithin(a: string, b: string, maxDistance: number): boolean {
    if (a === b) return true;
    if (maxDistance <= 0) return false;
    if (Math.abs(a.length - b.length) > maxDistance) return false;

    const bLen = b.length;
    let prev = new Array<number>(bLen + 1);
    for (let j = 0; j <= bLen; j += 1) prev[j] = j;

    for (let i = 1; i <= a.length; i += 1) {
      const curr = new Array<number>(bLen + 1);
      curr[0] = i;

      const from = Math.max(1, i - maxDistance);
      const to = Math.min(bLen, i + maxDistance);

      for (let j = 1; j < from; j += 1) curr[j] = maxDistance + 1;
      let rowMin = curr[0];

      for (let j = from; j <= to; j += 1) {
        const cost = a.charCodeAt(i - 1) === b.charCodeAt(j - 1) ? 0 : 1;
        const insertCost = curr[j - 1] + 1;
        const deleteCost = prev[j] + 1;
        const replaceCost = prev[j - 1] + cost;
        const best = Math.min(insertCost, deleteCost, replaceCost);
        curr[j] = best;
        if (best < rowMin) rowMin = best;
      }

      for (let j = to + 1; j <= bLen; j += 1) curr[j] = maxDistance + 1;
      if (rowMin > maxDistance) return false;
      prev = curr;
    }

    return prev[bLen] <= maxDistance;
  }

  private fuzzyIncludes(tokens: string[], term: string, maxDistance: number): boolean {
    if (!tokens.length || !term) return false;
    if (maxDistance <= 0) return false;

    const termLen = term.length;
    const termFirst = term.charAt(0);
    const termLast = term.charAt(termLen - 1);

    for (const token of tokens) {
      if (!token) continue;
      if (Math.abs(token.length - termLen) > maxDistance) continue;
      if (token.charAt(0) !== termFirst) continue;
      if (termLen >= 4 && token.charAt(token.length - 1) !== termLast) continue;
      if (this.levenshteinWithin(token, term, maxDistance)) return true;
    }

    return false;
  }

  private fuzzyWindowIncludes(tokens: string[], term: string, maxDistance: number): boolean {
    if (!tokens.length || !term || maxDistance <= 0) return false;
    const maxWindow = Math.min(tokens.length, Math.max(1, Math.ceil(term.length / 2)));
    for (let start = 0; start < tokens.length; start += 1) {
      let joined = '';
      for (let size = 1; size <= maxWindow && start + size <= tokens.length; size += 1) {
        joined += tokens[start + size - 1];
        if (Math.abs(joined.length - term.length) > maxDistance) continue;
        if (this.levenshteinWithin(joined, term, maxDistance)) return true;
      }
    }
    return false;
  }

  private hasEmojiLikeContent(text: string): boolean {
    return /[\p{Emoji_Presentation}\p{Extended_Pictographic}\uFE0F]/u.test(text);
  }

  private isMostlyEmojiOrSymbols(text: string): boolean {
    const normalized = text.replace(/\s+/g, '');
    if (!normalized) return false;
    const withoutEmoji = normalized.replace(/[\p{Emoji_Presentation}\p{Extended_Pictographic}\uFE0F\u200D]/gu, '');
    return withoutEmoji.replace(/[\p{P}\p{S}]/gu, '').length === 0;
  }

  private hasMediaTitleContext(text: string): boolean {
    const tokens = new Set(this.tokenizeForSearch(text));
    return [
      'سریال',
      'فیلم',
      'فصل',
      'قسمت',
      'دانلود',
      'لینک',
      'قرار',
      'اپیزود',
      'episode',
      'season',
      'series',
      'movie'
    ].some((token) => tokens.has(token));
  }

  private containsEmojiObfuscatedKeyword(text: string, keywordTerms: string[]): boolean {
    if (!this.config.get<boolean>('crawlFuzzyEmojiWildcard')) return false;
    if (!this.hasEmojiLikeContent(text) || !this.hasMediaTitleContext(text)) return false;
    const compactKeyword = keywordTerms.map((term) => this.compactForSearch(term)).join('');
    if (compactKeyword.length < 3) return false;

    const quoteMatches = Array.from(text.matchAll(/[«"'“”‘’](.*?)[»"'“”‘’]/gs));
    for (const match of quoteMatches) {
      const inner = String(match[1] || '');
      if (this.isMostlyEmojiOrSymbols(inner)) return true;
    }

    return /(?:[\p{Emoji_Presentation}\p{Extended_Pictographic}\uFE0F]\s*){2,}/u.test(text);
  }

  private tokenizeForSearch(text: string): string[] {
    return this.normalizeForSearch(text).match(/[\p{L}\p{N}]+/gu) || [];
  }

  private compactForSearch(text: string): string {
    return this.tokenizeForSearch(text).join('');
  }

  private splitKeywordTerms(keyword: string): string[] {
    const tokens = this.tokenizeForSearch(keyword).filter(Boolean);
    if (tokens.length) return tokens;
    const compact = this.compactForSearch(keyword);
    return compact ? [compact] : [];
  }

  private messageContainsKeyword(text: string, keywordTerms: string[]): boolean {
    if (!keywordTerms.length) return false;
    if (this.containsEmojiObfuscatedKeyword(text, keywordTerms)) return true;
    const normalizedTokens = this.tokenizeForSearch(text).filter(Boolean);
    if (!normalizedTokens.length) return false;
    const compactText = normalizedTokens.join('');
    const terms = keywordTerms.map((part) => this.compactForSearch(part)).filter(Boolean);
    if (!terms.length) return false;

    const fuzzyEnabled = this.config.get<boolean>('crawlFuzzyEnabled') !== false;
    const fuzzyMinLen = Math.max(1, Number(this.config.get<number>('crawlFuzzyMinTermLength') || 4));
    const fuzzyMaxDistanceCfg = Math.max(0, Number(this.config.get<number>('crawlFuzzyMaxDistance') || 1));
    const joinedTerm = terms.join('');
    if (compactText.includes(joinedTerm)) return true;
    if (fuzzyEnabled && joinedTerm.length >= fuzzyMinLen && fuzzyMaxDistanceCfg > 0) {
      const adaptiveDistance = Math.max(1, Math.floor(joinedTerm.length * 0.25));
      const maxDistance = Math.min(fuzzyMaxDistanceCfg, adaptiveDistance);
      if (this.fuzzyWindowIncludes(normalizedTokens, joinedTerm, maxDistance)) return true;
    }

    return terms.every((part) => {
      if (compactText.includes(part)) return true;
      if (!fuzzyEnabled) return false;
      if (part.length < fuzzyMinLen) return false;
      if (fuzzyMaxDistanceCfg <= 0) return false;

      const adaptiveDistance = Math.max(1, Math.floor(part.length * 0.25));
      const maxDistance = Math.min(fuzzyMaxDistanceCfg, adaptiveDistance);
      if (maxDistance <= 0) return false;

      return this.fuzzyIncludes(normalizedTokens, part, maxDistance)
        || this.fuzzyWindowIncludes(normalizedTokens, part, maxDistance);
    });
  }

  private extractUsernameFromLink(link: string): string | null {
    const normalized = this.normalizeLink(link);
    const base = normalized.replace(/^https?:\/\/(?:www\.)?(?:t|telegram)\.me\//i, '');
    const path = base.split(/[?#]/)[0];
    const segments = path.split('/').filter(Boolean);
    if (!segments.length) return null;

    let candidate = segments[0];
    if (candidate.toLowerCase() === 's' && segments[1]) {
      candidate = segments[1];
    }
    if (candidate.startsWith('+')) return null;

    const reserved = new Set(['joinchat', 'addlist', 'c', 's', 'share', 'proxy', 'blog', 'iv']);
    if (reserved.has(candidate.toLowerCase())) return null;
    if (!/^[A-Za-z0-9_]+$/.test(candidate)) return null;
    return candidate.trim();
  }

  private isVideoOrClipMessage(msgAny: any): boolean {
    const media = msgAny?.media;
    if (!media) return false;

    const mediaType = String(media?._ || media?.className || media?.constructor?.name || '').toLowerCase();
    if (mediaType.includes('video') || mediaType.includes('roundvideo')) {
      return true;
    }

    const mimeType = String(media?.document?.mimeType || media?.mimeType || '').toLowerCase();
    if (mimeType.startsWith('video/')) {
      return true;
    }

    const attrs = Array.isArray(media?.document?.attributes)
      ? media.document.attributes
      : Array.isArray(media?.attributes)
        ? media.attributes
        : [];

    for (const attr of attrs) {
      const attrType = String(attr?._ || attr?.className || attr?.constructor?.name || '').toLowerCase();
      if (attrType.includes('documentattributevideo') || attrType.includes('roundvideo')) {
        return true;
      }
      if (typeof attr?.roundMessage === 'boolean' && attr.roundMessage) {
        return true;
      }
      if (typeof attr?.supportsStreaming === 'boolean' && attr.supportsStreaming) {
        return true;
      }
    }

    if (typeof msgAny?.video === 'boolean' && msgAny.video) return true;
    if (typeof msgAny?.roundMessage === 'boolean' && msgAny.roundMessage) return true;
    return false;
  }

  private extractMediaMetadata(msgAny: any): MessageMediaMetadata | undefined {
    const media = msgAny?.media;
    if (!media) return undefined;

    const document = media?.document || media;
    const attrs = Array.isArray(document?.attributes)
      ? document.attributes
      : Array.isArray(media?.attributes)
        ? media.attributes
        : [];

    const metadata: MessageMediaMetadata = {
      type: String(media?._ || media?.className || media?.constructor?.name || '').replace(/^MessageMedia/i, '') || undefined,
      mimeType: document?.mimeType || media?.mimeType || undefined,
      size: Number.isFinite(Number(document?.size)) ? Number(document.size) : undefined
    };

    for (const attr of attrs) {
      const attrType = String(attr?._ || attr?.className || attr?.constructor?.name || '').toLowerCase();
      if (typeof attr?.fileName === 'string' && attr.fileName.trim()) metadata.fileName = attr.fileName.trim();
      if (typeof attr?.duration === 'number') metadata.duration = attr.duration;
      if (typeof attr?.w === 'number') metadata.width = attr.w;
      if (typeof attr?.h === 'number') metadata.height = attr.h;
      if (typeof attr?.title === 'string' && attr.title.trim()) metadata.title = attr.title.trim();
      if (typeof attr?.performer === 'string' && attr.performer.trim()) metadata.performer = attr.performer.trim();
      if (typeof attr?.voice === 'boolean') metadata.voice = attr.voice;
      if (typeof attr?.roundMessage === 'boolean') metadata.roundMessage = attr.roundMessage;
      if (typeof attr?.supportsStreaming === 'boolean') metadata.supportsStreaming = attr.supportsStreaming;
      if (attrType.includes('documentattributevideo') && metadata.type === undefined) metadata.type = 'Video';
      if (attrType.includes('documentattributeaudio') && metadata.type === undefined) metadata.type = 'Audio';
    }

    const hasValue = Object.values(metadata).some((value) => value !== undefined && value !== '');
    return hasValue ? metadata : undefined;
  }

  private mediaMetadataToText(metadata?: MessageMediaMetadata): string {
    if (!metadata) return '';
    return [
      metadata.fileName,
      metadata.title,
      metadata.performer,
      metadata.mimeType,
      metadata.type
    ].filter(Boolean).join(' ');
  }

  private buildMetadataQueries(metadata?: MessageMediaMetadata): string[] {
    if (!metadata) return [];
    const candidates = new Set<string>();
    const add = (value?: string) => {
      const normalized = String(value || '')
        .replace(/\.[A-Za-z0-9]{1,6}$/g, ' ')
        .replace(/[._\-()[\]{}]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
      if (normalized.length >= 3) candidates.add(normalized);
    };

    add(metadata.fileName);
    add(metadata.title);
    if (metadata.title && metadata.performer) add(metadata.performer + ' ' + metadata.title);

    const text = [metadata.fileName, metadata.title, metadata.performer].filter(Boolean).join(' ');
    for (const token of this.tokenizeForSearch(text)) {
      if (token.length >= 4 && !/^\d+$/.test(token)) candidates.add(token);
    }

    return Array.from(candidates).slice(0, 6);
  }

  private async joinInviteIfNeeded(link: string, abortSignal?: AbortSignal): Promise<Api.TypeChat | null> {
    const hash = this.parseInviteHash(link);
    if (!hash) return null;
    try {
      const checked: any = await this.invokeWithTimeout(
        this.client.invoke(
          new Api.messages.CheckChatInvite({ hash } as any)
        ),
        15000,
        'iterative_check_chat_invite',
        abortSignal
      );
      if (checked?.chat) return checked.chat as Api.TypeChat;

      const res: any = await this.invokeWithTimeout(
        this.client.invoke(
          new Api.messages.ImportChatInvite({ hash } as any)
        ),
        15000,
        'iterative_import_chat_invite',
        abortSignal
      );
      if (res?.chats?.length) return res.chats[0] as Api.TypeChat;
    } catch (err) {
      this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
      const message = err instanceof Error ? err.message : String(err);
      if (this.isAlreadyJoinedResult(message)) {
        this.logger.log('ImportChatInvite already joined', { link, result: message });
        return null;
      }
      this.logger.warn('ImportChatInvite failed', { link, error: message });
    }
    return null;
  }

  private async leaveChannelIfNeeded(chat: Api.TypeChat, searchId: number): Promise<void> {
    const chatAny = chat as any;
    const chatType = String(chatAny?._ || '').toLowerCase();
    if (!chatType.includes('channel')) return;
    const chatKey = this.getChatKey(chat);
    try {
      await this.invokeWithTimeout(
        this.client.invoke(
          new Api.channels.LeaveChannel({
            channel: chat as any
          } as any)
        ),
        15000,
        'iterative_leave_channel'
      );
      await this.logStep(searchId, 'iterative_private_chat_left', { chat: chatKey });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await this.logStep(searchId, 'iterative_private_chat_leave_error', { chat: chatKey, error: message });
    }
  }

  private async leaveJoinedPrivateChats(chats: Api.TypeChat[], searchId: number): Promise<void> {
    for (const chat of chats) {
      await this.leaveChannelIfNeeded(chat, searchId);
    }
  }

  private randomLong(): bigint {
    const raw = randomBytes(8).toString('hex');
    const asBigInt = BigInt(`0x${raw}`);
    return BigInt.asUintN(63, asBigInt);
  }

  private async startBotIfNeeded(
    username: string,
    startParam: string | undefined,
    searchId: number,
    abortSignal?: AbortSignal
  ): Promise<void> {
    const command = startParam ? `/start ${startParam}` : '/start';
    try {
      const peer = await this.invokeWithTimeout(
        this.client.getInputEntity(`@${username}`) as Promise<Api.TypeInputPeer>,
        15000,
        'iterative_bot_start_input',
        abortSignal
      );
      await this.invokeWithTimeout(
        this.client.invoke(
          new Api.messages.SendMessage({
            peer: peer as any,
            message: command,
            randomId: this.randomLong() as any,
            noWebpage: true
          } as any)
        ),
        15000,
        'iterative_bot_start_send',
        abortSignal
      );
      await this.logStep(searchId, 'iterative_bot_started', {
        bot: username,
        startParam: startParam || null
      });
    } catch (err) {
      this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
      const message = err instanceof Error ? err.message : String(err);
      await this.logStep(searchId, 'iterative_bot_start_error', {
        bot: username,
        startParam: startParam || null,
        error: message
      });
    }
  }

  private async resolveChatFromParsedLink(
    parsed: ParsedTargetLink,
    autoJoinInvites: boolean,
    abortSignal?: AbortSignal
  ): Promise<Api.TypeChat | null> {
    if (parsed.kind === 'invite') {
      if (!autoJoinInvites) return null;
      return this.joinInviteIfNeeded(parsed.canonical, abortSignal);
    }

    if (parsed.username) {
      try {
        return await this.invokeWithTimeout(
          this.client.getEntity(`@${parsed.username}`) as Promise<Api.TypeChat>,
          15000,
          'iterative_resolve_chat_by_username',
          abortSignal
        );
      } catch {
        return null;
      }
    }

    if ((parsed.kind === 'private_message' || parsed.kind === 'private_chat') && parsed.privateChannelId) {
      try {
        const fullId = BigInt(`-100${parsed.privateChannelId}`);
        return await this.invokeWithTimeout(
          this.client.getEntity(fullId as any) as Promise<Api.TypeChat>,
          15000,
          'iterative_resolve_chat_by_private_id',
          abortSignal
        );
      } catch {
        return null;
      }
    }

    return null;
  }

  private classifyClientLinkType(link: string): string {
    const parsed = this.parseTargetLink(link);
    if (!parsed) return 'message';
    if (parsed.kind === 'bot') return 'bot';
    if (parsed.kind === 'invite') return 'invite';
    if (parsed.kind === 'public_chat' || parsed.kind === 'private_chat') return 'chat';
    return 'message';
  }

  private async targetMessageContainsKeyword(
    chat: Api.TypeChat,
    messageId: number,
    keywordTerms: string[],
    abortSignal?: AbortSignal
  ): Promise<boolean> {
    if (!Number.isFinite(messageId) || messageId <= 0) return false;
    let inputPeer: Api.TypeInputPeer;
    try {
      inputPeer = await this.invokeWithTimeout(
        this.client.getInputEntity(chat) as Promise<Api.TypeInputPeer>,
        15000,
        'target_message_input_peer',
        abortSignal
      );
    } catch {
      this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
      return false;
    }

    try {
      const page = await this.fetchHistoryPage(
        inputPeer,
        messageId + 1,
        5,
        15000,
        abortSignal
      );
      const exact = page.find((m) => Number((m as any)?.id || 0) === messageId);
      if (!exact) return false;
      const text = String((exact as any)?.message || '');
      return this.messageContainsKeyword(text, keywordTerms);
    } catch {
      this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
      return false;
    }
  }

  private async extractBotBioLinks(username: string, searchId: number, abortSignal?: AbortSignal): Promise<string[]> {
    try {
      const input = await this.invokeWithTimeout(
        this.client.getInputEntity(`@${username}`) as unknown as Promise<Api.TypeInputUser>,
        15000,
        'bot_bio_input_user',
        abortSignal
      );
      const full = await this.invokeWithTimeout(
        this.client.invoke(
          new Api.users.GetFullUser({
            id: input as any
          } as any)
        ),
        15000,
        'bot_bio_get_full_user',
        abortSignal
      );
      const about = String((full as any)?.fullUser?.about || '');
      if (!about.trim()) return [];
      const links = this.collectTelegramLinks(about, [], undefined);
      await this.logStep(searchId, 'iterative_bot_bio_links', { bot: username, links: links.length });
      return links;
    } catch (err) {
      this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
      const message = err instanceof Error ? err.message : String(err);
      await this.logStep(searchId, 'iterative_bot_bio_links_error', { bot: username, error: message });
      return [];
    }
  }

  private async extractChatBioLinks(
    chat: Api.TypeChat,
    searchId: number,
    abortSignal?: AbortSignal
  ): Promise<string[]> {
    const chatAny = chat as any;
    const chatType = String(chatAny?._ || '').toLowerCase();
    const chatKey = this.getChatKey(chat);
    try {
      let about = '';
      if (chatType.includes('channel')) {
        const input = await this.invokeWithTimeout(
          this.client.getInputEntity(chat) as unknown as Promise<Api.TypeInputChannel>,
          15000,
          'chat_bio_input_channel',
          abortSignal
        );
        const full = await this.invokeWithTimeout(
          this.client.invoke(
            new Api.channels.GetFullChannel({
              channel: input as any
            } as any)
          ),
          15000,
          'chat_bio_get_full_channel',
          abortSignal
        );
        about = String((full as any)?.fullChat?.about || '');
      } else if (chatType.includes('chat')) {
        const chatId = Number(chatAny?.id || 0);
        if (!Number.isFinite(chatId) || chatId <= 0) return [];
        const full = await this.invokeWithTimeout(
          this.client.invoke(
            new Api.messages.GetFullChat({
              chatId: chatId as any
            } as any)
          ),
          15000,
          'chat_bio_get_full_chat',
          abortSignal
        );
        about = String((full as any)?.fullChat?.about || '');
      } else {
        return [];
      }

      if (!about.trim()) return [];
      const links = this.collectTelegramLinks(about, [], undefined);
      await this.logStep(searchId, 'iterative_chat_bio_links', { chat: chatKey, links: links.length });
      return links;
    } catch (err) {
      this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
      const message = err instanceof Error ? err.message : String(err);
      await this.logStep(searchId, 'iterative_chat_bio_links_error', { chat: chatKey, error: message });
      return [];
    }
  }

  private async resolveParsedLinkForFrontier(params: {
    parsed: ParsedTargetLink;
    autoJoinInvites: boolean;
    keywordTerms: string[];
    searchId: number;
    joinedPrivateChats: Map<string, Api.TypeChat>;
    abortSignal?: AbortSignal;
  }): Promise<Api.TypeChat | null> {
    const {
      parsed,
      autoJoinInvites,
      keywordTerms,
      searchId,
      joinedPrivateChats,
      abortSignal
    } = params;

    let resolved: Api.TypeChat | null = null;
    if (parsed.kind === 'invite') {
      if (!autoJoinInvites) return null;
      resolved = await this.joinInviteIfNeeded(parsed.canonical, abortSignal);
      if (resolved) {
        const joinedKey = this.getChatKey(resolved);
        if (joinedKey) joinedPrivateChats.set(joinedKey, resolved);
      }
    } else {
      resolved = await this.resolveChatFromParsedLink(parsed, autoJoinInvites, abortSignal);
    }
    if (!resolved) return null;

    if (parsed.messageId) {
      const targetHasKeyword = await this.targetMessageContainsKeyword(
        resolved,
        parsed.messageId,
        keywordTerms,
        abortSignal
      );
      if (!targetHasKeyword) {
        if (this.chatIdentityContainsKeyword(resolved, keywordTerms)) {
          await this.logStep(searchId, 'iterative_link_target_keyword_fallback_identity', {
            link: parsed.canonical,
            messageId: parsed.messageId
          });
          return resolved;
        }
        await this.logStep(searchId, 'iterative_link_target_keyword_miss', {
          link: parsed.canonical,
          messageId: parsed.messageId
        });
        return null;
      }
    }

    return resolved;
  }

  private isAccessDeniedError(errorMessage: string): boolean {
    return /CHANNEL_PRIVATE|USER_NOT_PARTICIPANT|CHAT_ADMIN_REQUIRED|CHANNEL_INVALID|CHANNEL_PUBLIC_GROUP_NA/i
      .test(errorMessage);
  }

  private isAlreadyJoinedResult(errorMessage: string): boolean {
    return /USER_ALREADY_PARTICIPANT|INVITE_REQUEST_SENT|ALREADY/i.test(errorMessage);
  }

  private async tryJoinChat(
    chat: Api.TypeChat,
    searchId: number,
    reason: string,
    abortSignal?: AbortSignal
  ): Promise<boolean> {
    const chatAny = chat as any;
    const chatType = String(chatAny?._ || '').toLowerCase();
    if (!chatType.includes('channel')) return false;

    const chatKey = this.getChatKey(chat);
    try {
      await this.invokeWithTimeout(
        this.client.invoke(
          new Api.channels.JoinChannel({
            channel: chat as any
          } as any)
        ),
        15000,
        `${reason}_join_channel`,
        abortSignal
      );
      await this.logStep(searchId, 'iterative_chat_joined', { chat: chatKey, reason });
      return true;
    } catch (err) {
      this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
      const message = err instanceof Error ? err.message : String(err);
      if (this.isAlreadyJoinedResult(message)) {
        await this.logStep(searchId, 'iterative_chat_join_ack', { chat: chatKey, reason, result: message });
        return true;
      }
      await this.logStep(searchId, 'iterative_chat_join_error', { chat: chatKey, reason, error: message });
      return false;
    }
  }

  private async getInputPeerForChat(
    chat: Api.TypeChat,
    chatKey: string,
    label: string,
    abortSignal?: AbortSignal
  ): Promise<Api.TypeInputPeer> {
    const existing = this.inputPeerCache.get(chatKey);
    if (existing) return existing;

    const promise = this.invokeWithTimeout(
      this.client.getInputEntity(chat) as Promise<Api.TypeInputPeer>,
      15000,
      label,
      abortSignal
    );
    this.inputPeerCache.set(chatKey, promise);
    return promise;
  }

  private async fetchHistoryPage(
    peer: Api.TypeInputPeer,
    offsetId: number,
    limit: number,
    timeoutMs: number,
    abortSignal?: AbortSignal,
    cacheKey?: string
  ): Promise<Api.Message[]> {
    const now = Date.now();
    if (cacheKey) {
      const cached = this.historyPageCache.get(cacheKey);
      if (cached && now - cached.createdAt < 60 * 60 * 1000) {
        return cached.messages;
      }
    }

    const hist = await this.invokeWithTimeout(
      this.client.invoke(
        new Api.messages.GetHistory({
          peer: peer as any,
          offsetId: offsetId as any,
          addOffset: 0,
          limit,
          maxId: 0 as any,
          minId: 0 as any,
          hash: 0 as any
        } as any)
      ),
      timeoutMs,
      'iterative_chat_history_page',
      abortSignal
    );
    const histAny = hist as any;
    const messages = (histAny.messages || []).filter((m: any) => Boolean(m?.message)) as Api.Message[];
    if (cacheKey) {
      this.historyPageCache.set(cacheKey, { createdAt: now, messages });
      if (this.historyCacheMaxPages === 0) {
        this.historyPageCache.clear();
      } else if (this.historyPageCache.size > this.historyCacheMaxPages) {
        const overflow = this.historyPageCache.size - this.historyCacheMaxPages;
        const keysToDelete = Array.from(this.historyPageCache.keys()).slice(0, Math.max(overflow, 50));
        for (const key of keysToDelete) this.historyPageCache.delete(key);
      }
    }
    return messages;
  }

  clearRuntimeCaches() {
    this.inputPeerCache.clear();
    this.historyPageCache.clear();
    this.chatBioLinksCache.clear();
  }

  private normalizeAbortReason(reason: unknown, label: string): Error {
    if (reason instanceof Error) return reason;
    const reasonText = typeof reason === 'string' && reason.trim() ? `: ${reason.trim()}` : '';
    const err = new Error(`${label} aborted${reasonText}`);
    err.name = 'AbortError';
    return err;
  }

  private throwIfAborted(abortSignal: AbortSignal | undefined, label: string): void {
    if (!abortSignal?.aborted) return;
    throw this.normalizeAbortReason(abortSignal.reason, label);
  }

  private async invokeWithTimeout<T>(
    promise: Promise<T>,
    timeoutMs: number,
    label: string,
    abortSignal?: AbortSignal
  ): Promise<T> {
    this.throwIfAborted(abortSignal, label);
    const startedAt = Date.now();
    const guardedPromise = promise.then(
      (value) => ({ ok: true as const, value }),
      (err) => ({ ok: false as const, err })
    );
    // Rate limit before any Telegram API call to avoid flood waits
    await this.apiRateLimiter.wait();

    return new Promise<T>((resolve, reject) => {
      let settled = false;
      let timer: NodeJS.Timeout | null = null;
      const finish = (handler: () => void) => {
        if (settled) return;
        settled = true;
        const elapsedMs = Date.now() - startedAt;
        if (elapsedMs >= 5000) {
          this.logger.warn('Slow Telegram API call', { label, elapsedMs, timeoutMs });
        }
        if (timer) clearTimeout(timer);
        if (abortSignal) {
          abortSignal.removeEventListener('abort', onAbort);
        }
        handler();
      };

      const onAbort = () => {
        finish(() => reject(this.normalizeAbortReason(abortSignal?.reason, label)));
      };

      if (abortSignal) {
        abortSignal.addEventListener('abort', onAbort, { once: true });
      }

      timer = setTimeout(() => {
        finish(() => reject(new Error(`${label} timeout after ${timeoutMs}ms`)));
      }, timeoutMs);

      guardedPromise.then((result) => {
        if (result.ok) {
          finish(() => resolve(result.value));
        } else {
          finish(() => reject(result.err));
        }
      });
    });
  }

  private async logStep(searchId: number, step: string, details?: Record<string, unknown>) {
    this.logger.log(`Crawl step: ${step}`, details ? { details } : undefined);
    const persistSteps = this.config.get<boolean>('crawlPersistSteps') === true;
    if (!persistSteps) return;
    const timeoutMs = Math.max(500, Number(this.config.get<number>('crawlDbTimeoutMs') || 5000));
    try {
      await this.invokeWithTimeout(
        this.crawlStepRepo.save({
          search_id: searchId,
          step,
          details: details ? JSON.stringify(details) : undefined
        }),
        timeoutMs,
        'crawl_step_save'
      );
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.warn('Crawl step save failed', { step, error: message });
    }
  }

  private isOracleUniqueViolation(errorMessage: string): boolean {
    return /ORA-00001|unique constraint/i.test(errorMessage);
  }

  private isOracleInvalidIdentifier(errorMessage: string): boolean {
    return /ORA-00904|invalid identifier/i.test(errorMessage);
  }

  private quoteIdentifier(identifier: string): string {
    return `"${identifier.replace(/"/g, '""')}"`;
  }

  private tableRefForName(tableName: string): string {
    if (/^[A-Z][A-Z0-9_$#]*$/.test(tableName)) return tableName;
    return this.quoteIdentifier(tableName);
  }

  private truncateVarchar(value: string, maxLength: number): string {
    if (value.length <= maxLength) return value;
    return value.slice(0, maxLength);
  }

  private async getChannelMatchTableMeta(forceRefresh = false): Promise<ChannelMatchTableMeta> {
    if (!forceRefresh && this.channelMatchTableMeta) {
      return this.channelMatchTableMeta;
    }

    const tableRows = await this.channelRepo.query(`
      SELECT TABLE_NAME
      FROM USER_TABLES
      WHERE LOWER(TABLE_NAME) = 'channel_matches'
      ORDER BY CASE
        WHEN TABLE_NAME = 'channel_matches' THEN 0
        WHEN TABLE_NAME = 'CHANNEL_MATCHES' THEN 1
        ELSE 2
      END
    `);
    if (!tableRows?.length) {
      throw new Error('channel_matches table not found in current Oracle schema');
    }
    const tableName = String(tableRows[0]?.TABLE_NAME || tableRows[0]?.table_name || '').trim();
    if (!tableName) {
      throw new Error('channel_matches table name resolution failed');
    }
    const safeTableName = tableName.replace(/'/g, "''");
    const columnRows = await this.channelRepo.query(
      `SELECT COLUMN_NAME FROM USER_TAB_COLS WHERE TABLE_NAME = '${safeTableName}'`
    );
    const columnsByLower = new Map<string, string>();
    for (const row of columnRows || []) {
      const columnName = String(row?.COLUMN_NAME || row?.column_name || '').trim();
      if (!columnName) continue;
      if (!columnsByLower.has(columnName.toLowerCase())) {
        columnsByLower.set(columnName.toLowerCase(), columnName);
      }
    }

    this.channelMatchTableMeta = {
      tableName,
      tableRef: this.tableRefForName(tableName),
      columnsByLower
    };
    return this.channelMatchTableMeta;
  }

  private async getSearchLinkTableMeta(forceRefresh = false): Promise<SearchLinkTableMeta> {
    if (!forceRefresh && this.searchLinkTableMeta) {
      return this.searchLinkTableMeta;
    }

    const tableRows = await this.searchLinkRepo.query(`
      SELECT TABLE_NAME
      FROM USER_TABLES
      WHERE LOWER(TABLE_NAME) = 'search_links'
      ORDER BY CASE
        WHEN TABLE_NAME = 'search_links' THEN 0
        WHEN TABLE_NAME = 'SEARCH_LINKS' THEN 1
        ELSE 2
      END
    `);
    if (!tableRows?.length) {
      throw new Error('search_links table not found in current Oracle schema');
    }
    const tableName = String(tableRows[0]?.TABLE_NAME || tableRows[0]?.table_name || '').trim();
    if (!tableName) {
      throw new Error('search_links table name resolution failed');
    }
    const safeTableName = tableName.replace(/'/g, "''");
    const columnRows = await this.searchLinkRepo.query(
      `SELECT COLUMN_NAME FROM USER_TAB_COLS WHERE TABLE_NAME = '${safeTableName}'`
    );
    const columnsByLower = new Map<string, string>();
    for (const row of columnRows || []) {
      const columnName = String(row?.COLUMN_NAME || row?.column_name || '').trim();
      if (!columnName) continue;
      if (!columnsByLower.has(columnName.toLowerCase())) {
        columnsByLower.set(columnName.toLowerCase(), columnName);
      }
    }

    this.searchLinkTableMeta = {
      tableName,
      tableRef: this.tableRefForName(tableName),
      columnsByLower
    };
    return this.searchLinkTableMeta;
  }

  private resolveTableColumn(meta: ChannelMatchTableMeta, logicalName: string): string | undefined {
    return meta.columnsByLower.get(logicalName.toLowerCase());
  }

  private resolveSearchLinkTableColumn(meta: SearchLinkTableMeta, logicalName: string): string | undefined {
    return meta.columnsByLower.get(logicalName.toLowerCase());
  }

  private isBotUsernameLink(link: string): boolean {
    const match = link.match(/^https:\/\/t\.me\/([A-Za-z0-9_]+)(?:[/?#].*)?$/i);
    return Boolean(match?.[1] && /bot$/i.test(match[1]));
  }

  private toClientLinkFromParsedTarget(parsed: ParsedTargetLink): string | null {
    switch (parsed.kind) {
      case 'bot':
      case 'invite':
      case 'public_chat':
      case 'private_chat':
        return parsed.canonical;
      case 'private_message':
        return parsed.privateChannelId ? `https://t.me/c/${parsed.privateChannelId}` : null;
      default:
        return null;
    }
  }

  private buildClientLinksFromStoredRow(row: {
    channelType?: string;
    channelLink?: string;
    messageLink?: string;
    discoveredViaLink?: string;
    relatedLinks?: string[];
  }): string[] {
    const out = new Set<string>();
    const parsedMessage = row.messageLink ? this.parseTargetLink(row.messageLink) : null;
    const isPrivateMessage = parsedMessage?.kind === 'private_message';

    if ((row.channelType || '').toLowerCase() === 'bot') {
      const parsedChannel = row.channelLink ? this.parseTargetLink(row.channelLink) : null;
      const botRoot = parsedChannel && parsedChannel.kind === 'bot'
        ? parsedChannel.canonical
        : null;
      if (botRoot) out.add(botRoot);
    } else if (row.messageLink) {
      out.add(row.messageLink);
      if (isPrivateMessage && parsedMessage.privateChannelId) {
        out.add(`https://t.me/c/${parsedMessage.privateChannelId}`);
      }
    }

    if (isPrivateMessage) {
      const parsedVia = row.discoveredViaLink ? this.parseTargetLink(row.discoveredViaLink) : null;
      if (parsedVia) {
        const viaLink = this.toClientLinkFromParsedTarget(parsedVia);
        if (viaLink) out.add(viaLink);
      }
    }

    for (const related of row.relatedLinks || []) {
      const parsedRelated = this.parseTargetLink(related);
      if (parsedRelated) {
        const relatedLink = this.toClientLinkFromParsedTarget(parsedRelated);
        if (relatedLink) out.add(relatedLink);
      }
    }
    return Array.from(out);
  }

  private buildChannelMatchValues(
    meta: ChannelMatchTableMeta,
    mode: 'extended' | 'legacy',
    row: {
      searchId?: number;
      searchRef: string;
      channelType?: string;
      channelLink?: string;
      messageLink?: string;
      messageId: number;
      messageDate?: number;
      matchReason?: 'keyword_hyperlink' | 'keyword_video' | 'keyword_metadata';
      iterationNo?: number;
      discoveredViaLink?: string;
      discoveredFromMessageLink?: string;
      discoveredFromChannel?: string;
      messageText: string;
      relatedLinks: string[];
      mediaMetadata?: { [key: string]: unknown };
      metadataQuery?: string;
    },
    channel: string
  ): Record<string, unknown> | null {
    const colChannel = this.resolveTableColumn(meta, 'channel');
    const colMessageId = this.resolveTableColumn(meta, 'message_id');
    const colSearchId = this.resolveTableColumn(meta, 'search_id');
    const colSearchRef = this.resolveTableColumn(meta, 'search_ref');
    const colChannelLink = this.resolveTableColumn(meta, 'channel_link');
    const colMessageLink = this.resolveTableColumn(meta, 'message_link');
    const colChannelType = this.resolveTableColumn(meta, 'channel_type');
    const colDate = this.resolveTableColumn(meta, 'date');
    const colText = this.resolveTableColumn(meta, 'text');
    const colLinks = this.resolveTableColumn(meta, 'links');
    const colMatchReason = this.resolveTableColumn(meta, 'match_reason');
    const colIterationNo = this.resolveTableColumn(meta, 'iteration_no');
    const colDiscoveredViaLink = this.resolveTableColumn(meta, 'discovered_via_link');
    const colDiscoveredFromMessageLink = this.resolveTableColumn(meta, 'discovered_from_message_link');
    const colDiscoveredFromChannel = this.resolveTableColumn(meta, 'discovered_from_channel');
    const colMediaMetadata = this.resolveTableColumn(meta, 'media_metadata');
    const colMetadataQuery = this.resolveTableColumn(meta, 'metadata_query');
    if (!colChannel || !colMessageId) return null;

    const values: Record<string, unknown> = {
      [colChannel]: channel,
      [colMessageId]: row.messageId
    };
    if (colSearchId) {
      values[colSearchId] = typeof row.searchId === 'number' ? row.searchId : null;
    }
    if (colChannelType) {
      values[colChannelType] = row.channelType || null;
    }
    if (colChannelLink) {
      values[colChannelLink] = row.channelLink || null;
    }
    if (colMessageLink) {
      values[colMessageLink] = row.messageLink || null;
    }
    if (mode === 'extended' && colSearchRef) {
      values[colSearchRef] = row.searchRef;
    }
    if (colDate) values[colDate] = row.messageDate ? new Date(row.messageDate) : new Date();
    if (colText) values[colText] = row.messageText;
    if (colMatchReason) values[colMatchReason] = row.matchReason || null;
    if (colIterationNo) values[colIterationNo] = Number.isFinite(row.iterationNo) ? row.iterationNo : null;
    if (colDiscoveredViaLink) values[colDiscoveredViaLink] = row.discoveredViaLink || null;
    if (colDiscoveredFromMessageLink) values[colDiscoveredFromMessageLink] = row.discoveredFromMessageLink || null;
    if (colDiscoveredFromChannel) values[colDiscoveredFromChannel] = row.discoveredFromChannel || null;
    if (colMediaMetadata) values[colMediaMetadata] = row.mediaMetadata ? JSON.stringify(row.mediaMetadata) : null;
    if (colMetadataQuery) values[colMetadataQuery] = row.metadataQuery || null;
    if (colLinks) {
      const includeMessageLinkInLinks = !colMessageLink;
      const payload = mode === 'extended'
        ? JSON.stringify(row.relatedLinks || [])
        : JSON.stringify([
            ...(includeMessageLinkInLinks && row.messageLink ? [row.messageLink] : []),
            ...(row.relatedLinks || [])
          ]);
      values[colLinks] = this.truncateVarchar(payload, 4000);
    }

    return values;
  }

  private async insertChannelMatchRow(meta: ChannelMatchTableMeta, values: Record<string, unknown>): Promise<void> {
    const columns = Object.keys(values);
    const sql = `INSERT INTO ${meta.tableRef} (${columns.map((c) => this.quoteIdentifier(c)).join(', ')})
      VALUES (${columns.map((_, i) => `:${i + 1}`).join(', ')})`;
    const binds = columns.map((column) => values[column]);
    await this.channelRepo.query(sql, binds);
  }

  private async updateChannelMatchRow(
    meta: ChannelMatchTableMeta,
    values: Record<string, unknown>,
    channelColumn: string,
    messageIdColumn: string,
    searchIdColumn?: string
  ): Promise<void> {
    const setColumns = Object.keys(values).filter(
      (column) => column !== channelColumn && column !== messageIdColumn
    );
    if (!setColumns.length) return;
    const setSql = setColumns.map((column, i) => `${this.quoteIdentifier(column)} = :${i + 1}`).join(', ');
    const whereChannelIdx = setColumns.length + 1;
    const whereMessageIdx = setColumns.length + 2;
    const hasSearchFilter = Boolean(searchIdColumn && Object.prototype.hasOwnProperty.call(values, searchIdColumn));
    const whereSearchIdx = setColumns.length + 3;
    const sql = `UPDATE ${meta.tableRef}
      SET ${setSql}
      WHERE ${this.quoteIdentifier(channelColumn)} = :${whereChannelIdx}
        AND ${this.quoteIdentifier(messageIdColumn)} = :${whereMessageIdx}
        ${hasSearchFilter ? `AND ${this.quoteIdentifier(searchIdColumn!)} ${values[searchIdColumn!] === null ? 'IS NULL' : `= :${whereSearchIdx}`}` : ''}`;
    const binds = [
      ...setColumns.map((column) => values[column]),
      values[channelColumn],
      values[messageIdColumn]
    ];
    if (hasSearchFilter && values[searchIdColumn!] !== null) {
      binds.push(values[searchIdColumn!]);
    }
    await this.channelRepo.query(sql, binds);
  }

  private async findChannelMatchId(
    meta: ChannelMatchTableMeta,
    channelColumn: string,
    messageIdColumn: string,
    channel: string,
    messageId: number,
    searchIdColumn?: string,
    searchId?: number | null
  ): Promise<number | null> {
    const idColumn = this.resolveTableColumn(meta, 'id');
    if (!idColumn) return null;

    let sql = `SELECT ${this.quoteIdentifier(idColumn)} AS ID
      FROM ${meta.tableRef}
      WHERE ${this.quoteIdentifier(channelColumn)} = :1
        AND ${this.quoteIdentifier(messageIdColumn)} = :2`;
    const binds: unknown[] = [channel, messageId];

    if (searchIdColumn) {
      if (typeof searchId === 'number') {
        sql += ` AND ${this.quoteIdentifier(searchIdColumn)} = :3`;
        binds.push(searchId);
      } else {
        sql += ` AND ${this.quoteIdentifier(searchIdColumn)} IS NULL`;
      }
    }

    const rows = await this.channelRepo.query(sql, binds);
    const first = rows?.[0];
    const value = first?.ID ?? first?.id;
    if (value === undefined || value === null) return null;
    const asNumber = Number(value);
    return Number.isFinite(asNumber) ? asNumber : null;
  }

  private async upsertSearchLinkRow(
    searchId: number,
    link: string,
    linkType: string,
    channelMatchId?: number | null
  ): Promise<void> {
    const existing = await this.searchLinkRepo.findOne({
      where: { search_id: searchId, link } as any
    });

    if (existing) {
      const updatePayload: Partial<SearchLinkEntity> = { link_type: linkType };
      if (channelMatchId) updatePayload.channel_match_id = existing.channel_match_id ?? channelMatchId;
      await this.searchLinkRepo.update(existing.id, updatePayload as any);
      return;
    }

    await this.searchLinkRepo.save({
      search_id: searchId,
      link: this.truncateVarchar(link, 512),
      link_type: this.truncateVarchar(linkType, 32),
      channel_match_id: channelMatchId ?? null
    } as any);
  }

  async getSearchLinksBySearchId(searchId: number): Promise<string[]> {
    if (!Number.isFinite(searchId) || searchId <= 0) return [];
    try {
      const rows = await this.searchLinkRepo.find({
        where: { search_id: searchId } as any,
        order: { created_at: 'ASC', id: 'ASC' } as any
      });
      return rows
        .filter((row) => (row.link_type || '').toLowerCase() !== 'related')
        .map((row) => String(row.link || '').trim())
        .filter(Boolean);
    } catch {
      return [];
    }
  }

  async countSearchLinks(searchId: number): Promise<number> {
    if (!Number.isFinite(searchId) || searchId <= 0) return 0;
    try {
      const rows = await this.searchLinkRepo.find({ where: { search_id: searchId } as any });
      return rows.filter((row) => (row.link_type || '').toLowerCase() !== 'related').length;
    } catch {
      return 0;
    }
  }

  private async detectChannelMatchSchemaMode(): Promise<'extended' | 'legacy'> {
    this.channelMatchSchemaMode = 'extended';
    return 'extended';
  }

  async persistMatchesToOracle(searchRef: string): Promise<{ total: number; inserted: number; updated: number; failed: number }> {
    const rows = await this.redisStore.getAllMatches(searchRef);
    let inserted = 0;
    let updated = 0;
    let failed = 0;
    const failureSamples: string[] = [];

    for (const row of rows) {
      const channel = this.truncateVarchar(row.channelLink || row.channelKey || '', 128);
      if (!channel || !row.messageId) {
        failed += 1;
        continue;
      }

      const searchIdFromRow = typeof row.searchId === 'number' && Number.isFinite(row.searchId)
        ? row.searchId
        : (/^\d+$/.test(row.searchRef || '') ? Number.parseInt(row.searchRef, 10) : undefined);

      const relatedLinks = row.relatedLinks || [];
      let matchId: number | null = null;

      try {
        const existing = await this.channelRepo.findOne({
          where: {
            search_id: searchIdFromRow ?? null,
            channel,
            message_id: row.messageId
          } as any
        });

        const payload = {
          search_id: searchIdFromRow ?? null,
          channel,
          channel_type: row.channelType || null,
          channel_link: row.channelLink ? this.truncateVarchar(row.channelLink, 512) : null,
          message_link: row.messageLink ? this.truncateVarchar(row.messageLink, 512) : null,
          message_id: row.messageId,
          date: row.messageDate ? new Date(row.messageDate) : new Date(),
          match_reason: row.matchReason || null,
          iteration_no: Number.isFinite(row.iterationNo) ? row.iterationNo : null,
          discovered_via_link: row.discoveredViaLink ? this.truncateVarchar(row.discoveredViaLink, 512) : null,
          discovered_from_message_link: row.discoveredFromMessageLink
            ? this.truncateVarchar(row.discoveredFromMessageLink, 512)
            : null,
          discovered_from_channel: row.discoveredFromChannel
            ? this.truncateVarchar(row.discoveredFromChannel, 128)
            : null,
          text: row.messageText || null,
          links: this.truncateVarchar(JSON.stringify(relatedLinks), 4000),
          media_metadata: row.mediaMetadata ? JSON.stringify(row.mediaMetadata) : null,
          metadata_query: row.metadataQuery ? this.truncateVarchar(row.metadataQuery, 256) : null
        };

        if (existing) {
          await this.channelRepo.update(existing.id, payload as any);
          matchId = existing.id;
          updated += 1;
        } else {
          const saved = await this.channelRepo.save(payload as any) as ChannelMatchEntity;
          matchId = saved.id;
          inserted += 1;
        }

        if (typeof searchIdFromRow === 'number' && searchIdFromRow > 0) {
          const clientLinks = this.buildClientLinksFromStoredRow({
            channelType: row.channelType,
            channelLink: row.channelLink,
            messageLink: row.messageLink,
            discoveredViaLink: row.discoveredViaLink,
            relatedLinks
          });
          for (const link of clientLinks) {
            await this.upsertSearchLinkRow(
              searchIdFromRow,
              link,
              this.classifyClientLinkType(link),
              matchId
            );
          }
        }
      } catch (err) {
        failed += 1;
        const message = err instanceof Error ? err.message : String(err);
        if (failureSamples.length < 5) failureSamples.push(message);
      }
    }

    if (failed > 0 && failureSamples.length) {
      this.logger.warn('Channel match row persistence failures', {
        searchRef,
        failed,
        samples: failureSamples
      });
    }

    return { total: rows.length, inserted, updated, failed };
  }

  async crawlKeywordIterative(
    keyword: string,
    searchId: number,
    maxIterationsArg?: number,
    searchRefArg?: string,
    options?: CrawlRunOptions
  ): Promise<KeywordCrawlResultDto> {
    const abortSignal = options?.abortSignal;
    this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
    await this.initUserClient();
    this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
    const searchRef = searchRefArg || (searchId > 0 ? String(searchId) : `tmp_${Date.now()}`);

    const keywordTerms = this.splitKeywordTerms(keyword);
    if (!keywordTerms.length) {
      return {
        seedPublics: [],
        links: [],
        clientLinks: [],
        invites: [],
        chatsProcessed: 0,
        messagesStored: 0,
        iterations: 0
      };
    }

    try {
      await this.redisStore.upsertSearchRun(searchRef, keyword);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.warn('Redis store upsertSearchRun failed', { searchRef, error: message });
    }

    const seedLimit = Number(this.config.get<number>('tgDynamicChatLimit') || 200);
    const pageSize = Math.max(1, Number(this.config.get<number>('crawlSearchPageSize') || 20));
    const maxPagesPerChat = Math.max(
      1,
      Number(this.config.get<number>('crawlSearchPagesPerChat') || 10)
    );
    const maxChatsPerIteration = Math.max(
      5,
      Number(this.config.get<number>('crawlMaxChatsPerIteration') || 15)
    );
    const maxLinksPerIteration = Math.max(
      10,
      Number(this.config.get<number>('crawlMaxLinksPerIteration') || 80)
    );
    const maxStored = Math.max(1, Number(this.config.get<number>('crawlMsgLimit') || 1000));
    const maxRuntimeMs = Math.max(60000, Number(this.config.get<number>('crawlMaxRuntimeMs') || 300000));
    const startedAt = Date.now();
    const deadlineAt = startedAt + maxRuntimeMs;
    const autoJoinInvites = this.config.get<boolean>('crawlAutoJoin') !== false;
    const joinPublicChannels = this.config.get<boolean>('crawlJoinPublic') === true;
    const startBots = this.config.get<boolean>('crawlStartBots') === true;
    const leaveJoinedPrivate = this.config.get<boolean>('crawlLeaveJoinedPrivate') !== false;
    const allowVideoCaptionWithoutLink = this.config.get<boolean>('crawlAllowVideoCaptionWithoutLink') !== false;
    const metadataExpansion = this.config.get<boolean>('crawlMetadataExpansion') !== false;
    const maxMetadataQueries = Math.max(0, Number(this.config.get<number>('crawlMetadataMaxQueries') || 25));
    const botBioLinksCache = new Map<string, Promise<string[]>>();
    const resolvedTargetCache = new Map<string, Promise<Api.TypeChat | null>>();
    const getCachedBotBioLinks = (username: string): Promise<string[]> => {
      const key = username.toLowerCase();
      const existing = botBioLinksCache.get(key);
      if (existing) return existing;
      const promise = this.extractBotBioLinks(username, searchId, abortSignal);
      botBioLinksCache.set(key, promise);
      return promise;
    };
    const resolveCachedTarget = (parsed: ParsedTargetLink): Promise<Api.TypeChat | null> => {
      const key = parsed.canonical;
      const existing = resolvedTargetCache.get(key);
      if (existing) return existing;
      const promise = this.resolveParsedLinkForFrontier({
        parsed,
        autoJoinInvites,
        keywordTerms,
        searchId,
        joinedPrivateChats,
        abortSignal
      });
      resolvedTargetCache.set(key, promise);
      return promise;
    };
    const maxIterations = Math.max(
      1,
      Number(
        maxIterationsArg
        ?? this.config.get<number>('crawlIterations')
        ?? this.config.get<number>('crawlDepth')
        ?? 5
      )
    );

    const seedChats = new Map<string, Api.TypeChat>();
    await this.logStep(searchId, 'iterative_seed_start', { keyword, maxIterations });

    try {
      const byName = await this.invokeWithTimeout(
        this.client.invoke(
          new Api.contacts.Search({ q: keyword, limit: seedLimit })
        ),
        20000,
        'iterative_seed_contacts_search',
        abortSignal
      );
      for (const chat of byName.chats || []) {
        const key = this.getChatKey(chat);
        if (key) seedChats.set(key, chat);
      }
      for (const user of (byName as any).users || []) {
        const key = this.getChatKey(user as Api.TypeChat);
        if (key) seedChats.set(key, user as Api.TypeChat);
      }
      await this.logStep(searchId, 'iterative_seed_contacts', { count: seedChats.size });
    } catch (err) {
      this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
      const message = err instanceof Error ? err.message : String(err);
      await this.logStep(searchId, 'iterative_seed_contacts_error', { error: message });
    }

    try {
      const global = await this.invokeWithTimeout(
        this.client.invoke(
          new Api.messages.SearchGlobal({
            q: keyword,
            offsetDate: 0 as any,
            offsetPeer: new Api.InputPeerEmpty(),
            offsetId: 0 as any,
            limit: seedLimit,
            filter: new Api.InputMessagesFilterEmpty()
          } as any)
        ),
        20000,
        'iterative_seed_global_search',
        abortSignal
      );
      const globalAny = global as any;
      for (const chat of globalAny.chats || []) {
        const key = this.getChatKey(chat);
        if (key) seedChats.set(key, chat);
      }
      for (const user of globalAny.users || []) {
        const key = this.getChatKey(user as Api.TypeChat);
        if (key) seedChats.set(key, user as Api.TypeChat);
      }
      await this.logStep(searchId, 'iterative_seed_global', { count: seedChats.size });
    } catch (err) {
      this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
      const message = err instanceof Error ? err.message : String(err);
      await this.logStep(searchId, 'iterative_seed_global_error', { error: message });
    }

    const seedPublics: string[] = [];
    const discoveredLinks = new Set<string>();
    const clientLinks = new Set<string>();
    const discoveredInvites = new Set<string>();
    const metadataQueries = new Set<string>();
    const processedMetadataQueries = new Set<string>();
    for (const chat of seedChats.values()) {
      const username = 'username' in chat ? chat.username : undefined;
      if (!username) continue;
      const link = this.normalizeLink(`https://t.me/${username}`);
      seedPublics.push(link);
      discoveredLinks.add(link);
    }
    await this.logStep(searchId, 'iterative_seed_publics_ready', { count: seedPublics.length });

    if (!seedChats.size) {
      await this.logStep(searchId, 'iterative_seed_empty');
      try {
        await this.redisStore.completeSearchRun(searchRef, {
          keyword,
          iterations: 0,
          chatsProcessed: 0,
          messagesStored: 0,
          links: 0,
          invites: 0,
          clientLinks: 0
        });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        this.logger.warn('Redis store completeSearchRun failed', { searchRef, error: message });
      }
      return {
        seedPublics: [],
        links: [],
        clientLinks: [],
        invites: [],
        chatsProcessed: 0,
        messagesStored: 0,
        iterations: 0
      };
    }

    const visitedChats = new Set<string>();
    const processedLinks = new Set<string>();
    const joinedPrivateChats = new Map<string, Api.TypeChat>();
    const startedBots = new Set<string>();
    let frontier: FrontierChatState[] = Array.from(seedChats.values())
      .map((chat) => ({ chat }))
      .sort((a, b) => this.getChatKey(a.chat).localeCompare(this.getChatKey(b.chat)));
    let chatsProcessed = 0;
    let messagesStored = 0;
    let iterations = 0;

    while (frontier.length && iterations < maxIterations && messagesStored < maxStored) {
      this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
      if (Date.now() > deadlineAt) {
        await this.logStep(searchId, 'iterative_runtime_timeout', {
          maxRuntimeMs,
          elapsedMs: Date.now() - startedAt,
          iterations,
          chatsProcessed,
          messagesStored
        });
        break;
      }
      iterations += 1;
      await this.logStep(searchId, 'iterative_round_start', {
        iteration: iterations,
        frontier: frontier.length,
        stored: messagesStored,
        links: discoveredLinks.size
      });

      const linksFromIteration = new Map<string, {
        discoveredFromMessageLink?: string;
        discoveredFromChannel?: string;
      }>();

      let chatsThisIteration = 0;
      for (const frontierItem of frontier) {
        this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
        if (chatsThisIteration >= maxChatsPerIteration) {
          await this.logStep(searchId, 'iterative_chats_cap_reached', {
            iteration: iterations,
            chatsThisIteration,
            maxChatsPerIteration,
            remainingInFrontier: frontier.length - chatsThisIteration
          });
          break;
        }
        const chat = frontierItem.chat;
        if (Date.now() > deadlineAt) {
          await this.logStep(searchId, 'iterative_runtime_timeout_in_round', {
            maxRuntimeMs,
            elapsedMs: Date.now() - startedAt,
            iteration: iterations,
            chatsProcessed,
            messagesStored
          });
          break;
        }
        if (messagesStored >= maxStored) break;

        const chatKey = this.getChatKey(chat);
        const chatUsername = this.getChatUsername(chat);
        const chatChannelLink = this.buildChannelLink(chat);
        const selfPublicLink = chatUsername
          ? this.canonicalizeTelegramLink(`https://t.me/${chatUsername}`)
          : null;
        const canJoinChannel = String((chat as any)?._ || '').toLowerCase().includes('channel');
        if (!chatKey || visitedChats.has(chatKey)) continue;
        visitedChats.add(chatKey);
        chatsProcessed += 1;
        chatsThisIteration += 1;

        await this.logStep(searchId, 'iterative_chat_start', {
          iteration: iterations,
          chat: chatKey
        });

        let joinRetriedAfterAccessError = false;
        if (canJoinChannel && joinPublicChannels) {
          await this.tryJoinChat(chat, searchId, 'iterative_chat_prejoin', abortSignal);
        }

        if (startBots && chatUsername && this.classifyChatType(chat) === 'bot') {
          const botKey = chatUsername.toLowerCase();
          if (!startedBots.has(botKey)) {
            startedBots.add(botKey);
            await this.startBotIfNeeded(chatUsername, undefined, searchId, abortSignal);
          }
        }

        let inputPeer: Api.TypeInputPeer;
        try {
          inputPeer = await this.getInputPeerForChat(chat, chatKey, 'iterative_chat_input_peer', abortSignal);
        } catch (err) {
          this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
          const message = err instanceof Error ? err.message : String(err);
          await this.logStep(searchId, 'iterative_chat_input_peer_error', {
            chat: chatKey,
            error: message
          });
          continue;
        }

        let offsetId = 0;
        let pages = 0;
        let previousSig = '';

        while (pages < maxPagesPerChat && messagesStored < maxStored) {
          this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
          if (Date.now() > deadlineAt) {
            await this.logStep(searchId, 'iterative_runtime_timeout_in_chat', {
              maxRuntimeMs,
              elapsedMs: Date.now() - startedAt,
              iteration: iterations,
              chat: chatKey,
              pages,
              messagesStored
            });
            break;
          }
          await this.logStep(searchId, 'iterative_chat_page_request', {
            iteration: iterations,
            chat: chatKey,
            offsetId,
            pages
          });
          let msgs: Api.Message[] = [];
          try {
            msgs = await this.fetchHistoryPage(
              inputPeer,
              offsetId,
              pageSize,
              15000,
              abortSignal,
              chatKey + ':' + offsetId + ':' + pageSize
            );
          } catch (err) {
            this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
            const message = err instanceof Error ? err.message : String(err);
            if (
              this.isAccessDeniedError(message)
              && canJoinChannel
              && joinPublicChannels
              && !joinRetriedAfterAccessError
            ) {
              joinRetriedAfterAccessError = true;
              const joined = await this.tryJoinChat(chat, searchId, 'iterative_chat_access', abortSignal);
              if (joined) {
                await this.logStep(searchId, 'iterative_chat_retry_after_join', {
                  chat: chatKey,
                  iteration: iterations,
                  offsetId,
                  pages
                });
                try {
                  this.inputPeerCache.delete(chatKey);
                  inputPeer = await this.getInputPeerForChat(chat, chatKey, 'iterative_chat_input_peer_retry', abortSignal);
                } catch (retryErr) {
                  this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
                  const retryMessage = retryErr instanceof Error ? retryErr.message : String(retryErr);
                  await this.logStep(searchId, 'iterative_chat_input_peer_retry_error', {
                    chat: chatKey,
                    error: retryMessage
                  });
                  break;
                }
                continue;
              }
            }
            await this.logStep(searchId, 'iterative_chat_page_error', {
              chat: chatKey,
              offsetId,
              error: message
            });
            break;
          }

          if (!msgs.length) break;

          const firstId = msgs[0].id;
          const lastId = msgs[msgs.length - 1].id;
          const sig = `${firstId}:${lastId}:${msgs.length}`;
          if (sig === previousSig || lastId === offsetId) {
            await this.logStep(searchId, 'iterative_chat_page_stalled', { chat: chatKey, sig });
            break;
          }
          previousSig = sig;

          for (const msg of msgs) {
            const msgAny = msg as any;
            const text = String(msgAny.message || '');
            const mediaMetadata = this.extractMediaMetadata(msgAny);
            const metadataText = this.mediaMetadataToText(mediaMetadata);
            const textKeywordMatch = this.messageContainsKeyword(text, keywordTerms);
            const metadataKeywordMatch = metadataExpansion && this.messageContainsKeyword(metadataText, keywordTerms);
            const messageKeywordMatch = textKeywordMatch || metadataKeywordMatch;
            if (!messageKeywordMatch) continue;

            const chatType = this.classifyChatType(chat);
            const messageLink = this.buildMessageLink(chat, msgAny.id);
            const links = this.collectTelegramLinks(
              text,
              msgAny.entities || [],
              msgAny.replyMarkup
            );

            const filteredLinks = links.filter((normalized) =>
              !selfPublicLink || normalized !== selfPublicLink
            );
            const videoCaptionMatch = allowVideoCaptionWithoutLink && this.isVideoOrClipMessage(msgAny);
            const metadataMediaMatch = metadataKeywordMatch && Boolean(mediaMetadata);
            if (!filteredLinks.length && !videoCaptionMatch && !metadataMediaMatch) continue;

            if (filteredLinks.length) {
              for (const normalized of filteredLinks) {
                discoveredLinks.add(normalized);
                if (!linksFromIteration.has(normalized)) {
                  linksFromIteration.set(normalized, {
                    discoveredFromMessageLink: messageLink,
                    discoveredFromChannel: chatChannelLink || chatKey
                  });
                }
                if (this.isInviteLink(normalized)) discoveredInvites.add(normalized);
              }
            }

            if (metadataExpansion && mediaMetadata && metadataQueries.size < maxMetadataQueries) {
              for (const query of this.buildMetadataQueries(mediaMetadata)) {
                if (metadataQueries.size >= maxMetadataQueries) break;
                if (!this.messageContainsKeyword(query, keywordTerms)) metadataQueries.add(query);
              }
            }

            const matchReason: 'keyword_hyperlink' | 'keyword_video' | 'keyword_metadata' = filteredLinks.length
              ? 'keyword_hyperlink'
              : metadataMediaMatch
                ? 'keyword_metadata'
                : 'keyword_video';

            try {
              await this.redisStore.upsertMatch({
                searchRef,
                searchId: searchId > 0 ? searchId : undefined,
                keyword,
                channelKey: chatKey,
                channelType: chatType,
                channelLink: chatChannelLink,
                messageId: msgAny.id,
                messageLink,
                messageText: text,
                messageDate: Number(msgAny.date || 0) * 1000,
                matchReason,
                iterationNo: iterations,
                discoveredViaLink: frontierItem.discoveredViaLink,
                discoveredFromMessageLink: frontierItem.discoveredFromMessageLink,
                discoveredFromChannel: frontierItem.discoveredFromChannel,
                relatedLinks: filteredLinks,
                mediaMetadata,
                metadataQuery: metadataKeywordMatch ? keyword : undefined
              });
            } catch (err) {
              const message = err instanceof Error ? err.message : String(err);
              this.logger.warn('Redis store upsertMatch failed', {
                searchRef,
                chat: chatKey,
                messageId: msgAny.id,
                error: message
              });
            }

            if (chatType === 'bot') {
              if (chatChannelLink) clientLinks.add(chatChannelLink);
            } else if (messageLink) {
              clientLinks.add(messageLink);
            }

            messagesStored += 1;
            if (messagesStored >= maxStored) break;
          }

          pages += 1;
          offsetId = lastId;
        }

        let bioLinksPromise = this.chatBioLinksCache.get(chatKey);
        if (!bioLinksPromise) {
          bioLinksPromise = this.extractChatBioLinks(chat, searchId, abortSignal);
          this.chatBioLinksCache.set(chatKey, bioLinksPromise);
        }
        const bioLinks = await bioLinksPromise;
        for (const bioLink of bioLinks) {
          if (selfPublicLink && bioLink === selfPublicLink) continue;
          discoveredLinks.add(bioLink);
          if (!linksFromIteration.has(bioLink)) {
            linksFromIteration.set(bioLink, {
              discoveredFromMessageLink: frontierItem.discoveredFromMessageLink,
              discoveredFromChannel: chatChannelLink || chatKey
            });
          }
          if (this.isInviteLink(bioLink)) discoveredInvites.add(bioLink);
        }

        await this.logStep(searchId, 'iterative_chat_done', {
          iteration: iterations,
          chat: chatKey,
          pages,
          stored: messagesStored
        });
      }

      const nextFrontierMap = new Map<string, FrontierChatState>();
      let linksProcessedThisIteration = 0;
      for (const [link, origin] of linksFromIteration.entries()) {
        this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
        if (Date.now() > deadlineAt) {
          await this.logStep(searchId, 'iterative_runtime_timeout_in_links', {
            maxRuntimeMs,
            elapsedMs: Date.now() - startedAt,
            iteration: iterations,
            linksProcessedThisIteration,
            linksPending: linksFromIteration.size - linksProcessedThisIteration
          });
          break;
        }
        if (linksProcessedThisIteration >= maxLinksPerIteration) {
          await this.logStep(searchId, 'iterative_links_cap_reached', {
            iteration: iterations,
            linksProcessedThisIteration,
            maxLinksPerIteration,
            linksPending: linksFromIteration.size - linksProcessedThisIteration
          });
          break;
        }
        linksProcessedThisIteration += 1;
        if (processedLinks.has(link)) continue;
        processedLinks.add(link);
        const parsed = this.parseTargetLink(link);
        if (!parsed) continue;

        if (parsed.kind === 'bot') {
          clientLinks.add(parsed.canonical);
          if (startBots && parsed.username) {
            const botKey = parsed.username.toLowerCase();
            if (!startedBots.has(botKey)) {
              startedBots.add(botKey);
              await this.startBotIfNeeded(parsed.username, parsed.botStartParam, searchId, abortSignal);
            }
          }
          if (parsed.username) {
            const botBioLinks = await getCachedBotBioLinks(parsed.username);
            for (const botBioLink of botBioLinks) {
              this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
              if (Date.now() > deadlineAt) break;
              if (processedLinks.has(botBioLink)) continue;
              processedLinks.add(botBioLink);
              discoveredLinks.add(botBioLink);
              clientLinks.add(botBioLink);
              if (this.isInviteLink(botBioLink)) discoveredInvites.add(botBioLink);

              const parsedBio = this.parseTargetLink(botBioLink);
              if (!parsedBio) continue;
              if (parsedBio.kind === 'bot') {
                clientLinks.add(parsedBio.canonical);
                continue;
              }
              const resolvedBio = await resolveCachedTarget(parsedBio);
              if (!resolvedBio) continue;

              const bioKey = this.getChatKey(resolvedBio);
              if (!bioKey || visitedChats.has(bioKey) || nextFrontierMap.has(bioKey)) continue;
              nextFrontierMap.set(bioKey, {
                chat: resolvedBio,
                discoveredViaLink: parsedBio.canonical,
                discoveredFromMessageLink: origin.discoveredFromMessageLink,
                discoveredFromChannel: origin.discoveredFromChannel
              });
            }
          }
          continue;
        }

        const resolved = await resolveCachedTarget(parsed);
        if (!resolved) continue;

        const key = this.getChatKey(resolved);
        if (!key || visitedChats.has(key) || nextFrontierMap.has(key)) continue;
        nextFrontierMap.set(key, {
          chat: resolved,
          discoveredViaLink: parsed.canonical,
          discoveredFromMessageLink: origin.discoveredFromMessageLink,
          discoveredFromChannel: origin.discoveredFromChannel
        });
      }

      if (metadataExpansion && metadataQueries.size) {
        for (const metadataQuery of metadataQueries) {
          this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
          if (Date.now() > deadlineAt) break;
          if (processedMetadataQueries.has(metadataQuery)) continue;
          processedMetadataQueries.add(metadataQuery);

          const addMetadataSeed = (chat: Api.TypeChat) => {
            const key = this.getChatKey(chat);
            if (!key || visitedChats.has(key) || nextFrontierMap.has(key)) return;
            nextFrontierMap.set(key, {
              chat,
              discoveredViaLink: 'metadata:' + metadataQuery,
              discoveredFromChannel: 'metadata'
            });
          };

          try {
            const byName = await this.invokeWithTimeout(
              this.client.invoke(new Api.contacts.Search({ q: metadataQuery, limit: seedLimit })),
              20000,
              'metadata_seed_contacts_search',
              abortSignal
            );
            for (const chat of byName.chats || []) addMetadataSeed(chat);
            for (const user of (byName as any).users || []) addMetadataSeed(user as Api.TypeChat);
          } catch (err) {
            this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
            const message = err instanceof Error ? err.message : String(err);
            await this.logStep(searchId, 'metadata_seed_contacts_error', { query: metadataQuery, error: message });
          }

          try {
            const global = await this.invokeWithTimeout(
              this.client.invoke(
                new Api.messages.SearchGlobal({
                  q: metadataQuery,
                  offsetDate: 0 as any,
                  offsetPeer: new Api.InputPeerEmpty(),
                  offsetId: 0 as any,
                  limit: seedLimit,
                  filter: new Api.InputMessagesFilterEmpty()
                } as any)
              ),
              20000,
              'metadata_seed_global_search',
              abortSignal
            );
            const globalAny = global as any;
            for (const chat of globalAny.chats || []) addMetadataSeed(chat);
            for (const user of globalAny.users || []) addMetadataSeed(user as Api.TypeChat);
          } catch (err) {
            this.throwIfAborted(abortSignal, 'crawlKeywordIterative');
            const message = err instanceof Error ? err.message : String(err);
            await this.logStep(searchId, 'metadata_seed_global_error', { query: metadataQuery, error: message });
          }

          await this.logStep(searchId, 'metadata_seed_done', {
            query: metadataQuery,
            nextFrontier: nextFrontierMap.size
          });
        }
      }

      frontier = Array.from(nextFrontierMap.values()).sort((a, b) =>
        this.getChatKey(a.chat).localeCompare(this.getChatKey(b.chat))
      );

      await this.logStep(searchId, 'iterative_round_done', {
        iteration: iterations,
        chatsProcessed,
        messagesStored,
        links: discoveredLinks.size,
        invites: discoveredInvites.size,
        nextFrontier: frontier.length
      });
    }

    await this.logStep(searchId, 'iterative_done', {
      chatsProcessed,
      messagesStored,
      iterations,
      links: discoveredLinks.size,
      invites: discoveredInvites.size
    });

    let clientLinksOut = Array.from(clientLinks);
    try {
      const fromStore = await this.redisStore.getClientLinks(searchRef);
      if (fromStore.length) clientLinksOut = fromStore;
      await this.redisStore.completeSearchRun(searchRef, {
        keyword,
        iterations,
        chatsProcessed,
        messagesStored,
        links: discoveredLinks.size,
        invites: discoveredInvites.size,
        clientLinks: clientLinksOut.length
      });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.warn('Redis store finalization failed', { searchRef, error: message });
    }

    if (leaveJoinedPrivate && joinedPrivateChats.size) {
      await this.leaveJoinedPrivateChats(Array.from(joinedPrivateChats.values()), searchId);
    }

    return {
      seedPublics: Array.from(new Set(seedPublics)),
      links: Array.from(discoveredLinks),
      clientLinks: clientLinksOut,
      invites: Array.from(discoveredInvites),
      chatsProcessed,
      messagesStored,
      iterations
    };
  }

}
