Total output lines: 2500

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

    const quoteMatc…12996 tokens truncated…let frontier: FrontierChatState[] = Array.from(seedChats.values())
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
