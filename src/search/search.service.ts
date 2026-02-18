import { Injectable } from '@nestjs/common';
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

@Injectable()
export class SearchService {
  private readonly client: TelegramClient;
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
    const normalizedTokens = this.tokenizeForSearch(text).filter(Boolean);
    if (!normalizedTokens.length) return false;
    const compactText = normalizedTokens.join('');
    const terms = keywordTerms.map((part) => this.compactForSearch(part)).filter(Boolean);
    if (!terms.length) return false;

    const fuzzyEnabled = this.config.get<boolean>('crawlFuzzyEnabled') !== false;
    const fuzzyMinLen = Math.max(1, Number(this.config.get<number>('crawlFuzzyMinTermLength') || 4));
    const fuzzyMaxDistanceCfg = Math.max(0, Number(this.config.get<number>('crawlFuzzyMaxDistance') || 1));

    return terms.every((part) => {
      if (compactText.includes(part)) return true;
      if (!fuzzyEnabled) return false;
      if (part.length < fuzzyMinLen) return false;
      if (fuzzyMaxDistanceCfg <= 0) return false;

      const adaptiveDistance = Math.max(1, Math.floor(part.length * 0.25));
      const maxDistance = Math.min(fuzzyMaxDistanceCfg, adaptiveDistance);
      if (maxDistance <= 0) return false;

      return this.fuzzyIncludes(normalizedTokens, part, maxDistance);
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

  private async joinInviteIfNeeded(link: string): Promise<Api.TypeChat | null> {
    const hash = this.parseInviteHash(link);
    if (!hash) return null;
    try {
      const res: any = await this.client.invoke(
        new Api.messages.ImportChatInvite({ hash } as any)
      );
      if (res?.chats?.length) return res.chats[0] as Api.TypeChat;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
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

  private async startBotIfNeeded(username: string, startParam: string | undefined, searchId: number): Promise<void> {
    const command = startParam ? `/start ${startParam}` : '/start';
    try {
      const peer = await this.invokeWithTimeout(
        this.client.getInputEntity(`@${username}`) as Promise<Api.TypeInputPeer>,
        15000,
        'iterative_bot_start_input'
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
        'iterative_bot_start_send'
      );
      await this.logStep(searchId, 'iterative_bot_started', {
        bot: username,
        startParam: startParam || null
      });
    } catch (err) {
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
    autoJoinInvites: boolean
  ): Promise<Api.TypeChat | null> {
    if (parsed.kind === 'invite') {
      if (!autoJoinInvites) return null;
      return this.joinInviteIfNeeded(parsed.canonical);
    }

    if (parsed.username) {
      try {
        return await this.client.getEntity(`@${parsed.username}`) as Api.TypeChat;
      } catch {
        return null;
      }
    }

    if ((parsed.kind === 'private_message' || parsed.kind === 'private_chat') && parsed.privateChannelId) {
      try {
        const fullId = BigInt(`-100${parsed.privateChannelId}`);
        return await this.client.getEntity(fullId as any) as Api.TypeChat;
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
    keywordTerms: string[]
  ): Promise<boolean> {
    if (!Number.isFinite(messageId) || messageId <= 0) return false;
    let inputPeer: Api.TypeInputPeer;
    try {
      inputPeer = await this.invokeWithTimeout(
        this.client.getInputEntity(chat) as Promise<Api.TypeInputPeer>,
        15000,
        'target_message_input_peer'
      );
    } catch {
      return false;
    }

    try {
      const page = await this.fetchHistoryPage(
        inputPeer,
        messageId + 1,
        5,
        15000
      );
      const exact = page.find((m) => Number((m as any)?.id || 0) === messageId);
      if (!exact) return false;
      const text = String((exact as any)?.message || '');
      return this.messageContainsKeyword(text, keywordTerms);
    } catch {
      return false;
    }
  }

  private async extractBotBioLinks(username: string, searchId: number): Promise<string[]> {
    try {
      const input = await this.invokeWithTimeout(
        this.client.getInputEntity(`@${username}`) as unknown as Promise<Api.TypeInputUser>,
        15000,
        'bot_bio_input_user'
      );
      const full = await this.invokeWithTimeout(
        this.client.invoke(
          new Api.users.GetFullUser({
            id: input as any
          } as any)
        ),
        15000,
        'bot_bio_get_full_user'
      );
      const about = String((full as any)?.fullUser?.about || '');
      if (!about.trim()) return [];
      const links = this.collectTelegramLinks(about, [], undefined);
      await this.logStep(searchId, 'iterative_bot_bio_links', { bot: username, links: links.length });
      return links;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await this.logStep(searchId, 'iterative_bot_bio_links_error', { bot: username, error: message });
      return [];
    }
  }

  private async extractChatBioLinks(chat: Api.TypeChat, searchId: number): Promise<string[]> {
    const chatAny = chat as any;
    const chatType = String(chatAny?._ || '').toLowerCase();
    const chatKey = this.getChatKey(chat);
    try {
      let about = '';
      if (chatType.includes('channel')) {
        const input = await this.invokeWithTimeout(
          this.client.getInputEntity(chat) as unknown as Promise<Api.TypeInputChannel>,
          15000,
          'chat_bio_input_channel'
        );
        const full = await this.invokeWithTimeout(
          this.client.invoke(
            new Api.channels.GetFullChannel({
              channel: input as any
            } as any)
          ),
          15000,
          'chat_bio_get_full_channel'
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
          'chat_bio_get_full_chat'
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
  }): Promise<Api.TypeChat | null> {
    const { parsed, autoJoinInvites, keywordTerms, searchId, joinedPrivateChats } = params;

    let resolved: Api.TypeChat | null = null;
    if (parsed.kind === 'invite') {
      if (!autoJoinInvites) return null;
      resolved = await this.joinInviteIfNeeded(parsed.canonical);
      if (resolved) {
        const joinedKey = this.getChatKey(resolved);
        if (joinedKey) joinedPrivateChats.set(joinedKey, resolved);
      }
    } else {
      resolved = await this.resolveChatFromParsedLink(parsed, autoJoinInvites);
    }
    if (!resolved) return null;

    if (parsed.messageId) {
      const targetHasKeyword = await this.targetMessageContainsKeyword(
        resolved,
        parsed.messageId,
        keywordTerms
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

  private async tryJoinChat(chat: Api.TypeChat, searchId: number, reason: string): Promise<boolean> {
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
        `${reason}_join_channel`
      );
      await this.logStep(searchId, 'iterative_chat_joined', { chat: chatKey, reason });
      return true;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      if (this.isAlreadyJoinedResult(message)) {
        await this.logStep(searchId, 'iterative_chat_join_ack', { chat: chatKey, reason, result: message });
        return true;
      }
      await this.logStep(searchId, 'iterative_chat_join_error', { chat: chatKey, reason, error: message });
      return false;
    }
  }

  private async fetchHistoryPage(
    peer: Api.TypeInputPeer,
    offsetId: number,
    limit: number,
    timeoutMs: number
  ): Promise<Api.Message[]> {
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
      'iterative_chat_history_page'
    );
    const histAny = hist as any;
    return (histAny.messages || []).filter((m: any) => Boolean(m?.message)) as Api.Message[];
  }

  private async invokeWithTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
    return Promise.race([
      promise,
      new Promise<T>((_, reject) =>
        setTimeout(() => reject(new Error(`${label} timeout after ${timeoutMs}ms`)), timeoutMs)
      )
    ]);
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

  private buildClientLinksFromStoredRow(row: {
    channelType?: string;
    channelLink?: string;
    messageLink?: string;
    discoveredViaLink?: string;
    relatedLinks?: string[];
  }): string[] {
    const out = new Set<string>();
    if ((row.channelType || '').toLowerCase() === 'bot') {
      if (row.channelLink) out.add(row.channelLink);
    } else if (row.messageLink) {
      out.add(row.messageLink);
    }

    const parsedVia = row.discoveredViaLink ? this.parseTargetLink(row.discoveredViaLink) : null;
    if (
      parsedVia
      && (parsedVia.kind === 'invite'
        || parsedVia.kind === 'public_chat'
        || parsedVia.kind === 'private_chat'
        || parsedVia.kind === 'bot')
    ) {
      out.add(parsedVia.canonical);
    }

    for (const related of row.relatedLinks || []) {
      const parsedRelated = this.parseTargetLink(related);
      if (parsedRelated?.kind === 'bot') {
        out.add(parsedRelated.canonical);
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
      matchReason?: 'keyword_hyperlink' | 'keyword_video';
      iterationNo?: number;
      discoveredViaLink?: string;
      discoveredFromMessageLink?: string;
      discoveredFromChannel?: string;
      messageText: string;
      relatedLinks: string[];
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
    const meta = await this.getSearchLinkTableMeta();
    const colSearchId = this.resolveSearchLinkTableColumn(meta, 'search_id');
    const colLink = this.resolveSearchLinkTableColumn(meta, 'link');
    const colType = this.resolveSearchLinkTableColumn(meta, 'link_type');
    const colChannelMatchId = this.resolveSearchLinkTableColumn(meta, 'channel_match_id');
    if (!colSearchId || !colLink || !colType) return;

    const insertValues: Record<string, unknown> = {
      [colSearchId]: searchId,
      [colLink]: link,
      [colType]: linkType
    };
    if (colChannelMatchId) {
      insertValues[colChannelMatchId] = channelMatchId ?? null;
    }

    const columns = Object.keys(insertValues);
    const insertSql = `INSERT INTO ${meta.tableRef} (${columns.map((c) => this.quoteIdentifier(c)).join(', ')})
      VALUES (${columns.map((_, i) => `:${i + 1}`).join(', ')})`;
    const insertBinds = columns.map((column) => insertValues[column]);

    try {
      await this.searchLinkRepo.query(insertSql, insertBinds);
      return;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      if (!this.isOracleUniqueViolation(message)) {
        throw err;
      }
    }

    const setColumns: string[] = [colType];
    if (colChannelMatchId && channelMatchId) {
      setColumns.push(colChannelMatchId);
    }

    const setParts = setColumns.map((column, idx) => {
      if (column === colChannelMatchId) {
        return `${this.quoteIdentifier(column)} = COALESCE(${this.quoteIdentifier(column)}, :${idx + 1})`;
      }
      return `${this.quoteIdentifier(column)} = :${idx + 1}`;
    });

    const updateSql = `UPDATE ${meta.tableRef}
      SET ${setParts.join(', ')}
      WHERE ${this.quoteIdentifier(colSearchId)} = :${setColumns.length + 1}
        AND ${this.quoteIdentifier(colLink)} = :${setColumns.length + 2}`;
    const updateBinds: unknown[] = [
      ...setColumns.map((column) => (column === colChannelMatchId ? channelMatchId : linkType)),
      searchId,
      link
    ];
    await this.searchLinkRepo.query(updateSql, updateBinds);
  }

  async getSearchLinksBySearchId(searchId: number): Promise<string[]> {
    if (!Number.isFinite(searchId) || searchId <= 0) return [];
    try {
      const meta = await this.getSearchLinkTableMeta();
      const colSearchId = this.resolveSearchLinkTableColumn(meta, 'search_id');
      const colLink = this.resolveSearchLinkTableColumn(meta, 'link');
      const colType = this.resolveSearchLinkTableColumn(meta, 'link_type');
      const colCreated = this.resolveSearchLinkTableColumn(meta, 'created_at');
      const colId = this.resolveSearchLinkTableColumn(meta, 'id');
      if (!colSearchId || !colLink) return [];

      const orderColumns = [colCreated, colId].filter(Boolean) as string[];
      const orderClause = orderColumns.length
        ? ` ORDER BY ${orderColumns.map((c) => this.quoteIdentifier(c)).join(', ')}`
        : '';
      const typeFilter = colType
        ? ` AND (${this.quoteIdentifier(colType)} IS NULL OR LOWER(${this.quoteIdentifier(colType)}) <> 'related')`
        : '';
      const sql = `SELECT ${this.quoteIdentifier(colLink)} AS LINK
        FROM ${meta.tableRef}
        WHERE ${this.quoteIdentifier(colSearchId)} = :1${typeFilter}${orderClause}`;
      const rows = await this.searchLinkRepo.query(sql, [searchId]);
      return rows
        .map((row: any) => String(row?.LINK ?? row?.link ?? '').trim())
        .filter(Boolean);
    } catch {
      return [];
    }
  }

  async countSearchLinks(searchId: number): Promise<number> {
    if (!Number.isFinite(searchId) || searchId <= 0) return 0;
    try {
      const meta = await this.getSearchLinkTableMeta();
      const colSearchId = this.resolveSearchLinkTableColumn(meta, 'search_id');
      const colType = this.resolveSearchLinkTableColumn(meta, 'link_type');
      if (!colSearchId) return 0;
      const typeFilter = colType
        ? ` AND (${this.quoteIdentifier(colType)} IS NULL OR LOWER(${this.quoteIdentifier(colType)}) <> 'related')`
        : '';
      const sql = `SELECT COUNT(*) AS CNT
        FROM ${meta.tableRef}
        WHERE ${this.quoteIdentifier(colSearchId)} = :1${typeFilter}`;
      const rows = await this.searchLinkRepo.query(sql, [searchId]);
      const countValue = rows?.[0]?.CNT ?? rows?.[0]?.cnt ?? 0;
      const count = Number(countValue);
      return Number.isFinite(count) ? count : 0;
    } catch {
      return 0;
    }
  }

  private async detectChannelMatchSchemaMode(): Promise<'extended' | 'legacy'> {
    if (this.channelMatchSchemaMode !== 'unknown') {
      return this.channelMatchSchemaMode;
    }

    try {
      const meta = await this.getChannelMatchTableMeta();
      const hasChannelLink = Boolean(this.resolveTableColumn(meta, 'channel_link'));
      const hasMessageLink = Boolean(this.resolveTableColumn(meta, 'message_link'));
      this.channelMatchSchemaMode = hasChannelLink && hasMessageLink
        ? 'extended'
        : 'legacy';
      return this.channelMatchSchemaMode;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.warn('Oracle schema detection failed; using legacy payload', { error: message });
      this.channelMatchSchemaMode = 'legacy';
      return this.channelMatchSchemaMode;
    }
  }

  async persistMatchesToOracle(searchRef: string): Promise<{ total: number; inserted: number; updated: number; failed: number }> {
    const rows = await this.redisStore.getAllMatches(searchRef);
    let mode = await this.detectChannelMatchSchemaMode();
    let meta: ChannelMatchTableMeta;
    try {
      meta = await this.getChannelMatchTableMeta();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.logger.warn('Oracle table metadata unavailable', { searchRef, error: message });
      return { total: rows.length, inserted: 0, updated: 0, failed: rows.length };
    }

    const channelColumn = this.resolveTableColumn(meta, 'channel');
    const messageIdColumn = this.resolveTableColumn(meta, 'message_id');
    const searchIdColumn = this.resolveTableColumn(meta, 'search_id');
    if (!channelColumn || !messageIdColumn) {
      this.logger.warn('Oracle channel_matches missing required columns', {
        searchRef,
        tableName: meta.tableName
      });
      return { total: rows.length, inserted: 0, updated: 0, failed: rows.length };
    }

    let inserted = 0;
    let updated = 0;
    let failed = 0;
    const failureSamples: string[] = [];

    for (const row of rows) {
      const channel = (row.channelLink || row.channelKey || '').slice(0, 128);
      if (!channel || !row.messageId) {
        failed += 1;
        continue;
      }
      const searchIdFromRow = typeof row.searchId === 'number' && Number.isFinite(row.searchId)
        ? row.searchId
        : (/^\d+$/.test(row.searchRef || '') ? Number.parseInt(row.searchRef, 10) : undefined);

      const tryPersist = async (currentMode: 'extended' | 'legacy'): Promise<'inserted' | 'updated' | 'failed'> => {
        const values = this.buildChannelMatchValues(meta, currentMode, row, channel);
        if (!values) return 'failed';

        try {
          await this.insertChannelMatchRow(meta, values);
          return 'inserted';
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          if (this.isOracleUniqueViolation(message)) {
            try {
              await this.updateChannelMatchRow(
                meta,
                values,
                channelColumn,
                messageIdColumn,
                searchIdColumn
              );
              return 'updated';
            } catch (updateErr) {
              const updateMessage = updateErr instanceof Error ? updateErr.message : String(updateErr);
              if (failureSamples.length < 5) failureSamples.push(updateMessage);
              return 'failed';
            }
          }

          if (failureSamples.length < 5) failureSamples.push(message);
          if (currentMode === 'extended' && this.isOracleInvalidIdentifier(message)) {
            this.channelMatchSchemaMode = 'legacy';
            mode = 'legacy';
            return tryPersist('legacy');
          }
          return 'failed';
        }
      };

      const result = await tryPersist(mode);
      if (result === 'inserted') inserted += 1;
      else if (result === 'updated') updated += 1;
      else failed += 1;

      if (result !== 'failed' && typeof searchIdFromRow === 'number' && searchIdFromRow > 0) {
        try {
          const matchId = await this.findChannelMatchId(
            meta,
            channelColumn,
            messageIdColumn,
            channel,
            row.messageId,
            searchIdColumn,
            searchIdFromRow
          );
          const clientLinks = this.buildClientLinksFromStoredRow({
            channelType: row.channelType,
            channelLink: row.channelLink,
            messageLink: row.messageLink,
            discoveredViaLink: row.discoveredViaLink,
            relatedLinks: row.relatedLinks
          });
          for (const link of clientLinks) {
            await this.upsertSearchLinkRow(
              searchIdFromRow,
              link,
              this.classifyClientLinkType(link),
              matchId
            );
          }
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          if (failureSamples.length < 5) failureSamples.push(`search_links:${message}`);
        }
      }
    }

    if (failed > 0 && failureSamples.length) {
      this.logger.warn('Oracle channel match row persistence failures', {
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
    searchRefArg?: string
  ): Promise<KeywordCrawlResultDto> {
    await this.initUserClient();
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
      Number(this.config.get<number>('crawlSearchPagesPerChat') || 30)
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
      const byName = await this.client.invoke(
        new Api.contacts.Search({ q: keyword, limit: seedLimit })
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
      const message = err instanceof Error ? err.message : String(err);
      await this.logStep(searchId, 'iterative_seed_contacts_error', { error: message });
    }

    try {
      const global = await this.client.invoke(
        new Api.messages.SearchGlobal({
          q: keyword,
          offsetDate: 0 as any,
          offsetPeer: new Api.InputPeerEmpty(),
          offsetId: 0 as any,
          limit: seedLimit,
          filter: new Api.InputMessagesFilterEmpty()
        } as any)
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
      const message = err instanceof Error ? err.message : String(err);
      await this.logStep(searchId, 'iterative_seed_global_error', { error: message });
    }

    const seedPublics: string[] = [];
    const discoveredLinks = new Set<string>();
    const clientLinks = new Set<string>();
    const discoveredInvites = new Set<string>();
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

      for (const frontierItem of frontier) {
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
        const chatIdentityKeywordMatch = this.chatIdentityContainsKeyword(chat, keywordTerms);
        const selfPublicLink = chatUsername
          ? this.canonicalizeTelegramLink(`https://t.me/${chatUsername}`)
          : null;
        const canJoinChannel = String((chat as any)?._ || '').toLowerCase().includes('channel');
        if (!chatKey || visitedChats.has(chatKey)) continue;
        visitedChats.add(chatKey);
        chatsProcessed += 1;

        await this.logStep(searchId, 'iterative_chat_start', {
          iteration: iterations,
          chat: chatKey
        });

        let joinRetriedAfterAccessError = false;
        if (canJoinChannel && joinPublicChannels) {
          await this.tryJoinChat(chat, searchId, 'iterative_chat_prejoin');
        }

        if (startBots && chatUsername && this.classifyChatType(chat) === 'bot') {
          const botKey = chatUsername.toLowerCase();
          if (!startedBots.has(botKey)) {
            startedBots.add(botKey);
            await this.startBotIfNeeded(chatUsername, undefined, searchId);
          }
        }

        let inputPeer: Api.TypeInputPeer;
        try {
          inputPeer = await this.invokeWithTimeout(
            this.client.getInputEntity(chat) as Promise<Api.TypeInputPeer>,
            15000,
            'iterative_chat_input_peer'
          );
        } catch (err) {
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
              15000
            );
          } catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            if (
              this.isAccessDeniedError(message)
              && canJoinChannel
              && joinPublicChannels
              && !joinRetriedAfterAccessError
            ) {
              joinRetriedAfterAccessError = true;
              const joined = await this.tryJoinChat(chat, searchId, 'iterative_chat_access');
              if (joined) {
                await this.logStep(searchId, 'iterative_chat_retry_after_join', {
                  chat: chatKey,
                  iteration: iterations,
                  offsetId,
                  pages
                });
                try {
                  inputPeer = await this.invokeWithTimeout(
                    this.client.getInputEntity(chat) as Promise<Api.TypeInputPeer>,
                    15000,
                    'iterative_chat_input_peer_retry'
                  );
                } catch (retryErr) {
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
            const messageKeywordMatch = this.messageContainsKeyword(text, keywordTerms);
            if (!messageKeywordMatch && !chatIdentityKeywordMatch) continue;

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
            if (!filteredLinks.length && !videoCaptionMatch) continue;

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

            const matchReason: 'keyword_hyperlink' | 'keyword_video' = filteredLinks.length
              ? 'keyword_hyperlink'
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
                relatedLinks: filteredLinks
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

        const bioLinks = await this.extractChatBioLinks(chat, searchId);
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
      for (const [link, origin] of linksFromIteration.entries()) {
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
              await this.startBotIfNeeded(parsed.username, parsed.botStartParam, searchId);
            }
          }
          if (parsed.username) {
            const botBioLinks = await this.extractBotBioLinks(parsed.username, searchId);
            for (const botBioLink of botBioLinks) {
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
              const resolvedBio = await this.resolveParsedLinkForFrontier({
                parsed: parsedBio,
                autoJoinInvites,
                keywordTerms,
                searchId,
                joinedPrivateChats
              });
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

        const resolved = await this.resolveParsedLinkForFrontier({
          parsed,
          autoJoinInvites,
          keywordTerms,
          searchId,
          joinedPrivateChats
        });
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
