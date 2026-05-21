import { config as loadEnv } from 'dotenv';
import { readFile, writeFile } from 'fs/promises';
import { TelegramClient, Api } from 'telegram';
import { StringSession } from 'telegram/sessions';

interface TargetRecord {
  uid?: string;
  title?: string;
  links: string[];
  privateMessages?: PrivateMessageRef[];
  resolvedText?: string;
  resolvedLinks?: string[];
  resolvedError?: string;
}

interface PrivateMessageRef {
  uid: string;
  messageId: number;
  link: string;
}

interface FoundMessage {
  targetUid?: string;
  targetTitle?: string;
  targetLinks: string[];
  query: string;
  reason: string;
  sourceType: string;
  sourceTitle: string;
  sourceUsername?: string;
  sourceLink?: string;
  messageId: number;
  messageLink?: string;
  text: string;
  extractedLinks: string[];
  confidence: 'exact' | 'strong' | 'candidate' | 'error';
  botProbe?: {
    bot: string;
    command: string;
    responseText: string;
    responseLinks: string[];
    responseReason?: string;
  };
}

type SocksProxyConfig = { ip: string; port: number; socksType: 4 | 5 };

function parseArgs() {
  const args = process.argv.slice(2);
  const out: Record<string, string | boolean> = {};
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg.startsWith('--')) {
      const key = arg.slice(2);
      const next = args[i + 1];
      if (!next || next.startsWith('--')) {
        out[key] = true;
      } else {
        out[key] = next;
        i += 1;
      }
    }
  }
  return out;
}

function parseProxy(proxyUrl: string): SocksProxyConfig | undefined {
  const raw = proxyUrl.trim();
  if (!raw) return undefined;
  const parsed = new URL(raw);
  const proto = parsed.protocol.toLowerCase();
  if (proto !== 'socks5:' && proto !== 'socks4:') {
    throw new Error('Unsupported proxy protocol: ' + parsed.protocol);
  }
  const port = Number(parsed.port || 1080);
  if (!port || Number.isNaN(port)) throw new Error('Invalid SOCKS_PROXY port.');
  return {
    ip: parsed.hostname,
    port,
    socksType: proto === 'socks4:' ? 4 : 5
  };
}

function normalizeText(text: string): string {
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

function tokenize(text: string): string[] {
  return normalizeText(text).match(/[\p{L}\p{N}]+/gu) || [];
}

function normalizeTelegramLink(link: string): string {
  let normalized = link.trim();
  normalized = normalized.replace(/[)\],.;!?،؛]+$/g, '');
  normalized = normalized.replace(/^http:\/\//i, 'https://');
  if (/^(?:t|telegram)\.me\//i.test(normalized)) normalized = 'https://' + normalized;
  return normalized.replace(/^https?:\/\/(?:www\.)?(?:telegram\.me|t\.me)\//i, 'https://t.me/');
}

function extractLinks(text: string): string[] {
  const matches = text.match(/(?:https?:\/\/)?(?:t|telegram)\.me\/[^\s<>)\]]+/gi) || [];
  return matches.map(normalizeTelegramLink);
}

function extractLinksFromEntities(text: string, entities: any[]): string[] {
  const links: string[] = [];
  for (const ent of entities || []) {
    const type = String(ent?._ || ent?.className || ent?.constructor?.name || '');
    if (ent?.url) links.push(normalizeTelegramLink(String(ent.url)));
    if (/messageEntityUrl|MessageEntityUrl/.test(type) && Number.isFinite(ent.offset) && Number.isFinite(ent.length)) {
      links.push(normalizeTelegramLink(text.substring(ent.offset, ent.offset + ent.length)));
    }
  }
  return links.filter((link) => /^https:\/\/t\.me\//i.test(link));
}

function extractLinksFromMarkup(markup: any): string[] {
  const links: string[] = [];
  const rows = markup?.rows || markup?.inlineKeyboard?.rows || [];
  for (const row of rows) {
    const buttons = row?.buttons || row;
    for (const button of buttons || []) {
      if (button?.url) links.push(normalizeTelegramLink(String(button.url)));
    }
  }
  return links.filter((link) => /^https:\/\/t\.me\//i.test(link));
}

function parseTargets(input: string): TargetRecord[] {
  const blocks = input
    .split(/-{5,}/g)
    .map((block) => block.trim())
    .filter(Boolean);
  const targets: TargetRecord[] = [];

  for (const block of blocks) {
    const uid = block.match(/Telegram UID:\s*([0-9]+)/i)?.[1];
    const title = block.match(/Title:\s*(.+?)(?:\n|$)/i)?.[1]?.trim();
    const links = Array.from(new Set(extractLinks(block)));
    const privateMessages = links
      .map((link) => {
        const match = link.match(/^https:\/\/t\.me\/c\/([0-9]+)\/([0-9]+)/i);
        if (!match) return null;
        return { uid: match[1], messageId: Number(match[2]), link };
      })
      .filter((item): item is PrivateMessageRef => Boolean(item && Number.isFinite(item.messageId)));
    if (uid || title || links.length) targets.push({ uid, title, links, privateMessages });
  }

  return targets;
}

function importantTitleQueries(title: string): string[] {
  const stopWords = new Set([
    'دانلود',
    'رایگان',
    'کانال',
    'گروه',
    'فیلم',
    'سریال',
    'ایرانی',
    'خارجی',
    'قسمت',
    'فصل',
    'link',
    'movie',
    'serial',
    'series',
    'channel',
    'group'
  ]);
  const tokens = tokenize(title).filter((token) => token.length >= 3 && !stopWords.has(token));
  const queries = new Set<string>();
  if (title.trim().length >= 3) queries.add(title.trim());
  for (const token of tokens.slice(0, 6)) queries.add(token);
  if (tokens.length >= 2) queries.add(tokens.slice(0, 3).join(' '));
  return Array.from(queries);
}

function buildQueries(target: TargetRecord): string[] {
  const queries = new Set<string>();
  for (const link of target.links) {
    queries.add(link);
    const privateMatch = link.match(/^https:\/\/t\.me\/c\/([0-9]+)(?:\/([0-9]+))?/i);
    if (privateMatch?.[1]) queries.add(privateMatch[1]);
    const usernameMatch = link.match(/^https:\/\/t\.me\/([A-Za-z0-9_]+)/);
    if (usernameMatch?.[1]) queries.add(usernameMatch[1]);
  }
  if (target.uid) queries.add(target.uid);
  if (target.title) {
    for (const query of importantTitleQueries(target.title)) queries.add(query);
  }
  if (target.resolvedText) {
    for (const query of importantTitleQueries(target.resolvedText).slice(0, 8)) queries.add(query);
  }
  for (const link of target.resolvedLinks || []) {
    queries.add(link);
    const bot = parseBotStartLink(link);
    if (bot) queries.add(bot.username);
  }
  return Array.from(queries).filter((query) => query.trim().length >= 3);
}

function chatKey(chat: any): string {
  return String(chat?.id || chat?.accessHash || chat?.username || '');
}

function chatTitle(chat: any): string {
  return String(chat?.title || [chat?.firstName, chat?.lastName].filter(Boolean).join(' ') || chat?.username || '').trim();
}

function chatUsername(chat: any): string | undefined {
  return typeof chat?.username === 'string' && chat.username.trim() ? chat.username.trim() : undefined;
}

function sourceType(chat: any): string {
  const kind = String(chat?._ || chat?.className || chat?.constructor?.name || '').toLowerCase();
  if (kind.includes('user') && chat?.bot) return 'bot';
  if (kind.includes('channel')) return chat?.broadcast ? 'channel' : 'group';
  if (kind.includes('chat')) return 'group';
  return kind || 'unknown';
}

function messageLink(chat: any, messageId: number): string | undefined {
  const username = chatUsername(chat);
  if (username) return 'https://t.me/' + username + '/' + messageId;
  const rawId = String(chat?.id || '').replace(/^-/, '');
  if (!rawId) return undefined;
  const privateId = rawId.startsWith('100') ? rawId.slice(3) : rawId;
  return 'https://t.me/c/' + privateId + '/' + messageId;
}

function parseBotStartLink(link: string): { username: string; command: string } | null {
  const normalized = normalizeTelegramLink(link);
  const match = normalized.match(/^https:\/\/t\.me\/([A-Za-z0-9_]*bot)(?:[/?#].*)?$/i);
  if (!match?.[1]) return null;
  try {
    const parsed = new URL(normalized);
    const start = parsed.searchParams.get('start')
      || parsed.searchParams.get('startgroup')
      || parsed.searchParams.get('startapp');
    return {
      username: match[1],
      command: start ? '/start ' + start : '/start'
    };
  } catch {
    return {
      username: match[1],
      command: '/start'
    };
  }
}

function targetMatches(target: TargetRecord, text: string, links: string[]): string | null {
  const normalizedText = normalizeText(text);
  const normalizedLinks = links.map(normalizeTelegramLink);
  for (const targetLink of target.links.map(normalizeTelegramLink)) {
    if (normalizedText.includes(normalizeText(targetLink))) return 'exact_text_link';
    if (normalizedLinks.includes(targetLink)) return 'exact_entity_or_button_link';
    const privateId = targetLink.match(/^https:\/\/t\.me\/c\/([0-9]+)/i)?.[1];
    if (privateId && (normalizedText.includes(privateId) || normalizedLinks.some((link) => link.includes('/c/' + privateId)))) {
      return 'private_uid_reference';
    }
  }
  if (target.uid && (normalizedText.includes(target.uid) || normalizedLinks.some((link) => link.includes('/c/' + target.uid)))) {
    return 'uid_reference';
  }
  if (target.title) {
    const titleTokens = tokenize(target.title).filter((token) => token.length >= 3);
    if (titleTokens.length) {
      const hitCount = titleTokens.filter((token) => normalizedText.includes(token)).length;
      if (hitCount >= Math.min(2, titleTokens.length)) return 'title_terms';
    }
  }
  if (target.resolvedText) {
    const resolvedTokens = tokenize(target.resolvedText)
      .filter((token) => token.length >= 4 && !/^\d+$/.test(token))
      .slice(0, 10);
    if (resolvedTokens.length) {
      const hitCount = resolvedTokens.filter((token) => normalizedText.includes(token)).length;
      if (hitCount >= Math.min(3, resolvedTokens.length)) return 'resolved_message_terms';
    }
  }
  for (const resolvedLink of target.resolvedLinks || []) {
    const normalized = normalizeTelegramLink(resolvedLink);
    if (normalizedText.includes(normalizeText(normalized))) return 'resolved_message_link_text';
    if (normalizedLinks.includes(normalized)) return 'resolved_message_link_entity';
  }
  return null;
}

function confidenceForReason(reason: string): FoundMessage['confidence'] {
  if (reason.startsWith('search_error') || reason.startsWith('bot_probe_error')) return 'error';
  if (/exact|uid_reference|private_uid_reference|resolved_message_link/.test(reason)) return 'exact';
  if (/bot_probe_|resolved_message_terms/.test(reason)) return 'strong';
  return 'candidate';
}

async function invokeWithTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(label + ' timeout after ' + timeoutMs + 'ms')), timeoutMs);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (err) => {
        clearTimeout(timer);
        reject(err);
      }
    );
  });
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function probeBot(
  client: TelegramClient,
  botLink: string,
  target: TargetRecord
): Promise<FoundMessage['botProbe'] | undefined> {
  const parsed = parseBotStartLink(botLink);
  if (!parsed) return undefined;
  const peer = await invokeWithTimeout(
    client.getInputEntity('@' + parsed.username) as Promise<Api.TypeInputPeer>,
    15000,
    'bot input'
  );
  await invokeWithTimeout(
    client.invoke(new Api.messages.SendMessage({
      peer: peer as any,
      message: parsed.command,
      randomId: BigInt(Date.now()) as any,
      noWebpage: true
    } as any)),
    15000,
    'bot start'
  );
  await sleep(2500);
  const history: any = await invokeWithTimeout(
    client.invoke(new Api.messages.GetHistory({
      peer: peer as any,
      offsetId: 0,
      addOffset: 0,
      limit: 5,
      maxId: 0,
      minId: 0,
      hash: 0 as any
    } as any)),
    15000,
    'bot history'
  );
  const texts: string[] = [];
  const links: string[] = [];
  for (const msg of history?.messages || []) {
    const msgAny = msg as any;
    const text = String(msgAny?.message || '');
    texts.push(text);
    links.push(
      ...extractLinks(text),
      ...extractLinksFromEntities(text, msgAny?.entities || []),
      ...extractLinksFromMarkup(msgAny?.replyMarkup)
    );
  }
  const responseText = texts.join('\n\n---\n\n').slice(0, 1500);
  const responseLinks = Array.from(new Set(links.map(normalizeTelegramLink)));
  return {
    bot: parsed.username,
    command: parsed.command,
    responseText,
    responseLinks,
    responseReason: targetMatches(target, responseText, responseLinks) || undefined
  };
}

async function hydrateTargetFromPrivateMessage(client: TelegramClient, target: TargetRecord): Promise<TargetRecord> {
  const refs = target.privateMessages || [];
  if (!refs.length) return target;
  const texts: string[] = [];
  const links: string[] = [];
  const errors: string[] = [];

  for (const ref of refs.slice(0, 3)) {
    try {
      const entity = await invokeWithTimeout(
        client.getEntity(BigInt('-100' + ref.uid) as any) as Promise<Api.TypeChat>,
        15000,
        'private target entity'
      );
      const peer = await invokeWithTimeout(
        client.getInputEntity(entity) as Promise<Api.TypeInputPeer>,
        15000,
        'private target input'
      );
      const history: any = await invokeWithTimeout(
        client.invoke(new Api.messages.GetHistory({
          peer: peer as any,
          offsetId: ref.messageId + 1,
          addOffset: 0,
          limit: 5,
          maxId: 0,
          minId: 0,
          hash: 0 as any
        } as any)),
        15000,
        'private target history'
      );
      const msg = (history?.messages || []).find((item: any) => Number(item?.id || 0) === ref.messageId);
      if (!msg) {
        errors.push(ref.link + ': message_not_found_or_inaccessible');
        continue;
      }
      const msgAny = msg as any;
      const text = String(msgAny?.message || '');
      if (text.trim()) texts.push(text);
      links.push(
        ...extractLinks(text),
        ...extractLinksFromEntities(text, msgAny?.entities || []),
        ...extractLinksFromMarkup(msgAny?.replyMarkup)
      );
    } catch (err) {
      errors.push(ref.link + ': ' + (err instanceof Error ? err.message : String(err)));
    }
  }

  return {
    ...target,
    resolvedText: texts.join('\n\n').slice(0, 3000) || target.resolvedText,
    resolvedLinks: Array.from(new Set([...(target.resolvedLinks || []), ...links.map(normalizeTelegramLink)])),
    resolvedError: errors.length ? errors.join(' | ') : undefined
  };
}

async function main() {
  const args = parseArgs();
  loadEnv({ path: String(args.env || process.env.ENV_FILE || '.env.production') });

  const inputPath = typeof args.input === 'string' ? args.input : '';
  if (!inputPath) throw new Error('Usage: npm run deep:invite-search -- --input targets.txt --out results.json');

  const apiId = Number(process.env.API_ID || 0);
  const apiHash = process.env.API_HASH || '';
  const sessionString = process.env.SESSION_STRING || '';
  if (!apiId || !apiHash || !sessionString) throw new Error('Missing API_ID/API_HASH/SESSION_STRING.');

  const input = await readFile(inputPath, 'utf8');
  const targets = parseTargets(input);
  const limit = Number(args.limit || 30);
  const delayMs = Number(args.delayMs || 800);
  const outPath = typeof args.out === 'string' ? args.out : '';
  const startBots = args.startBots === true || args.startBots === 'true';
  const maxBotProbes = Number(args.maxBotProbes || 20);
  const proxy = parseProxy(process.env.SOCKS_PROXY || '');

  const client = new TelegramClient(new StringSession(sessionString), apiId, apiHash, {
    connectionRetries: 5,
    ...(proxy ? { proxy } : {})
  });

  const found: FoundMessage[] = [];
  const hydratedTargets: TargetRecord[] = [];
  const seen = new Set<string>();
  const probedBots = new Set<string>();

  try {
    await client.connect();
    for (const rawTarget of targets) {
      const target = args.hydrate === 'false'
        ? rawTarget
        : await hydrateTargetFromPrivateMessage(client, rawTarget);
      hydratedTargets.push(target);
      const queries = buildQueries(target).slice(0, Number(args.queries || 12));
      for (const query of queries) {
        await sleep(delayMs);
        let result: any;
        try {
          result = await invokeWithTimeout(
            client.invoke(new Api.messages.SearchGlobal({
              q: query,
              offsetDate: 0 as any,
              offsetPeer: new Api.InputPeerEmpty(),
              offsetId: 0 as any,
              limit,
              filter: new Api.InputMessagesFilterEmpty()
            } as any)),
            30000,
            'SearchGlobal'
          );
        } catch (err) {
          found.push({
            targetUid: target.uid,
            targetTitle: target.title,
            targetLinks: target.links,
            query,
            reason: 'search_error:' + (err instanceof Error ? err.message : String(err)),
            sourceType: '',
            sourceTitle: '',
            messageId: 0,
            text: '',
            extractedLinks: [],
            confidence: 'error'
          });
          continue;
        }

        const chatsById = new Map<string, any>();
        for (const chat of [...(result?.chats || []), ...(result?.users || [])]) {
          const key = chatKey(chat);
          if (key) chatsById.set(key.replace(/^-/, ''), chat);
        }

        for (const msg of result?.messages || []) {
          const msgAny = msg as any;
          const text = String(msgAny?.message || '');
          const extractedLinks = Array.from(new Set([
            ...extractLinks(text),
            ...extractLinksFromEntities(text, msgAny?.entities || []),
            ...extractLinksFromMarkup(msgAny?.replyMarkup)
          ]));
          const reason = targetMatches(target, text, extractedLinks);
          if (!reason) continue;
          const confidence = confidenceForReason(reason);
          if (args.strict === 'true' && confidence === 'candidate') continue;

          const peerId = String(msgAny?.peerId?.channelId || msgAny?.peerId?.chatId || msgAny?.peerId?.userId || '').replace(/^-/, '');
          const chat = chatsById.get(peerId) || {};
          const id = Number(msgAny?.id || 0);
          const key = [target.uid || target.title || target.links[0] || '', query, peerId, id, reason].join('|');
          if (seen.has(key)) continue;
          seen.add(key);

          found.push({
            targetUid: target.uid,
            targetTitle: target.title,
            targetLinks: target.links,
            query,
            reason,
            sourceType: sourceType(chat),
            sourceTitle: chatTitle(chat),
            sourceUsername: chatUsername(chat),
            sourceLink: chatUsername(chat) ? 'https://t.me/' + chatUsername(chat) : undefined,
            messageId: id,
            messageLink: messageLink(chat, id),
            text: text.slice(0, 1000),
            extractedLinks,
            confidence
          });

          if (startBots && probedBots.size < maxBotProbes) {
            for (const link of extractedLinks) {
              const botStart = parseBotStartLink(link);
              if (!botStart) continue;
              const botKey = botStart.username.toLowerCase() + '|' + botStart.command;
              if (probedBots.has(botKey)) continue;
              probedBots.add(botKey);
              try {
                const botProbe = await probeBot(client, link, target);
                if (botProbe) {
                  found.push({
                    targetUid: target.uid,
                    targetTitle: target.title,
                    targetLinks: target.links,
                    query,
                    reason: botProbe.responseReason ? 'bot_probe_' + botProbe.responseReason : 'bot_probe_response',
                    sourceType: 'bot',
                    sourceTitle: botProbe.bot,
                    sourceUsername: botProbe.bot,
                    sourceLink: 'https://t.me/' + botProbe.bot,
                    messageId: 0,
                    text: botProbe.responseText,
                    extractedLinks: botProbe.responseLinks,
                    confidence: confidenceForReason(botProbe.responseReason ? 'bot_probe_' + botProbe.responseReason : 'bot_probe_response'),
                    botProbe
                  });
                }
              } catch (err) {
                found.push({
                  targetUid: target.uid,
                  targetTitle: target.title,
                  targetLinks: target.links,
                  query,
                  reason: 'bot_probe_error:' + (err instanceof Error ? err.message : String(err)),
                  sourceType: 'bot',
                  sourceTitle: botStart.username,
                  sourceUsername: botStart.username,
                  sourceLink: 'https://t.me/' + botStart.username,
                  messageId: 0,
                  text: '',
                  extractedLinks: [],
                  confidence: 'error'
                });
              }
            }
          }
        }
      }
    }
  } finally {
    await client.disconnect();
  }

  const payload = {
    generatedAt: new Date().toISOString(),
    targets: targets.length,
    hydratedTargets,
    results: found
  };
  const json = JSON.stringify(payload, null, 2);
  if (outPath) {
    await writeFile(outPath, json, 'utf8');
  } else {
    process.stdout.write(json + '\n');
  }
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
});
