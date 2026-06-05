import 'reflect-metadata';
import { config as loadEnv } from 'dotenv';
import { readFile, writeFile } from 'fs/promises';
import { Api, TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions';
import ExcelJS from 'exceljs';
import JSZip from 'jszip';
import { AppDataSource } from '../database/data-source';
import { TelegramChatEntity } from '../entities/telegram-chat.entity';
import { TelegramInputLinkEntity } from '../entities/telegram-input-link.entity';
import { TelegramMessageLinkEntity } from '../entities/telegram-message-link.entity';

type LinkType = 'invite' | 'bot' | 'public_chat' | 'public_message' | 'private_chat' | 'private_message';

interface Args {
  input?: string;
  text?: string;
  out?: string;
  env?: string;
  dryRun?: boolean;
  parseOnly?: boolean;
  startBots?: boolean;
  joinInvites?: boolean;
  joinPublic?: boolean;
  requestPrivate?: boolean;
  botHistoryLimit?: number;
  botDelayMs?: number;
  botPollCount?: number;
  botPollIntervalMs?: number;
}

interface ParsedLink {
  raw: string;
  canonical: string;
  type: LinkType;
  username?: string;
  privateUid?: string;
  messageId?: number;
  inviteHash?: string;
  startCommand?: string;
}

interface ProcessResult {
  link: string;
  type: LinkType;
  status: string;
  chatId?: number;
  chatKey?: string;
  title?: string;
  username?: string;
  inviteLink?: string;
  extractedLinks: string[];
  error?: string;
}

interface ChatUpsertInput {
  chatKey: string;
  chatType: string;
  title?: string | null;
  username?: string | null;
  publicLink?: string | null;
  inviteLink?: string | null;
  privateUid?: string | null;
  joinStatus: string;
  error?: string | null;
}

function parseArgs(): Args {
  const out: Args = {};
  const raw = process.argv.slice(2);
  for (let i = 0; i < raw.length; i += 1) {
    const arg = raw[i];
    if (!arg.startsWith('--')) continue;
    const key = arg.slice(2);
    const next = raw[i + 1];
    const value = !next || next.startsWith('--') ? true : next;
    if (value !== true) i += 1;
    switch (key) {
      case 'input':
      case 'text':
      case 'out':
      case 'env':
        out[key] = String(value);
        break;
      case 'dryRun':
      case 'parseOnly':
      case 'startBots':
      case 'joinInvites':
      case 'joinPublic':
      case 'requestPrivate':
        out[key] = value === true || value === 'true';
        break;
      case 'botHistoryLimit':
      case 'botDelayMs':
      case 'botPollCount':
      case 'botPollIntervalMs':
        out[key] = Number(value);
        break;
    }
  }
  return out;
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
  return Array.from(new Set(matches.map(normalizeTelegramLink)));
}

function parseLink(rawLink: string): ParsedLink | null {
  const canonical = normalizeTelegramLink(rawLink);
  const invite = canonical.match(/^https:\/\/t\.me\/(?:\+|joinchat\/)([A-Za-z0-9_-]+)(?:[/?#].*)?$/i);
  if (invite?.[1]) {
    return {
      raw: rawLink,
      canonical: `https://t.me/+${invite[1]}`,
      type: 'invite',
      inviteHash: invite[1]
    };
  }

  const privateMessage = canonical.match(/^https:\/\/t\.me\/c\/(\d+)\/(\d+)(?:[/?#].*)?$/i);
  if (privateMessage) {
    return {
      raw: rawLink,
      canonical: `https://t.me/c/${privateMessage[1]}/${privateMessage[2]}`,
      type: 'private_message',
      privateUid: privateMessage[1],
      messageId: Number(privateMessage[2])
    };
  }

  const privateChat = canonical.match(/^https:\/\/t\.me\/c\/(\d+)\/?(?:[?#].*)?$/i);
  if (privateChat) {
    return {
      raw: rawLink,
      canonical: `https://t.me/c/${privateChat[1]}`,
      type: 'private_chat',
      privateUid: privateChat[1]
    };
  }

  const publicMessage = canonical.match(/^https:\/\/t\.me\/(?:s\/)?([A-Za-z0-9_]+)\/(\d+)(?:[/?#].*)?$/i);
  if (publicMessage) {
    return {
      raw: rawLink,
      canonical: `https://t.me/${publicMessage[1]}/${publicMessage[2]}`,
      type: 'public_message',
      username: publicMessage[1],
      messageId: Number(publicMessage[2])
    };
  }

  const username = canonical.match(/^https:\/\/t\.me\/([A-Za-z0-9_]+)(?:[/?#].*)?$/i);
  if (!username?.[1]) return null;
  const parsedUrl = new URL(canonical);
  const start = parsedUrl.searchParams.get('start')
    || parsedUrl.searchParams.get('startgroup')
    || parsedUrl.searchParams.get('startapp');
  const isBot = /bot$/i.test(username[1]);
  return {
    raw: rawLink,
    canonical: isBot && start ? canonical : `https://t.me/${username[1]}`,
    type: isBot ? 'bot' : 'public_chat',
    username: username[1],
    startCommand: isBot ? (start ? `/start ${start}` : '/start') : undefined
  };
}

async function readInput(args: Args): Promise<string> {
  if (args.text) return args.text;
  if (!args.input) throw new Error('Usage: npm run telegram:import-links -- --input links.txt [--joinInvites true --joinPublic true --startBots true]');

  if (/\.xlsx?$/i.test(args.input)) {
    return readSpreadsheetInput(args.input);
  }

  return readFile(args.input, 'utf8');
}

function cellText(cell: ExcelJS.Cell): string {
  const value: any = cell.value;
  if (value && typeof value === 'object') {
    if (typeof value.hyperlink === 'string') return [value.text, value.hyperlink].filter(Boolean).join(' ');
    if (typeof value.text === 'string') return value.text;
    if (Array.isArray(value.richText)) return value.richText.map((part: any) => part.text || '').join('');
    if (value.result !== undefined) return String(value.result);
  }
  return String(cell.text || value || '').trim();
}

async function readSpreadsheetInput(inputPath: string): Promise<string> {
  const content = await readFile(inputPath);
  const values: string[] = [];
  try {
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.load(content as unknown as Parameters<ExcelJS.Xlsx['load']>[0]);
    for (const sheet of workbook.worksheets) {
      sheet.eachRow({ includeEmpty: false }, (row) => {
        row.eachCell({ includeEmpty: false }, (cell) => {
          values.push(cellText(cell));
        });
      });
    }
    return values.join('\n');
  } catch (err) {
    const zipText = await readXlsxZipText(content).catch(() => '');
    const rawText = content.toString('utf8');
    const fallbackText = [zipText, rawText].filter(Boolean).join('\n');
    if (extractLinks(fallbackText).length) return fallbackText;
    const message = err instanceof Error ? err.message : String(err);
    throw new Error(`Could not read spreadsheet links from ${inputPath}: ${message}`);
  }
}

async function readXlsxZipText(content: Buffer): Promise<string> {
  const zip = await JSZip.loadAsync(content);
  const parts: string[] = [];
  const entries = Object.values(zip.files).filter((file) => (
    !file.dir && /^xl\//i.test(file.name) && /\.(?:xml|rels)$/i.test(file.name)
  ));
  for (const file of entries) {
    parts.push(await file.async('string'));
  }
  return parts.join('\n');
}

function chatTitle(chat: any): string | null {
  const title = String(chat?.title || [chat?.firstName, chat?.lastName].filter(Boolean).join(' ') || '').trim();
  return title || null;
}

function chatUsername(chat: any): string | null {
  return typeof chat?.username === 'string' && chat.username.trim() ? chat.username.trim() : null;
}

function chatType(chat: any): string {
  const kind = String(chat?._ || chat?.className || chat?.constructor?.name || '').toLowerCase();
  if (kind.includes('user') && chat?.bot) return 'bot';
  if (kind.includes('channel')) return chat?.broadcast ? 'channel' : 'group';
  if (kind.includes('chat')) return 'group';
  return 'unknown';
}

function chatKey(chat: any, fallback: string): string {
  const username = chatUsername(chat);
  if (username) return username.toLowerCase();
  const id = String(chat?.id || '').replace(/^-/, '');
  return id || fallback;
}

function statusFromTelegramError(error: string): string {
  if (/FLOOD_WAIT|A wait of \d+ seconds is required/i.test(error)) return 'flood_wait';
  if (/INVITE_REQUEST_SENT/i.test(error)) return 'join_requested';
  if (/USER_ALREADY_PARTICIPANT/i.test(error)) return 'already_joined';
  return 'error';
}

function privateUidFromChat(chat: any): string | null {
  const id = String(chat?.id || '').replace(/^-/, '');
  if (!id) return null;
  return id.startsWith('100') ? id.slice(3) : id;
}

function publicLinkFromChat(chat: any): string | null {
  const username = chatUsername(chat);
  return username ? `https://t.me/${username}` : null;
}

function messageLinkFromChat(chat: any, messageId: number): string | null {
  const username = chatUsername(chat);
  if (username) return `https://t.me/${username}/${messageId}`;
  const privateUid = privateUidFromChat(chat);
  return privateUid ? `https://t.me/c/${privateUid}/${messageId}` : null;
}

function extractLinksFromEntities(text: string, entities: any[]): string[] {
  const links: string[] = [];
  for (const ent of entities || []) {
    if (ent?.url) links.push(String(ent.url));
    const type = String(ent?._ || ent?.className || ent?.constructor?.name || '');
    if (/messageEntityUrl|MessageEntityUrl/.test(type) && Number.isFinite(ent.offset) && Number.isFinite(ent.length)) {
      links.push(text.substring(ent.offset, ent.offset + ent.length));
    }
  }
  return links.filter((link) => /(?:t|telegram)\.me\//i.test(link)).map(normalizeTelegramLink);
}

function extractLinksFromMarkup(markup: any): string[] {
  const links: string[] = [];
  const rows = markup?.rows || markup?.inlineKeyboard?.rows || [];
  for (const row of rows) {
    const buttons = row?.buttons || row;
    for (const button of buttons || []) {
      if (button?.url && /(?:t|telegram)\.me\//i.test(button.url)) links.push(normalizeTelegramLink(String(button.url)));
    }
  }
  return links;
}

function extractLinksFromMedia(media: any): string[] {
  const links: string[] = [];
  const webPage = media?.webpage || media?.webPage || media;
  for (const value of [webPage?.url, webPage?.displayUrl, webPage?.display_url]) {
    if (value && /(?:t|telegram)\.me\//i.test(String(value))) {
      links.push(normalizeTelegramLink(String(value)));
    }
  }
  return links;
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function invokeWithTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} timeout after ${timeoutMs}ms`)), timeoutMs);
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

async function upsertChat(input: ChatUpsertInput): Promise<TelegramChatEntity> {
  const repo = AppDataSource.getRepository(TelegramChatEntity);
  const existing = await repo.findOne({ where: { chat_key: input.chatKey } });
  const row = existing || repo.create({ chat_key: input.chatKey });
  row.chat_type = input.chatType;
  row.title = input.title || row.title || null;
  row.username = input.username || row.username || null;
  row.public_link = input.publicLink || row.public_link || null;
  row.invite_link = input.inviteLink || row.invite_link || null;
  row.private_uid = input.privateUid || row.private_uid || null;
  row.join_status = input.joinStatus;
  row.last_error = input.error || null;
  row.updated_at = new Date();
  return repo.save(row);
}

async function upsertInputLink(parsed: ParsedLink, status: string, chatId?: number, error?: string): Promise<void> {
  const repo = AppDataSource.getRepository(TelegramInputLinkEntity);
  const existing = await repo.findOne({ where: { link: parsed.canonical } });
  const row = existing || repo.create({ link: parsed.canonical });
  row.link_type = parsed.type;
  row.telegram_chat_id = chatId || row.telegram_chat_id || null;
  row.status = status;
  row.last_error = error || null;
  row.updated_at = new Date();
  await repo.save(row);
}

async function saveMessageLink(messageLink: string, chatId?: number, messageId?: number, inviteLink?: string, from?: string): Promise<void> {
  const repo = AppDataSource.getRepository(TelegramMessageLinkEntity);
  const existing = await repo.findOne({ where: { message_link: messageLink } });
  const row = existing || repo.create({ message_link: messageLink });
  row.telegram_chat_id = chatId || row.telegram_chat_id || null;
  row.message_id = Number.isFinite(messageId) ? messageId! : row.message_id || null;
  row.invite_link = inviteLink || row.invite_link || null;
  row.discovered_from_link = from || row.discovered_from_link || null;
  await repo.save(row);
}

async function saveMessageLinksForChat(chat: any, chatRow: TelegramChatEntity, links: string[], from: string): Promise<void> {
  for (const link of links) {
    const parsed = parseLink(link);
    if (!parsed || !parsed.messageId) continue;
    await saveMessageLink(parsed.canonical, chatRow.id, parsed.messageId, chatRow.invite_link || undefined, from);
  }

  // Keep direct message links for joined chats grouped under their chat even when extracted from bot responses.
  const direct = links
    .map(parseLink)
    .filter((item): item is ParsedLink => Boolean(item && item.messageId));
  for (const item of direct) {
    const ownLink = messageLinkFromChat(chat, item.messageId || 0);
    if (ownLink) await saveMessageLink(ownLink, chatRow.id, item.messageId, chatRow.invite_link || undefined, from);
  }
}

async function processInvite(client: TelegramClient, parsed: ParsedLink, args: Args): Promise<ProcessResult> {
  let status = 'checked';
  let chat: any | null = null;
  let error: string | undefined;
  try {
    const checked: any = await invokeWithTimeout(
      client.invoke(new Api.messages.CheckChatInvite({ hash: parsed.inviteHash! } as any)),
      15000,
      'CheckChatInvite'
    );
    chat = checked?.chat || null;
    if (!chat && checked?.title) {
      status = 'invite_requires_join';
    }
    if (args.joinInvites && !args.dryRun) {
      const imported: any = await invokeWithTimeout(
        client.invoke(new Api.messages.ImportChatInvite({ hash: parsed.inviteHash! } as any)),
        20000,
        'ImportChatInvite'
      );
      chat = (imported?.chats || [])[0] || chat;
      status = 'joined';
    }
  } catch (err) {
    error = err instanceof Error ? err.message : String(err);
    status = statusFromTelegramError(error);
  }

  let chatRow: TelegramChatEntity | undefined;
  if (chat) {
    chatRow = await upsertChat({
      chatKey: chatKey(chat, parsed.canonical),
      chatType: chatType(chat),
      title: chatTitle(chat),
      username: chatUsername(chat),
      publicLink: publicLinkFromChat(chat),
      inviteLink: parsed.canonical,
      privateUid: privateUidFromChat(chat),
      joinStatus: status,
      error
    });
  }
  await upsertInputLink(parsed, status, chatRow?.id, error);
  return {
    link: parsed.canonical,
    type: parsed.type,
    status,
    chatId: chatRow?.id,
    chatKey: chatRow?.chat_key,
    title: chatRow?.title || undefined,
    username: chatRow?.username || undefined,
    inviteLink: parsed.canonical,
    extractedLinks: [],
    error
  };
}

async function processPublicChat(client: TelegramClient, parsed: ParsedLink, args: Args): Promise<ProcessResult> {
  let status = 'resolved';
  let chat: any | null = null;
  let error: string | undefined;
  try {
    chat = await invokeWithTimeout(client.getEntity('@' + parsed.username) as Promise<Api.TypeChat>, 15000, 'public entity');
    if (args.joinPublic && !args.dryRun) {
      await invokeWithTimeout(
        client.invoke(new Api.channels.JoinChannel({ channel: chat as any } as any)),
        20000,
        'JoinChannel'
      );
      status = 'joined';
    }
  } catch (err) {
    error = err instanceof Error ? err.message : String(err);
    status = statusFromTelegramError(error);
  }

  const chatRow = chat
    ? await upsertChat({
      chatKey: chatKey(chat, parsed.canonical),
      chatType: chatType(chat),
      title: chatTitle(chat),
      username: chatUsername(chat),
      publicLink: publicLinkFromChat(chat) || parsed.canonical,
      inviteLink: parsed.canonical,
      privateUid: privateUidFromChat(chat),
      joinStatus: status,
      error
    })
    : undefined;
  await upsertInputLink(parsed, status, chatRow?.id, error);
  return {
    link: parsed.canonical,
    type: parsed.type,
    status,
    chatId: chatRow?.id,
    chatKey: chatRow?.chat_key,
    title: chatRow?.title || undefined,
    username: chatRow?.username || undefined,
    inviteLink: chatRow?.invite_link || parsed.canonical,
    extractedLinks: [],
    error
  };
}

async function processMessageLink(client: TelegramClient, parsed: ParsedLink, args: Args): Promise<ProcessResult> {
  let status = 'stored';
  let chat: any | null = null;
  let error: string | undefined;
  let chatRow: TelegramChatEntity | undefined;
  if (parsed.privateUid) {
    chatRow = await upsertChat({
      chatKey: parsed.privateUid,
      chatType: 'private',
      privateUid: parsed.privateUid,
      joinStatus: 'message_link_only'
    });
  } else if (parsed.username) {
    try {
      chat = await invokeWithTimeout(client.getEntity('@' + parsed.username) as Promise<Api.TypeChat>, 15000, 'public message entity');
      status = 'resolved';
      if (args.joinPublic && !args.dryRun) {
        const kind = String(chat?._ || chat?.className || chat?.constructor?.name || '').toLowerCase();
        const canJoin = kind.includes('channel');
        if (canJoin && chat?.left !== false) {
          await invokeWithTimeout(
            client.invoke(new Api.channels.JoinChannel({ channel: chat as any } as any)),
            20000,
            'JoinChannelForPublicMessage'
          );
          status = 'joined';
        } else if (canJoin) {
          status = 'already_joined';
        }
      }
    } catch (err) {
      error = err instanceof Error ? err.message : String(err);
      status = statusFromTelegramError(error);
    }

    chatRow = await upsertChat({
      chatKey: chat ? chatKey(chat, parsed.canonical) : parsed.username.toLowerCase(),
      chatType: chat ? chatType(chat) : 'public',
      title: chatTitle(chat),
      username: chatUsername(chat) || parsed.username,
      publicLink: publicLinkFromChat(chat) || `https://t.me/${parsed.username}`,
      inviteLink: publicLinkFromChat(chat) || `https://t.me/${parsed.username}`,
      privateUid: chat ? privateUidFromChat(chat) : null,
      joinStatus: status,
      error
    });
  }
  await saveMessageLink(parsed.canonical, chatRow?.id, parsed.messageId, chatRow?.invite_link || undefined, parsed.canonical);
  await upsertInputLink(parsed, status, chatRow?.id, error);
  return {
    link: parsed.canonical,
    type: parsed.type,
    status,
    chatId: chatRow?.id,
    chatKey: chatRow?.chat_key,
    title: chatRow?.title || undefined,
    username: chatRow?.username || undefined,
    inviteLink: chatRow?.invite_link || undefined,
    extractedLinks: [],
    error
  };
}

async function processBot(client: TelegramClient, parsed: ParsedLink, args: Args): Promise<ProcessResult> {
  const extractedLinks: string[] = [];
  let status = args.startBots ? 'started' : 'stored';
  let error: string | undefined;
  let chatRow: TelegramChatEntity | undefined;
  try {
    const peer = await invokeWithTimeout(client.getInputEntity('@' + parsed.username) as Promise<Api.TypeInputPeer>, 15000, 'bot input');
    if (args.startBots && !args.dryRun) {
      const beforeHistory: any = await invokeWithTimeout(
        client.invoke(new Api.messages.GetHistory({
          peer: peer as any,
          offsetId: 0,
          addOffset: 0,
          limit: 1,
          maxId: 0,
          minId: 0,
          hash: 0 as any
        } as any)),
        15000,
        'bot pre-start history'
      ).catch(() => null);
      const beforeTopId = Number(beforeHistory?.messages?.[0]?.id || 0);

      await invokeWithTimeout(
        client.invoke(new Api.messages.SendMessage({
          peer: peer as any,
          message: parsed.startCommand || '/start',
          randomId: BigInt(Date.now()) as any,
          noWebpage: true
        } as any)),
        15000,
        'bot start'
      );

      const seenMessageIds = new Set<number>();
      const pollCount = Math.max(1, args.botPollCount || 6);
      const pollIntervalMs = Math.max(500, args.botPollIntervalMs || args.botDelayMs || 2500);
      for (let poll = 0; poll < pollCount; poll += 1) {
        await sleep(pollIntervalMs);
        const history: any = await invokeWithTimeout(
          client.invoke(new Api.messages.GetHistory({
            peer: peer as any,
            offsetId: 0,
            addOffset: 0,
            limit: args.botHistoryLimit || 20,
            maxId: 0,
            minId: 0,
            hash: 0 as any
          } as any)),
          15000,
          'bot history'
        );
        for (const msg of history?.messages || []) {
          const msgAny = msg as any;
          const messageId = Number(msgAny?.id || 0);
          if (messageId && seenMessageIds.has(messageId)) continue;
          if (beforeTopId && messageId && messageId <= beforeTopId) continue;
          if (messageId) seenMessageIds.add(messageId);

          const text = String(msgAny?.message || '');
          extractedLinks.push(
            ...extractLinks(text),
            ...extractLinksFromEntities(text, msgAny?.entities || []),
            ...extractLinksFromMarkup(msgAny?.replyMarkup),
            ...extractLinksFromMedia(msgAny?.media)
          );
        }
        if (extractedLinks.length) break;
      }
    }
    chatRow = await upsertChat({
      chatKey: parsed.username!.toLowerCase(),
      chatType: 'bot',
      username: parsed.username,
      publicLink: `https://t.me/${parsed.username}`,
      inviteLink: parsed.canonical,
      joinStatus: status
    });
    await saveMessageLinksForChat({ username: parsed.username, bot: true }, chatRow, extractedLinks, parsed.canonical);
  } catch (err) {
    error = err instanceof Error ? err.message : String(err);
    status = 'error';
  }
  await upsertInputLink(parsed, status, chatRow?.id, error);
  return {
    link: parsed.canonical,
    type: parsed.type,
    status,
    chatId: chatRow?.id,
    chatKey: chatRow?.chat_key,
    username: parsed.username,
    inviteLink: chatRow?.invite_link || parsed.canonical,
    extractedLinks: Array.from(new Set(extractedLinks.map(normalizeTelegramLink))),
    error
  };
}

async function processParsedLink(
  client: TelegramClient,
  parsed: ParsedLink,
  args: Args,
  results: ProcessResult[],
  processed: Set<string>,
  depth = 0
): Promise<void> {
  const key = parsed.canonical;
  if (processed.has(key)) return;
  processed.add(key);

  if (parsed.type === 'invite') {
    results.push(await processInvite(client, parsed, args));
    return;
  }
  if (parsed.type === 'public_chat') {
    results.push(await processPublicChat(client, parsed, args));
    return;
  }
  if (parsed.type === 'public_message' || parsed.type === 'private_message' || parsed.type === 'private_chat') {
    results.push(await processMessageLink(client, parsed, args));
    return;
  }
  if (parsed.type !== 'bot') return;

  const result = await processBot(client, parsed, args);
  results.push(result);
  if (depth >= 3) return;

  const nestedLinks = Array.from(new Set(result.extractedLinks.map(normalizeTelegramLink)));
  for (const nested of nestedLinks.map(parseLink).filter((item): item is ParsedLink => Boolean(item))) {
    await processParsedLink(client, nested, args, results, processed, depth + 1);
  }
}

async function main(): Promise<void> {
  const args = parseArgs();
  loadEnv({ path: args.env || process.env.ENV_FILE || '.env.production' });
  const input = await readInput(args);
  const parsedLinks = Array.from(new Map(
    extractLinks(input)
      .map(parseLink)
      .filter((item): item is ParsedLink => Boolean(item))
      .map((item) => [item.canonical, item])
  ).values());

  if (args.parseOnly) {
    process.stdout.write(JSON.stringify({
      generatedAt: new Date().toISOString(),
      mode: 'parseOnly',
      inputLinks: parsedLinks.length,
      links: parsedLinks
    }, null, 2) + '\n');
    return;
  }

  const apiId = Number(process.env.API_ID || 0);
  const apiHash = process.env.API_HASH || '';
  const sessionString = process.env.SESSION_STRING || '';
  if (!apiId || !apiHash || !sessionString) throw new Error('Missing API_ID/API_HASH/SESSION_STRING.');

  await AppDataSource.initialize();
  const client = new TelegramClient(new StringSession(sessionString), apiId, apiHash, { connectionRetries: 5 });
  const results: ProcessResult[] = [];
  const processedLinks = new Set<string>();
  try {
    await client.connect();
    for (const parsed of parsedLinks) {
      await processParsedLink(client, parsed, args, results, processedLinks);
    }
  } finally {
    await client.disconnect().catch(() => undefined);
    await AppDataSource.destroy().catch(() => undefined);
  }

  const payload = {
    generatedAt: new Date().toISOString(),
    dryRun: Boolean(args.dryRun),
    inputLinks: parsedLinks.length,
    results
  };
  const json = JSON.stringify(payload, null, 2);
  if (args.out) await writeFile(args.out, json, 'utf8');
  process.stdout.write(json + '\n');
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err);
  process.exit(1);
});
