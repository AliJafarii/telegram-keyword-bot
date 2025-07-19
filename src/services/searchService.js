// src/services/searchService.js

require('dotenv').config();
const axios                   = require('axios');
const cheerio                 = require('cheerio');
const { TelegramClient, Api } = require('telegram');
const { StringSession }       = require('telegram/sessions');
const logger                  = require('../logger');
const { apiId, apiHash, sessionString } = require('../config');

const client = new TelegramClient(
  new StringSession(sessionString),
  Number(apiId), apiHash,
  { connectionRetries: 5, proxy:{ ip:'127.0.0.1', port:10808, socksType:5 } }
);

async function initUserClient() {
  if (client.sessionLoaded) return;
  await client.start({
    phoneNumber: async()=>'', phoneCode: async()=>'', password: async()=>'', onError: console.error
  });
  logger.info('MTProto client started');
}

// Web: DuckDuckGo HTML scrape, paged
async function webSearch(keyword) {
  await initUserClient();
  const pages   = parseInt(process.env.WEB_PAGE_LIMIT) || 10;
  const perPage = parseInt(process.env.WEB_PER_PAGE)   || 20;
  const out     = [];
  try {
    for (let p = 0; p < pages; p++) {
      const res = await axios.post(
        'https://html.duckduckgo.com/html/',
        `q=${encodeURIComponent(keyword)}&s=${p*perPage}`,
        { headers:{
            'Content-Type':'application/x-www-form-urlencoded', 'User-Agent':'Mozilla/5.0'
          }, proxy:false }
      );
      const $ = cheerio.load(res.data);
      let count = 0;
      $('.result').each((i, el) => {
        if (count++ >= perPage) return false;
        const a = $(el).find('.result__a');
        let url = a.attr('href')||'';
        const m   = url.match(/uddg=(.+)/);
        if (m) url = decodeURIComponent(m[1]);
        out.push({ title: a.text().trim(), url, found_at: new Date() });
      });
      if (count < perPage) break;
    }
    logger.info('Web search complete', { keyword, count: out.length });
  } catch (e) {
    logger.error('Web search failed', { keyword, error: e.message });
  }
  return out;
}

// Telegram: combine name-search + message-search
async function telegramDynamicSearch(keyword) {
  await initUserClient();
  const chatLimit = parseInt(process.env.TG_DYNAMIC_CHAT_LIMIT) || 200;
  const msgLimit  = parseInt(process.env.TG_DYNAMIC_MSG_LIMIT)  || 20;

  // 1) name-based discovery
  const chatsMap = new Map();
  try {
    const nameRes = await client.invoke(new Api.contacts.Search({
      q:      keyword,
      limit:  chatLimit
    }));
    for (const c of nameRes.chats || []) {
      if (c.username) chatsMap.set(c.username, c);
    }
    logger.info('contacts.Search found', { keyword, count: chatsMap.size });
  } catch (e) {
    logger.error('contacts.Search failed', { keyword, error: e.message });
  }

  // 2) message-based discovery
  try {
    const glob = await client.invoke(new Api.messages.SearchGlobal({
      q:           keyword,
      offsetDate:  0,
      offsetPeer:  new Api.InputPeerEmpty(),
      offsetId:    0,
      limit:       chatLimit,
      filter:      new Api.InputMessagesFilterEmpty()
    }));
    for (const c of glob.chats || []) {
      if (c.username) chatsMap.set(c.username, c);
    }
    logger.info('SearchGlobal found', { keyword, count: chatsMap.size });
  } catch (e) {
    logger.error('SearchGlobal failed', { keyword, error: e.message });
  }

  // 3) For each chat, fetch actual messages
  const out = [];
  const words = keyword.trim().split(/\s+/);
  for (const chat of Array.from(chatsMap.values()).slice(0, chatLimit)) {
    let msgs = [];
    try {
      const hist = await client.invoke(new Api.messages.Search({
        peer:      chat,
        q:         keyword,
        filter:    new Api.InputMessagesFilterEmpty(),
        offsetId:  0,
        addOffset: 0,
        limit:     msgLimit,
        maxId:     0,
        minId:     0,
        hash:      0
      }));
      msgs = (hist.messages || []).filter(m => m.message);
      // multi-word filter: keep only those containing ALL words
      if (words.length > 1) {
        msgs = msgs.filter(m =>
          words.every(w => m.message.includes(w))
        );
      }
    } catch (e) {
      logger.warn('messages.Search failed', { chat: chat.username, error: e.message });
    }

    // determine category
    let category = 'bot';
    if (chat._ === 'channel') category = chat.broadcast ? 'channel' : 'group';
    else if (chat._ === 'chat')   category = 'group';

    out.push({
      category,
      username: chat.username,
      title:    chat.title || chat.username,
      messages: msgs.map(m => ({
        text:     m.message,
        url:      `https://t.me/${chat.username}/${m.id}`,
        found_at: new Date(m.date * 1000)
      }))
    });
  }

  logger.info('Telegram dynamic search complete', { keyword, count: out.length });
  return out;
}

// Combined
async function performSearch(keyword) {
  const [ web, telegram ] = await Promise.all([
    webSearch(keyword),
    telegramDynamicSearch(keyword)
  ]);
  return { web, telegram };
}

module.exports = { performSearch };