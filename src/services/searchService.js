// src/services/searchService.js
require('dotenv').config();
const axios   = require('axios');
const cheerio = require('cheerio');
const { TelegramClient, Api } = require('telegram');
const { StringSession }       = require('telegram/sessions');
const logger  = require('../logger');
const { apiId, apiHash, sessionString } = require('../config');

const client = new TelegramClient(
  new StringSession(sessionString),
  Number(apiId),
  apiHash,
  { connectionRetries: 5 }
);

async function initClient() {
  if (!client.session.userId) {
    await client.start();
  }
  return client;
}

/**
 * Follow redirects (up to 5) and unwrap DuckDuckGo uddg= wrappers.
 */
async function resolveUrl(rawUrl) {
  // First try a GET to follow redirects
  try {
    const resp = await axios.get(rawUrl, {
      maxRedirects: 5,
      validateStatus: null,      // accept 3xx so we can read final URL
      timeout: 5000
    });
    // Axios on Node exposes the final URL here:
    const finalUrl = resp.request.res.responseUrl;
    if (finalUrl) return finalUrl;
  } catch (err) {
    // ignore and fall through to wrapper fallback
  }

  // Fallback: unwrap DuckDuckGo uddg parameter if present
  try {
    const m = rawUrl.match(/[?&]uddg=([^&]+)/);
    if (m && m[1]) {
      return decodeURIComponent(m[1]);
    }
  } catch (_) {}

  // Last resort: return the original
  return rawUrl;
}

/**
 * 1) DuckDuckGo Lite web search, exact-phrase, paginated,
 *    then resolve each link to its final landing URL.
 */
async function webSearch(keyword) {
  const globalLimit = parseInt(process.env.WEB_GLOBAL_LIMIT) || 50;
  const perPage     = parseInt(process.env.WEB_PER_PAGE)      || 10;
  const tempResults = [];
  const finalResults = [];

  logger.info(`webSearch("${keyword}") starting (limit=${globalLimit}, perPage=${perPage})`);
  try {
    for (let offset = 0; offset < globalLimit; offset += perPage) {
      const url = `https://lite.duckduckgo.com/lite/?q=${encodeURIComponent(`"${keyword}"`)}&s=${offset}`;
      logger.debug(`Fetching DuckDuckGo Lite page: ${url}`);

      const res = await axios.get(url, {
        headers: { 'User-Agent': 'Mozilla/5.0' },
        timeout: 5000
      });
      const $ = cheerio.load(res.data);
      const links = $('a.result-link');

      logger.debug(`Found ${links.length} links on this page`);
      if (!links.length) {
        logger.warn(`No links at offset=${offset}; stopping pagination`);
        break;
      }

      links.each((i, el) => {
        if (tempResults.length >= globalLimit) return false;
        const title = $(el).text().trim();
        const link  = $(el).attr('href');
        if (title && link) {
          tempResults.push({ title, rawUrl: link });
        }
      });

      if (links.length < perPage) {
        logger.info(`Last page had <${perPage} links; ending pagination`);
        break;
      }
    }

    // Now resolve each rawUrl to final URL
    for (const { title, rawUrl } of tempResults) {
      const url = await resolveUrl(rawUrl);
      finalResults.push({ title, url });
    }

    logger.info(`webSearch("${keyword}") completed with ${finalResults.length} results`);
  } catch (err) {
    logger.error('Web search error', { error: err.message });
  }

  if (finalResults.length === 0) {
    logger.warn(`webSearch("${keyword}") returned NO results`);
  }
  return finalResults;
}

/**
 * 2) Telegram search: metadata + global message search.
 */
async function telegramDynamicSearch(keyword) {
  const client    = await initClient();
  const chatLimit = parseInt(process.env.TG_GLOBAL_LIMIT) || 100;
  const chatMap   = new Map();

  // a) metadata search
  try {
    const meta = await client.invoke(
      new Api.contacts.Search({ q: keyword, limit: chatLimit })
    );
    logger.info(`contacts.Search found ${meta.chats.length} chats for "${keyword}"`);
    for (const chat of meta.chats) {
      if (!chat.username) continue;
      const category = chat.broadcast
        ? 'channel'
        : chat.megagroup
          ? 'group'
          : chat.bot
            ? 'bot'
            : 'channel';
      chatMap.set(chat.id, {
        username: chat.username,
        title:    chat.title || chat.username,
        category,
        messages: []
      });
    }
  } catch (err) {
    logger.error('Telegram metadata search error', { error: err.message });
  }

  // b) content search
  try {
    const global = await client.invoke(
      new Api.messages.SearchGlobal({
        q:          keyword,
        filter:     new Api.InputMessagesFilterEmpty(),
        offsetRate: 0,
        offsetPeer: new Api.InputPeerEmpty(),
        offsetId:   0,
        offsetDate: 0,
        limit:      chatLimit
      })
    );
    logger.info(`messages.SearchGlobal found ${global.messages.length} messages and ${global.chats.length} chats`);

    // ensure all chats are registered
    for (const chat of global.chats) {
      if (!chat.username) continue;
      if (!chatMap.has(chat.id)) {
        const category = chat.broadcast
          ? 'channel'
          : chat.megagroup
            ? 'group'
            : chat.bot
              ? 'bot'
              : 'channel';
        chatMap.set(chat.id, {
          username: chat.username,
          title:    chat.title || chat.username,
          category,
          messages: []
        });
      }
    }

    // record first-found timestamp
    for (const msg of global.messages) {
      const peer   = msg.peerId;
      const chatId = peer.channelId ?? peer.chatId ?? peer.userId;
      const entry  = chatMap.get(chatId);
      if (entry && entry.messages.length === 0) {
        entry.messages.push({ found_at: msg.date });
      }
    }
  } catch (err) {
    logger.error('Telegram content search error', { error: err.message });
  }

  const out = Array.from(chatMap.values());
  logger.info(`telegramDynamicSearch("${keyword}") returning ${out.length} unique chats`);
  return out;
}

/**
 * Combined search
 */
async function performSearch(keyword) {
  const [web, telegram] = await Promise.all([
    webSearch(keyword),
    telegramDynamicSearch(keyword)
  ]);
  return { web, telegram };
}

module.exports = { performSearch };