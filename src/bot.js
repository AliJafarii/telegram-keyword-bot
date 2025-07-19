// src/bot.js

require('dotenv').config();
const { Telegraf }        = require('telegraf');
const { SocksProxyAgent } = require('socks-proxy-agent');
const logger              = require('./logger');
const { performSearch }   = require('./services/searchService');
const User                = require('./models/User');
const Search              = require('./models/Search');
require('./db');

const { botToken }      = require('./config');
const WEB_PAGE_SIZE     = parseInt(process.env.WEB_PAGE_SIZE)  || 10;
const TG_PAGE_SIZE      = parseInt(process.env.TG_PAGE_SIZE)   || 5;
const MAX_MESSAGE_LEN   = 4000;

const agent = new SocksProxyAgent('socks5h://127.0.0.1:10808');
const bot   = new Telegraf(botToken, { telegram: { agent } });

// split long messages
async function sendInChunks(chatId, text, opts={}) {
  for (let i=0; i<text.length; i+=MAX_MESSAGE_LEN) {
    await bot.telegram.sendMessage(chatId, text.substring(i, i+MAX_MESSAGE_LEN), opts);
  }
}

// safe slice by codepoint
function safeSlice(str, n) {
  return Array.from(str).slice(0, n).join('');
}

// pagination sender
async function sendPage(searchId, source, pageIdx, chatId, messageId) {
  const doc = await Search.findById(searchId);
  if (!doc) return;
  const items = doc.results[source];
  const pageSize = source==='web'?WEB_PAGE_SIZE:TG_PAGE_SIZE;
  const totalPages = Math.ceil(items.length/pageSize);
  if (pageIdx<0||pageIdx>=totalPages) return;

  const slice = items.slice(pageIdx*pageSize,(pageIdx+1)*pageSize);
  let text = source==='web'
    ? `🌐 Web Results (Page ${pageIdx+1}/${totalPages}):\n\n`
    : `🤖 Telegram Results (Page ${pageIdx+1}/${totalPages}):\n\n`;

  if (source==='web') {
    text += slice.map((r,i)=>`${pageIdx*pageSize+i+1}. ${r.title}\n${r.url}`).join('\n\n');
  } else {
    for (const chatEntry of slice) {
      text += `[${chatEntry.category.toUpperCase()}] @${chatEntry.username}\n`;
      text += `Title: ${chatEntry.title}\n`;
      if (chatEntry.messages.length) {
        for (const m of chatEntry.messages) {
          text += `• ${safeSlice(m.text,40)}…\n  ${m.url}\n`;
        }
      } else {
        text += `🔗 https://t.me/${chatEntry.username}\n`;
      }
      text += '\n';
    }
  }

  const prevBtn = pageIdx>0
    ? { text:'◀️ Prev', callback_data:`pg:${searchId}:${source}:${pageIdx-1}` }
    : { text:' ', callback_data:'nop' };
  const nextBtn = pageIdx<totalPages-1
    ? { text:'Next ▶️', callback_data:`pg:${searchId}:${source}:${pageIdx+1}` }
    : { text:' ', callback_data:'nop' };

  const opts = { reply_markup:{ inline_keyboard:[[prevBtn,nextBtn]] } };

  if (messageId) {
    await bot.telegram.editMessageText(chatId, messageId, undefined, text, opts);
  } else if (text.length <= MAX_MESSAGE_LEN) {
    await bot.telegram.sendMessage(chatId, text, opts);
  } else {
    await sendInChunks(chatId, text);
  }
}

bot.start(ctx => ctx.reply('👋 Welcome! /search <keyword>'));

bot.command('search', async ctx => {
  const tgId    = String(ctx.from.id);
  const keyword = ctx.message.text.split(' ').slice(1).join(' ').trim();
  if (!keyword) return ctx.reply('❗ Usage: /search <keyword>');

  const user = await User.findOne({ telegram_id: tgId });
  if (!user) return ctx.reply('❗ Please /start first.');

  await ctx.reply(`🔎 Searching for “${keyword}”…`);

  try {
    const { web, telegram } = await performSearch(keyword);
    const doc = await Search.create({ user_id:user._id, keyword, results:{web,telegram} });

    if (!web.length && !telegram.length) {
      return ctx.reply('😕 No results found.');
    }

    if (web.length)      await sendPage(doc._id, 'web', 0, ctx.chat.id);
    if (telegram.length) await sendPage(doc._id, 'telegram', 0, ctx.chat.id);
  } catch (err) {
    logger.error('Search failed', { error: err.message });
    await ctx.reply('⚠️ Search failed.');
  }
});

bot.on('callback_query', async ctx => {
  const d = ctx.callbackQuery.data;
  if (!d?.startsWith('pg:')) return ctx.answerCbQuery();
  await ctx.answerCbQuery();
  const [, searchId, source, pageStr] = d.split(':');
  const pageIdx = parseInt(pageStr, 10);
  const chatId  = ctx.callbackQuery.message.chat.id;
  const msgId   = ctx.callbackQuery.message.message_id;
  await sendPage(searchId, source, pageIdx, chatId, msgId);
});

bot.launch().then(()=>logger.info('Bot launched'));