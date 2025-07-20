// src/bot.js
require('dotenv').config();
const { Telegraf, Markup } = require('telegraf');
const logger = require('./logger');
const { performSearch } = require('./services/searchService');
const User   = require('./models/User');
const Search = require('./models/Search');
require('./db');
const { botToken } = require('./config');

const WEB_PAGE_SIZE   = parseInt(process.env.WEB_PAGE_SIZE) || 10;
const TG_PAGE_SIZE    = parseInt(process.env.TG_PAGE_SIZE)  || 5;
const MAX_MESSAGE_LEN = 4000;

const bot = new Telegraf(botToken);

// Helper to split long text
async function sendInChunks(chatId, text, opts = {}) {
  let start = 0;
  while (start < text.length) {
    const chunk = text.slice(start, start + MAX_MESSAGE_LEN);
    await bot.telegram.sendMessage(chatId, chunk, opts);
    start += MAX_MESSAGE_LEN;
  }
}

// Unified pagination sender
async function sendPage(searchId, source, pageIdx, chatId, messageId) {
  const doc      = await Search.findById(searchId);
  const items    = doc.results[source] || [];
  const pageSize = source === 'web' ? WEB_PAGE_SIZE : TG_PAGE_SIZE;
  const total    = Math.ceil(items.length / pageSize);
  if (pageIdx < 0 || pageIdx >= total) return;

  const slice = items.slice(pageIdx * pageSize, (pageIdx + 1) * pageSize);
  let text = source === 'web'
    ? `🌐 Web Results (Page ${pageIdx + 1}/${total}):\n\n`
    : `📱 Telegram Results (Page ${pageIdx + 1}/${total}):\n\n`;

  slice.forEach((r, i) => {
    const idx = pageIdx * pageSize + i + 1;
    if (source === 'web') {
      text += `${idx}. ${r.title}\n${r.url}\n\n`;
    } else {
      text += `${idx}. [${r.category.toUpperCase()}] https://t.me/${r.username}\n\n`;
    }
  });

  const buttons = [];
  if (pageIdx > 0)       buttons.push({ text: '◀️ Prev', callback_data: `pg:${searchId}:${source}:${pageIdx - 1}` });
  if (pageIdx < total-1) buttons.push({ text: 'Next ▶️', callback_data: `pg:${searchId}:${source}:${pageIdx + 1}` });

  const opts = {
    reply_markup: { inline_keyboard: [buttons] }
  };

  if (messageId) {
    await bot.telegram.editMessageText(chatId, messageId, undefined, text, opts);
  } else {
    await sendInChunks(chatId, text, opts);
  }
}

// /start & user upsert (unchanged from before)
bot.start(async ctx => {
  const { id, first_name, last_name, username, language_code } = ctx.from;
  const tgId = String(id);
  try {
    await User.findOneAndUpdate(
      { telegram_id: tgId },
      { telegram_id: tgId, first_name, last_name, username, language_code },
      { upsert: true, setDefaultsOnInsert: true }
    );
    await ctx.reply('👋 Welcome! Use /search <keyword> to begin.');
  } catch (err) {
    logger.error('Failed on /start', { error: err.message });
    ctx.reply('⚠️ Something went wrong.');
  }
});

// /search command
bot.command('search', async ctx => {
  const tgId    = String(ctx.from.id);
  const keyword = ctx.message.text.split(' ').slice(1).join(' ').trim();
  if (!keyword) return ctx.reply('❗ Usage: /search <keyword>');

  const user = await User.findOne({ telegram_id: tgId });
  if (!user) return ctx.reply('❗ Please /start first.');

  await ctx.reply(`🔍 Searching for “${keyword}”…`);
  try {
    const { web, telegram } = await performSearch(keyword);
    const doc = await Search.create({ user_id: user._id, keyword, results: { web, telegram } });

    if (web.length)      await sendPage(doc._id, 'web',      0, ctx.chat.id);
    else                 ctx.reply('🌐 No web results found.');

    if (telegram.length) await sendPage(doc._id, 'telegram', 0, ctx.chat.id);
    else                 ctx.reply('📱 No Telegram results found.');

  } catch (err) {
    logger.error('Search failed', { error: err.message });
    ctx.reply('⚠️ Search failed.');
  }
});

// pagination callback
bot.on('callback_query', async ctx => {
  const [ , searchId, source, page ] = ctx.callbackQuery.data.split(':');
  await ctx.answerCbQuery();
  await sendPage(searchId, source, parseInt(page, 10), ctx.chat.id, ctx.callbackQuery.message.message_id);
});

bot.launch()
   .then(() => logger.info('Bot launched'))
   .catch(err => logger.error('Bot launch failed', { error: err.message }));