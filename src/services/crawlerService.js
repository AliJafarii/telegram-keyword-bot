// src/services/crawlerService.js
const fs               = require('fs');
const path             = require('path');
const { TelegramClient, Api } = require('telegram');
const { StringSession }= require('telegram/sessions');
const logger           = require('../logger');
const ChannelMatch     = require('../models/ChannelMatch');
const { apiId, apiHash, sessionString } = require('../config');

const client = new TelegramClient(
  new StringSession(sessionString),
  Number(apiId),
  apiHash,
  {
    connectionRetries: 5,
    proxy: { ip: '127.0.0.1', port: 10808, socksType: 5 }
  }
);

async function initClient() {
  await client.start({
    phoneNumber: async () => '',
    password:    async () => '',
    phoneCode:   async () => '',
    onError:     console.error
  });
  logger.info('Crawler MTProto client started');
}

async function crawlChannel(username) {
  logger.info('Crawling channel', { username });
  const entity = await client.getEntity(`@${username}`);
  let offsetId = 0;
  while (true) {
    const hist = await client.invoke(new Api.messages.GetHistory({
      peer:       entity,
      offset_id:  offsetId,
      add_offset: 0,
      limit:      100,
      max_id:     0,
      min_id:     0,
      hash:       0
    }));
    const msgs = hist.messages;
    if (!msgs.length) break;

    for (const m of msgs) {
      if (!m.message) continue;
      await ChannelMatch.updateOne(
        { channel: username, message_id: m.id },
        {
          $set: {
            channel:    username,
            message_id: m.id,
            date:       new Date(m.date * 1000),
            text:       m.message
          }
        },
        { upsert: true }
      );
    }
    offsetId = msgs[msgs.length - 1].id;
  }
}

async function crawlAll() {
  await initClient();
  const list = JSON.parse(
    fs.readFileSync(path.join(__dirname, '../../channels.json'), 'utf8')
  );
  for (const username of list) {
    try {
      await crawlChannel(username);
    } catch (e) {
      logger.error('Error crawling channel', { username, error: e.message });
    }
  }
  logger.info('All channels crawled');
  process.exit(0);
}

module.exports = { crawlAll };