// scripts/discoverChannels.js

require('dotenv').config();
const fs   = require('fs');
const path = require('path');
const { TelegramClient, Api } = require('telegram');
const { StringSession }       = require('telegram/sessions');
const { SocksProxyAgent }     = require('socks-proxy-agent');
const logger                  = require('../src/logger');
const { apiId, apiHash, sessionString } = require('../src/config');

async function discoverChannels() {
  // 1) Initialize MTProto client with your SOCKS5 proxy
  const proxyAgent = new SocksProxyAgent('socks5h://127.0.0.1:10808');
  const client = new TelegramClient(
    new StringSession(sessionString),
    Number(apiId),
    apiHash,
    {
      connectionRetries: 5,
      // route MTProto over your v2rayN SOCKS5
      proxy: { ip: '127.0.0.1', port: 10808, socksType: 5 },
      // disable raw TCP to port 80
      // useWSS: true  // optional if you prefer WebSocket on 443
    }
  );

  await client.start({
    phoneNumber: async () => '',
    password:    async () => '',
    phoneCode:   async () => '',
    onError:     console.error
  });
  logger.info('Discovery client started via SOCKS5');

  // 2) Seed queries: a-z, 0-9
  const seeds = [];
  for (let c = 97; c <= 122; c++) seeds.push(String.fromCharCode(c));
  for (let d = 48; d <= 57;  d++) seeds.push(String.fromCharCode(d));

  const usernames = new Set();

  for (const q of seeds) {
    try {
      logger.info('Searching global for seed', { q });
      const res = await client.invoke(new Api.messages.SearchGlobal({
        q,
        offsetDate:  0,
        offsetPeer:  new Api.InputPeerEmpty(),
        offsetId:    0,
        limit:       100,
        filter:      new Api.InputMessagesFilterEmpty()
      }));
      for (const chat of res.chats || []) {
        if (chat.username) usernames.add(chat.username);
      }
      // throttle between requests
      await new Promise(r => setTimeout(r, 500));
    } catch (e) {
      logger.error('Seed discovery failed', { seed: q, error: e.message });
    }
  }

  // 3) Write to channels.json
  const arr = Array.from(usernames).sort();
  fs.writeFileSync(
    path.join(__dirname, '../channels.json'),
    JSON.stringify(arr, null, 2)
  );
  logger.info('Discovered channels saved', { count: arr.length });
  process.exit(0);
}

discoverChannels().catch(err => {
  logger.error('Discovery script error', { error: err.message });
  process.exit(1);
});