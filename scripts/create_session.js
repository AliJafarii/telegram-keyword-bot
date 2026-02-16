// create_session.js
require('dotenv').config();
const { TelegramClient } = require('telegram');
const { StringSession }  = require('telegram/sessions');
const input = require('prompt-sync')({ sigint: true });

const apiId   = Number(process.env.API_ID);
const apiHash = process.env.API_HASH;
const stringSession = new StringSession('');

(async () => {
  const proxyUrl = process.env.SOCKS_PROXY;
  const proxy = proxyUrl
    ? { ip: '127.0.0.1', port: 10808, socksType: 5 }
    : undefined;

  const client = new TelegramClient(stringSession, apiId, apiHash, {
    connectionRetries: 5,
    ...(proxy ? { proxy } : {})
  });

  await client.start({
    phoneNumber: async () => input('Phone (+countrycode): '),
    phoneCode:   async () => input('Code you received: '),
    password:    async () => input('2FA password (if any): '),
    onError:     err => console.error(err),
  });

  console.log('\n✅ Logged in successfully!');
  console.log('SESSION_STRING=' + client.session.save());
  process.exit(0);
})();
