// create_session.js
require('dotenv').config();
const { TelegramClient } = require('telegram');
const { StringSession }  = require('telegram/sessions');
const input = require('prompt-sync')({ sigint: true });

const apiId   = Number(process.env.API_ID);
const apiHash = process.env.API_HASH;
const stringSession = new StringSession('');

(async () => {
  const client = new TelegramClient(stringSession, apiId, apiHash, {
    connectionRetries: 5,

    // ← remove useWSS when proxying
    proxy: {
      ip: '127.0.0.1',    // your v2rayN SOCKS5 listen address
      port: 10808,        // your actual port
      socksType: 5,       // SOCKS5
      // login: '',        // if you set username/password in v2rayN
      // password: ''
    }
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