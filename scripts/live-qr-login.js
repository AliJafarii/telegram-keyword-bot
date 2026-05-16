require('dotenv').config({ path: '.env.production' });

const { writeFileSync } = require('fs');
const { TelegramClient } = require('telegram');
const { StringSession } = require('telegram/sessions');
const QRCode = require('qrcode');

function base64Url(buffer) {
  return Buffer.from(buffer)
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
}

async function writeQr(url) {
  writeFileSync('/tmp/telegram-login-url.txt', url);
  await QRCode.toFile('/tmp/telegram-login-qr.png', url, {
    errorCorrectionLevel: 'M',
    margin: 2,
    width: 512
  });
}

async function main() {
  const client = new TelegramClient(
    new StringSession(''),
    Number(process.env.API_ID),
    process.env.API_HASH,
    { connectionRetries: 2 }
  );

  try {
    await client.connect();
    console.log('CONNECTED');
    await client.signInUserWithQrCode(
      { apiId: Number(process.env.API_ID), apiHash: process.env.API_HASH },
      {
        qrCode: async ({ token, expires }) => {
          const url = 'tg://login?token=' + base64Url(token);
          await writeQr(url);
          console.log('QR_READY expires=' + expires);
          console.log('QR_URL=' + url);
        },
        onError: (error) => {
          console.log('QR_ERROR=' + (error && error.message || String(error)));
        }
      }
    );
    console.log('AUTHORIZED=' + await client.checkAuthorization());
    console.log('SESSION_STRING=' + client.session.save());
  } catch (error) {
    console.log('FATAL_NAME=' + (error && error.name || ''));
    console.log('FATAL_MESSAGE=' + (error && error.message || String(error)));
    console.log('FATAL_CODE=' + (error && error.code || ''));
    console.log('FATAL_ERROR_MESSAGE=' + (error && error.errorMessage || ''));
  } finally {
    try {
      await client.disconnect();
    } catch {
      // ignore disconnect failures
    }
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
