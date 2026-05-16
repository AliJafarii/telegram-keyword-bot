require('dotenv').config({ path: '.env.production' });

const { TelegramClient, Api } = require('telegram');
const { StringSession } = require('telegram/sessions');
const readline = require('readline');

function ask(question) {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  return new Promise((resolve) => {
    rl.question(question, (answer) => {
      rl.close();
      resolve(answer.trim());
    });
  });
}

async function main() {
  const phone = process.env.LOGIN_PHONE || '';
  if (!phone) throw new Error('LOGIN_PHONE is required');

  const client = new TelegramClient(
    new StringSession(''),
    Number(process.env.API_ID),
    process.env.API_HASH,
    { connectionRetries: 2 }
  );

  try {
    await client.connect();
    console.log('CONNECTED');
    const sent = await client.sendCode(
      { apiId: Number(process.env.API_ID), apiHash: process.env.API_HASH },
      phone
    );
    console.log('CODE_SENT via_app=' + sent.isCodeViaApp);
    const code = await ask('CODE: ');
    const result = await client.invoke(new Api.auth.SignIn({
      phoneNumber: phone,
      phoneCodeHash: sent.phoneCodeHash,
      phoneCode: code
    }));
    console.log('SIGNED_IN=' + Boolean(result));
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
