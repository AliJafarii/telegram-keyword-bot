import { config as loadEnv } from 'dotenv';
import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions';

type SocksProxyConfig = { ip: string; port: number; socksType: 4 | 5 };

function parseProxy(proxyUrl: string): SocksProxyConfig | undefined {
  const raw = proxyUrl.trim();
  if (!raw) return undefined;
  const parsed = new URL(raw);
  const proto = parsed.protocol.toLowerCase();
  if (proto !== 'socks5:' && proto !== 'socks4:') {
    throw new Error(`Unsupported proxy protocol: ${parsed.protocol}. Use socks5:// or socks4://`);
  }
  const port = Number(parsed.port || (proto === 'socks5:' ? 1080 : 1080));
  if (!port || Number.isNaN(port)) {
    throw new Error(`Invalid proxy port in SOCKS_PROXY: ${proxyUrl}`);
  }
  return {
    ip: parsed.hostname,
    port,
    socksType: proto === 'socks4:' ? 4 : 5
  };
}

async function main() {
  loadEnv({ path: process.env.ENV_FILE || '.env.production' });

  const apiId = Number(process.env.API_ID || 0);
  const apiHash = process.env.API_HASH || '';
  if (!apiId || !apiHash) {
    throw new Error('Missing API_ID / API_HASH in environment.');
  }

  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const promptSync = require('prompt-sync');
  const prompt = promptSync({ sigint: true });
  const proxy = parseProxy(process.env.SOCKS_PROXY || '');

  const client = new TelegramClient(
    new StringSession(''),
    apiId,
    apiHash,
    {
      connectionRetries: 5,
      ...(proxy ? { proxy } : {})
    }
  );

  try {
    await client.start({
      phoneNumber: async () => prompt('Phone (+countrycode...): ').trim(),
      phoneCode: async () => prompt('Code: ').trim(),
      password: async () => prompt.hide('2FA Password (if enabled): ').trim(),
      onError: (err) => {
        // eslint-disable-next-line no-console
        console.error('Telegram auth error:', err);
      }
    });
    const sessionString = client.session.save();
    // eslint-disable-next-line no-console
    console.log('\nSESSION_STRING=' + sessionString);
    // eslint-disable-next-line no-console
    console.log('Copy this value into your env file.');
  } finally {
    await client.disconnect();
  }
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('Session generation failed:', err instanceof Error ? err.message : String(err));
  process.exit(1);
});
