const requiredProductionKeys = [
  'TELEGRAM_BOT_TOKEN',
  'API_ID',
  'API_HASH',
  'SESSION_STRING'
];

function isBlank(value: unknown): boolean {
  return typeof value !== 'string' || value.trim().length === 0;
}

export function validateEnvironment(env: Record<string, unknown>): Record<string, unknown> {
  if (env.SKIP_ENV_VALIDATION === 'true') return env;

  const missing = requiredProductionKeys.filter((key) => isBlank(env[key]));
  const errors: string[] = [];
  if (missing.length) {
    errors.push(`missing required variables: ${missing.join(', ')}`);
  }

  const apiId = Number(env.API_ID);
  if (!isBlank(env.API_ID) && (!Number.isInteger(apiId) || apiId <= 0)) {
    errors.push('API_ID must be a positive integer');
  }

  if (errors.length) {
    const envFile = process.env.ENV_FILE || '.env.production';
    throw new Error(`Invalid production environment (${envFile}): ${errors.join('; ')}`);
  }

  return env;
}

export default () => ({
  botToken: process.env.TELEGRAM_BOT_TOKEN || '',
  apiId: Number(process.env.API_ID || 0),
  apiHash: process.env.API_HASH || '',
  sessionString: process.env.SESSION_STRING || '',
  logLevel: process.env.LOG_LEVEL || 'info',
  webPageSize: Number(process.env.WEB_PAGE_SIZE || 10),
  tgPageSize: Number(process.env.TG_PAGE_SIZE || 5),
  webPageLimit: Number(process.env.WEB_PAGE_LIMIT || 10),
  webPerPage: Number(process.env.WEB_PER_PAGE || 20),
  tgDynamicChatLimit: Number(process.env.TG_DYNAMIC_CHAT_LIMIT || 200),
  tgDynamicMsgLimit: Number(process.env.TG_DYNAMIC_MSG_LIMIT || 20),
  crawlDepth: Number(process.env.CRAWL_DEPTH || 10),
  crawlIterations: Number(process.env.CRAWL_ITERATIONS || 5),
  crawlMsgLimit: Number(process.env.CRAWL_MSG_LIMIT || 1000),
  crawlSearchPageSize: Number(process.env.CRAWL_SEARCH_PAGE_SIZE || 20),
  crawlSearchPagesPerChat: Number(process.env.CRAWL_SEARCH_PAGES_PER_CHAT || 30),
  crawlMaxRuntimeMs: Number(process.env.CRAWL_MAX_RUNTIME_MS || 300000),
  crawlAutoJoin: process.env.CRAWL_AUTO_JOIN !== 'false',
  crawlJoinPublic: process.env.CRAWL_JOIN_PUBLIC !== 'false',
  crawlStartBots: process.env.CRAWL_START_BOTS !== 'false',
  crawlLeaveJoinedPrivate: process.env.CRAWL_LEAVE_JOINED_PRIVATE === 'true',
  crawlAllowVideoCaptionWithoutLink: process.env.CRAWL_ALLOW_VIDEO_CAPTION_WITHOUT_LINK !== 'false',
  crawlPersistSteps: process.env.CRAWL_PERSIST_STEPS === 'true',
  crawlDbTimeoutMs: Number(process.env.CRAWL_DB_TIMEOUT_MS || 5000),
  dbOpTimeoutMs: Number(process.env.DB_OP_TIMEOUT_MS || 120000),
  searchCreateTimeoutMs: Number(process.env.SEARCH_CREATE_TIMEOUT_MS || 5000),
  searchCreateMaxAttempts: Number(process.env.SEARCH_CREATE_MAX_ATTEMPTS || 3),
  mtprotoInitTimeoutMs: Number(process.env.MTPROTO_INIT_TIMEOUT_MS || 30000),
  linksPageSize: Number(process.env.LINKS_PAGE_SIZE || 50),
  excelCrawlConcurrency: Number(process.env.EXCEL_CRAWL_CONCURRENCY || 2),
  crawlFuzzyEnabled: process.env.CRAWL_FUZZY_ENABLED !== 'false',
  crawlFuzzyMaxDistance: Number(process.env.CRAWL_FUZZY_MAX_DISTANCE || 1),
  crawlFuzzyMinTermLength: Number(process.env.CRAWL_FUZZY_MIN_TERM_LENGTH || 4),
  redisUrl: process.env.REDIS_URL || 'redis://127.0.0.1:6379',
  redisPrefix: process.env.REDIS_PREFIX || 'tkb',
  redisTtlSeconds: Number(process.env.REDIS_TTL_SECONDS || 604800),
  databaseType: process.env.DATABASE_TYPE || 'sqlite',
  sqliteDatabase: process.env.SQLITE_DATABASE || '/root/telegram-keyword-bot/data/telegram-keyword-bot.sqlite',
  databaseSynchronize: process.env.DATABASE_SYNCHRONIZE !== 'false',
  oracleUser: process.env.ORACLE_USER || '',
  oraclePassword: process.env.ORACLE_PASSWORD || '',
  oracleConnectString: process.env.ORACLE_CONNECT_STRING || '',
  oracleSynchronize: process.env.ORACLE_SYNCHRONIZE !== 'false'
});
