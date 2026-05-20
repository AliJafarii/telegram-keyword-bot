const path = require('path');
const dotenv = require('dotenv');
const sqlite3 = require('sqlite3');
const { Client } = require('pg');

dotenv.config({ path: process.env.ENV_FILE || '.env.production' });

const sqlitePath = process.env.SQLITE_DATABASE || '/root/telegram-keyword-bot/data/telegram-keyword-bot.sqlite';
const sqlite = new sqlite3.Database(sqlitePath);
const pg = new Client({
  host: process.env.POSTGRES_HOST || '127.0.0.1',
  port: Number(process.env.POSTGRES_PORT || 5432),
  database: process.env.POSTGRES_DATABASE || 'telegram_keyword_bot',
  user: process.env.POSTGRES_USER || 'telegram_keyword_bot',
  password: process.env.POSTGRES_PASSWORD || '',
  ssl: process.env.POSTGRES_SSL === 'true' ? { rejectUnauthorized: false } : false
});

const quote = (name) => '"' + name.replace(/"/g, '""') + '"';
const all = (sql) => new Promise((resolve, reject) => {
  sqlite.all(sql, (err, rows) => err ? reject(err) : resolve(rows));
});

async function copyTable(table, columns) {
  const rows = await all('SELECT ' + columns.map(quote).join(', ') + ' FROM ' + quote(table) + ' ORDER BY id');
  await pg.query('DELETE FROM ' + quote(table));
  for (const row of rows) {
    const values = columns.map((column) => row[column]);
    const placeholders = values.map((_, index) => '$' + (index + 1)).join(', ');
    await pg.query(
      'INSERT INTO ' + quote(table) + ' (' + columns.map(quote).join(', ') + ') VALUES (' + placeholders + ')',
      values
    );
  }
  if (columns.includes('id')) {
    await pg.query(
      "SELECT setval(pg_get_serial_sequence($1, 'id'), COALESCE((SELECT MAX(id) FROM " + quote(table) + "), 1), true)",
      [table]
    );
  }
  console.log(table + ': ' + rows.length);
}

async function main() {
  await pg.connect();
  await pg.query('BEGIN');
  try {
    await copyTable('users', ['id', 'telegram_id', 'username', 'created_at']);
    await copyTable('searches', ['id', 'user_id', 'keyword', 'results_web', 'results_telegram', 'RESULTS_LINKS', 'RESULTS_INVITES', 'created_at']);
    await copyTable('channel_matches', ['id', 'search_id', 'channel', 'channel_type', 'channel_link', 'message_link', 'message_id', 'date', 'match_reason', 'iteration_no', 'discovered_via_link', 'discovered_from_message_link', 'discovered_from_channel', 'text', 'LINKS', 'media_metadata', 'metadata_query']);
    await copyTable('search_links', ['id', 'search_id', 'channel_match_id', 'link', 'link_type', 'created_at']);
    await copyTable('crawl_steps', ['id', 'search_id', 'step', 'details', 'created_at']);
    await pg.query('COMMIT');
  } catch (err) {
    await pg.query('ROLLBACK');
    throw err;
  } finally {
    await pg.end();
    sqlite.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
