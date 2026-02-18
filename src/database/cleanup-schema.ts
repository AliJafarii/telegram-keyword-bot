import { config as loadEnv } from 'dotenv';
// eslint-disable-next-line @typescript-eslint/no-var-requires
const oracledb = require('oracledb');

loadEnv({ path: process.env.ENV_FILE || '.env.production' });

const q = (id: string) => `"${id.replace(/"/g, '""')}"`;
const IGNORABLE_ERRORS = /ORA-00955|ORA-01408|ORA-02275|ORA-02261|ORA-02260|ORA-01430|ORA-02443|ORA-01442/i;

type DbConnection = any;

function tableRefForName(tableName: string): string {
  if (/^[A-Z][A-Z0-9_$#]*$/.test(tableName)) {
    return tableName;
  }
  return q(tableName);
}

function resolveColumn(columns: string[], logicalName: string): string | undefined {
  const lower = logicalName.toLowerCase();
  return columns.find((c) => c.toLowerCase() === lower);
}

function shouldUseUpperIdentifiers(columns: string[]): boolean {
  if (!columns.length) return false;
  return columns.every((c) => c === c.toUpperCase());
}

function preferredColumnName(columns: string[], logicalName: string): string {
  return shouldUseUpperIdentifiers(columns) ? logicalName.toUpperCase() : logicalName;
}

async function listTables(conn: DbConnection, logicalName: string): Promise<string[]> {
  const r = await conn.execute(
    `SELECT TABLE_NAME
     FROM USER_TABLES
     WHERE LOWER(TABLE_NAME) = LOWER(:name)
     ORDER BY CASE
       WHEN TABLE_NAME = :exact THEN 0
       WHEN TABLE_NAME = UPPER(:exact) THEN 1
       ELSE 2
     END`,
    { name: logicalName, exact: logicalName },
    { outFormat: oracledb.OUT_FORMAT_OBJECT }
  );
  return (r.rows || []).map((row: any) => String(row.TABLE_NAME || row.table_name || '').trim()).filter(Boolean);
}

async function pickTable(conn: DbConnection, logicalName: string): Promise<string | null> {
  const tables = await listTables(conn, logicalName);
  return tables[0] || null;
}

async function tableExists(conn: DbConnection, logicalName: string): Promise<boolean> {
  return (await listTables(conn, logicalName)).length > 0;
}

async function getColumns(conn: DbConnection, tableName: string): Promise<string[]> {
  const r = await conn.execute(
    `SELECT COLUMN_NAME
     FROM USER_TAB_COLS
     WHERE TABLE_NAME = :tableName
     ORDER BY COLUMN_ID`,
    { tableName },
    { outFormat: oracledb.OUT_FORMAT_OBJECT }
  );
  return (r.rows || []).map((row: any) => String(row.COLUMN_NAME || row.column_name || '').trim()).filter(Boolean);
}

async function getColumnType(
  conn: DbConnection,
  tableName: string,
  columnName: string
): Promise<string | null> {
  const r = await conn.execute(
    `SELECT DATA_TYPE
     FROM USER_TAB_COLS
     WHERE TABLE_NAME = :tableName AND COLUMN_NAME = :columnName`,
    { tableName, columnName },
    { outFormat: oracledb.OUT_FORMAT_OBJECT }
  );
  const row = (r.rows || [])[0] as any;
  const type = row?.DATA_TYPE || row?.data_type;
  return type ? String(type) : null;
}

async function existsConstraint(conn: DbConnection, tableName: string, constraintName: string): Promise<boolean> {
  const r = await conn.execute(
    `SELECT COUNT(*) AS CNT
     FROM USER_CONSTRAINTS
     WHERE LOWER(TABLE_NAME) = LOWER(:tableName)
       AND LOWER(CONSTRAINT_NAME) = LOWER(:constraintName)`,
    { tableName, constraintName },
    { outFormat: oracledb.OUT_FORMAT_OBJECT }
  );
  return Number((r.rows?.[0] as any)?.CNT || 0) > 0;
}

async function existsIndex(conn: DbConnection, tableName: string, indexName: string): Promise<boolean> {
  const r = await conn.execute(
    `SELECT COUNT(*) AS CNT
     FROM USER_INDEXES
     WHERE LOWER(TABLE_NAME) = LOWER(:tableName)
       AND LOWER(INDEX_NAME) = LOWER(:indexName)`,
    { tableName, indexName },
    { outFormat: oracledb.OUT_FORMAT_OBJECT }
  );
  return Number((r.rows?.[0] as any)?.CNT || 0) > 0;
}

async function execSafe(conn: DbConnection, sql: string, binds: Record<string, unknown> = {}) {
  try {
    await conn.execute(sql, binds);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    if (IGNORABLE_ERRORS.test(message)) {
      return;
    }
    throw new Error(`${message} | SQL: ${sql}`);
  }
}

async function ensureColumn(
  conn: DbConnection,
  tableName: string,
  columns: string[],
  logicalName: string,
  sqlType: string
): Promise<string> {
  const existing = resolveColumn(columns, logicalName);
  if (existing) return existing;
  const newName = preferredColumnName(columns, logicalName);
  await execSafe(
    conn,
    `ALTER TABLE ${tableRefForName(tableName)} ADD (${q(newName)} ${sqlType})`
  );
  return newName;
}

async function convertClobToVarchar(
  conn: DbConnection,
  tableName: string,
  columns: string[],
  logicalName: string,
  varcharLen: number
): Promise<void> {
  const currentName = resolveColumn(columns, logicalName);
  if (!currentName) return;
  const dataType = await getColumnType(conn, tableName, currentName);
  if (!dataType || !/^CLOB$/i.test(dataType)) return;

  const tempLogical = `${logicalName}_tmp`;
  const existingTemp = resolveColumn(columns, tempLogical);
  if (existingTemp) {
    await execSafe(
      conn,
      `ALTER TABLE ${tableRefForName(tableName)} DROP COLUMN ${q(existingTemp)}`
    );
  }
  const tempName = preferredColumnName(columns, tempLogical);
  await execSafe(
    conn,
    `ALTER TABLE ${tableRefForName(tableName)} ADD (${q(tempName)} VARCHAR2(${varcharLen}))`
  );
  await execSafe(
    conn,
    `UPDATE ${tableRefForName(tableName)}
     SET ${q(tempName)} = DBMS_LOB.SUBSTR(${q(currentName)}, ${varcharLen}, 1)
     WHERE ${q(currentName)} IS NOT NULL`
  );
  await execSafe(
    conn,
    `ALTER TABLE ${tableRefForName(tableName)} DROP COLUMN ${q(currentName)}`
  );
  await execSafe(
    conn,
    `ALTER TABLE ${tableRefForName(tableName)} RENAME COLUMN ${q(tempName)} TO ${q(currentName)}`
  );
}

async function cleanup() {
  const conn = await oracledb.getConnection({
    user: process.env.ORACLE_USER,
    password: process.env.ORACLE_PASSWORD,
    connectString: process.env.ORACLE_CONNECT_STRING
  });

  try {
    await execSafe(conn, 'ALTER SESSION SET ddl_lock_timeout = 60');

    const searchesTable = await pickTable(conn, 'searches');
    const usersTable = await pickTable(conn, 'users');
    if (!searchesTable || !usersTable) {
      throw new Error('Required tables "searches" and/or "users" not found.');
    }

    const crawlTables = await listTables(conn, 'crawl_steps');
    const lowerCrawl = crawlTables.find((t) => t === 'crawl_steps');
    const upperCrawl = crawlTables.find((t) => t === 'CRAWL_STEPS');
    if (lowerCrawl && upperCrawl) {
      const lowerCols = await getColumns(conn, lowerCrawl);
      const upperCols = await getColumns(conn, upperCrawl);
      const lowerSearchId = resolveColumn(lowerCols, 'search_id');
      const lowerStep = resolveColumn(lowerCols, 'step');
      const lowerDetails = resolveColumn(lowerCols, 'details');
      const lowerCreatedAt = resolveColumn(lowerCols, 'created_at');
      const upperSearchId = resolveColumn(upperCols, 'search_id');
      const upperStep = resolveColumn(upperCols, 'step');
      const upperDetails = resolveColumn(upperCols, 'details');
      const upperCreatedAt = resolveColumn(upperCols, 'created_at');

      if (
        lowerSearchId && lowerStep && lowerDetails && lowerCreatedAt &&
        upperSearchId && upperStep && upperDetails && upperCreatedAt
      ) {
        await execSafe(
          conn,
          `INSERT INTO ${tableRefForName(lowerCrawl)} (${q(lowerSearchId)}, ${q(lowerStep)}, ${q(lowerDetails)}, ${q(lowerCreatedAt)})
           SELECT s.${q(upperSearchId)}, s.${q(upperStep)}, s.${q(upperDetails)}, s.${q(upperCreatedAt)}
           FROM ${tableRefForName(upperCrawl)} s
           WHERE NOT EXISTS (
             SELECT 1
             FROM ${tableRefForName(lowerCrawl)} t
             WHERE t.${q(lowerSearchId)} = s.${q(upperSearchId)}
               AND t.${q(lowerStep)} = s.${q(upperStep)}
               AND t.${q(lowerCreatedAt)} = s.${q(upperCreatedAt)}
           )`
        );
      }
      await execSafe(conn, `DROP TABLE ${tableRefForName(upperCrawl)} PURGE`);
    }

    const channelMatchesTable = await pickTable(conn, 'channel_matches');
    if (!channelMatchesTable) {
      throw new Error('Required table "channel_matches" not found.');
    }

    const allowedChannelCols = new Set([
      'id',
      'channel',
      'message_id',
      'date',
      'match_reason',
      'iteration_no',
      'discovered_via_link',
      'discovered_from_message_link',
      'discovered_from_channel',
      'text',
      'links',
      'search_ref',
      'search_id',
      'channel_type',
      'channel_link',
      'message_link'
    ]);

    let channelCols = await getColumns(conn, channelMatchesTable);
    for (const col of channelCols) {
      if (!allowedChannelCols.has(col.toLowerCase())) {
        await execSafe(
          conn,
          `ALTER TABLE ${tableRefForName(channelMatchesTable)} DROP COLUMN ${q(col)}`
        );
      }
    }

    channelCols = await getColumns(conn, channelMatchesTable);
    await convertClobToVarchar(conn, channelMatchesTable, channelCols, 'links', 4000);
    channelCols = await getColumns(conn, channelMatchesTable);

    await ensureColumn(conn, channelMatchesTable, channelCols, 'search_id', 'NUMBER');
    channelCols = await getColumns(conn, channelMatchesTable);
    await ensureColumn(conn, channelMatchesTable, channelCols, 'channel_type', 'VARCHAR2(16)');
    channelCols = await getColumns(conn, channelMatchesTable);
    await ensureColumn(conn, channelMatchesTable, channelCols, 'channel_link', 'VARCHAR2(512)');
    channelCols = await getColumns(conn, channelMatchesTable);
    await ensureColumn(conn, channelMatchesTable, channelCols, 'message_link', 'VARCHAR2(512)');
    channelCols = await getColumns(conn, channelMatchesTable);
    await ensureColumn(conn, channelMatchesTable, channelCols, 'match_reason', 'VARCHAR2(32)');
    channelCols = await getColumns(conn, channelMatchesTable);
    await ensureColumn(conn, channelMatchesTable, channelCols, 'iteration_no', 'NUMBER');
    channelCols = await getColumns(conn, channelMatchesTable);
    await ensureColumn(conn, channelMatchesTable, channelCols, 'discovered_via_link', 'VARCHAR2(512)');
    channelCols = await getColumns(conn, channelMatchesTable);
    await ensureColumn(conn, channelMatchesTable, channelCols, 'discovered_from_message_link', 'VARCHAR2(512)');
    channelCols = await getColumns(conn, channelMatchesTable);
    await ensureColumn(conn, channelMatchesTable, channelCols, 'discovered_from_channel', 'VARCHAR2(128)');
    channelCols = await getColumns(conn, channelMatchesTable);

    const cmSearchId = resolveColumn(channelCols, 'search_id');
    const cmSearchRef = resolveColumn(channelCols, 'search_ref');
    const cmChannel = resolveColumn(channelCols, 'channel');
    const cmMessageId = resolveColumn(channelCols, 'message_id');
    const cmId = resolveColumn(channelCols, 'id');
    if (!cmSearchId || !cmChannel || !cmMessageId || !cmId) {
      throw new Error('channel_matches is missing required columns after cleanup.');
    }

    if (cmSearchRef) {
      await execSafe(
        conn,
        `UPDATE ${tableRefForName(channelMatchesTable)}
         SET ${q(cmSearchId)} = TO_NUMBER(${q(cmSearchRef)})
         WHERE ${q(cmSearchId)} IS NULL
           AND REGEXP_LIKE(${q(cmSearchRef)}, '^[0-9]+$')`
      );
    }

    const searchesCols = await getColumns(conn, searchesTable);
    const usersCols = await getColumns(conn, usersTable);
    const searchesId = resolveColumn(searchesCols, 'id');
    const searchesUserId = resolveColumn(searchesCols, 'user_id');
    const usersId = resolveColumn(usersCols, 'id');
    if (!searchesId || !searchesUserId || !usersId) {
      throw new Error('searches/users tables are missing required columns.');
    }

    await execSafe(
      conn,
      `UPDATE ${tableRefForName(channelMatchesTable)} cm
       SET cm.${q(cmSearchId)} = NULL
       WHERE cm.${q(cmSearchId)} IS NOT NULL
         AND NOT EXISTS (
           SELECT 1
           FROM ${tableRefForName(searchesTable)} s
           WHERE s.${q(searchesId)} = cm.${q(cmSearchId)}
         )`
    );

    if (await existsConstraint(conn, channelMatchesTable, 'UQ_CHANNEL_MSG')) {
      await execSafe(
        conn,
        `ALTER TABLE ${tableRefForName(channelMatchesTable)} DROP CONSTRAINT ${q('UQ_CHANNEL_MSG')}`
      );
    }
    if (!(await existsConstraint(conn, channelMatchesTable, 'UQ_CM_SEARCH_CHANNEL_MSG'))) {
      await execSafe(
        conn,
        `ALTER TABLE ${tableRefForName(channelMatchesTable)}
         ADD CONSTRAINT ${q('UQ_CM_SEARCH_CHANNEL_MSG')}
         UNIQUE (${q(cmSearchId)}, ${q(cmChannel)}, ${q(cmMessageId)})`
      );
    }

    if (!(await existsConstraint(conn, channelMatchesTable, 'FK_CM_SEARCH'))) {
      await execSafe(
        conn,
        `ALTER TABLE ${tableRefForName(channelMatchesTable)}
         ADD CONSTRAINT ${q('FK_CM_SEARCH')}
         FOREIGN KEY (${q(cmSearchId)})
         REFERENCES ${tableRefForName(searchesTable)} (${q(searchesId)})
         ON DELETE CASCADE`
      );
    }

    if (!(await existsIndex(conn, channelMatchesTable, 'IDX_CM_SEARCH_ID'))) {
      await execSafe(
        conn,
        `CREATE INDEX ${q('IDX_CM_SEARCH_ID')}
         ON ${tableRefForName(channelMatchesTable)} (${q(cmSearchId)})`
      );
    }

    await execSafe(
      conn,
      `DELETE FROM ${tableRefForName(searchesTable)} s
       WHERE NOT EXISTS (
         SELECT 1
         FROM ${tableRefForName(usersTable)} u
         WHERE u.${q(usersId)} = s.${q(searchesUserId)}
       )`
    );

    if (!(await existsConstraint(conn, searchesTable, 'FK_SEARCHES_USER'))) {
      await execSafe(
        conn,
        `ALTER TABLE ${tableRefForName(searchesTable)}
         ADD CONSTRAINT ${q('FK_SEARCHES_USER')}
         FOREIGN KEY (${q(searchesUserId)})
         REFERENCES ${tableRefForName(usersTable)} (${q(usersId)})
         ON DELETE CASCADE`
      );
    }

    let searchLinksTable = await pickTable(conn, 'search_links');
    if (!searchLinksTable) {
      await execSafe(
        conn,
        `CREATE TABLE ${q('search_links')} (
          ${q('id')} NUMBER GENERATED BY DEFAULT AS IDENTITY,
          ${q('search_id')} NUMBER NOT NULL,
          ${q('channel_match_id')} NUMBER NULL,
          ${q('link')} VARCHAR2(512) NOT NULL,
          ${q('link_type')} VARCHAR2(32) NOT NULL,
          ${q('created_at')} TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
          CONSTRAINT ${q('PK_SEARCH_LINKS')} PRIMARY KEY (${q('id')})
        )`
      );
      searchLinksTable = 'search_links';
    }
    if (!searchLinksTable) {
      throw new Error('Unable to create/find search_links table.');
    }

    let slCols = await getColumns(conn, searchLinksTable);
    await ensureColumn(conn, searchLinksTable, slCols, 'search_id', 'NUMBER');
    slCols = await getColumns(conn, searchLinksTable);
    await ensureColumn(conn, searchLinksTable, slCols, 'channel_match_id', 'NUMBER');
    slCols = await getColumns(conn, searchLinksTable);
    await ensureColumn(conn, searchLinksTable, slCols, 'link', 'VARCHAR2(512)');
    slCols = await getColumns(conn, searchLinksTable);
    await ensureColumn(conn, searchLinksTable, slCols, 'link_type', 'VARCHAR2(32)');
    slCols = await getColumns(conn, searchLinksTable);
    await ensureColumn(conn, searchLinksTable, slCols, 'created_at', 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP');
    slCols = await getColumns(conn, searchLinksTable);

    const slSearchId = resolveColumn(slCols, 'search_id');
    const slMatchId = resolveColumn(slCols, 'channel_match_id');
    const slLink = resolveColumn(slCols, 'link');
    if (!slSearchId || !slMatchId || !slLink) {
      throw new Error('search_links is missing required columns after cleanup.');
    }

    await execSafe(
      conn,
      `DELETE FROM ${tableRefForName(searchLinksTable)} sl
       WHERE NOT EXISTS (
         SELECT 1
         FROM ${tableRefForName(searchesTable)} s
         WHERE s.${q(searchesId)} = sl.${q(slSearchId)}
       )`
    );

    await execSafe(
      conn,
      `UPDATE ${tableRefForName(searchLinksTable)} sl
       SET sl.${q(slMatchId)} = NULL
       WHERE sl.${q(slMatchId)} IS NOT NULL
         AND NOT EXISTS (
           SELECT 1
           FROM ${tableRefForName(channelMatchesTable)} cm
           WHERE cm.${q(cmId)} = sl.${q(slMatchId)}
         )`
    );

    if (!(await existsConstraint(conn, searchLinksTable, 'FK_SEARCH_LINKS_SEARCH'))) {
      await execSafe(
        conn,
        `ALTER TABLE ${tableRefForName(searchLinksTable)}
         ADD CONSTRAINT ${q('FK_SEARCH_LINKS_SEARCH')}
         FOREIGN KEY (${q(slSearchId)})
         REFERENCES ${tableRefForName(searchesTable)} (${q(searchesId)})
         ON DELETE CASCADE`
      );
    }

    if (!(await existsConstraint(conn, searchLinksTable, 'FK_SEARCH_LINKS_MATCH'))) {
      await execSafe(
        conn,
        `ALTER TABLE ${tableRefForName(searchLinksTable)}
         ADD CONSTRAINT ${q('FK_SEARCH_LINKS_MATCH')}
         FOREIGN KEY (${q(slMatchId)})
         REFERENCES ${tableRefForName(channelMatchesTable)} (${q(cmId)})
         ON DELETE CASCADE`
      );
    }

    if (!(await existsConstraint(conn, searchLinksTable, 'UQ_SEARCH_LINKS_SEARCH_LINK'))) {
      await execSafe(
        conn,
        `ALTER TABLE ${tableRefForName(searchLinksTable)}
         ADD CONSTRAINT ${q('UQ_SEARCH_LINKS_SEARCH_LINK')}
         UNIQUE (${q(slSearchId)}, ${q(slLink)})`
      );
    }

    if (!(await existsIndex(conn, searchLinksTable, 'IDX_SEARCH_LINKS_SEARCH'))) {
      await execSafe(
        conn,
        `CREATE INDEX ${q('IDX_SEARCH_LINKS_SEARCH')}
         ON ${tableRefForName(searchLinksTable)} (${q(slSearchId)})`
      );
    }

    if (!(await existsIndex(conn, searchLinksTable, 'IDX_SEARCH_LINKS_MATCH'))) {
      await execSafe(
        conn,
        `CREATE INDEX ${q('IDX_SEARCH_LINKS_MATCH')}
         ON ${tableRefForName(searchLinksTable)} (${q(slMatchId)})`
      );
    }

    await conn.commit();
    // eslint-disable-next-line no-console
    console.log('Schema cleanup completed successfully.');
  } finally {
    await conn.close();
  }
}

cleanup().catch((err) => {
  const message = err instanceof Error ? err.message : String(err);
  // eslint-disable-next-line no-console
  console.error('Schema cleanup failed:', err);
  if (/ORA-00054/i.test(message)) {
    // eslint-disable-next-line no-console
    console.error('Hint: another Oracle session currently holds locks on keywordBot tables. Stop writers and rerun `npm run db:cleanup`.');
  }
  process.exit(1);
});
