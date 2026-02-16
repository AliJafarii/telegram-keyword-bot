# Telegram Search Bot

## Setup
1. Start Redis with Docker:
   - `docker compose up -d redis`
2. Configure environment file with Telegram API/Bot, Oracle, and Redis values.
   - For app on host: `REDIS_URL=redis://127.0.0.1:6379`
   - For app in Docker network: `REDIS_URL=redis://redis:6379`
   - Profiles:
     - `.env.development.iran` (proxy/VPN)
     - `.env.development.global` (direct access)
     - `.env.production` (server)
   - You can copy from:
     - `.env.development.iran.example`
     - `.env.development.global.example`
     - `.env.production.example`
3. `npm install`
4. `npm run build`
5. `npm run db:cleanup` (recommended once per environment, before first run)
6. `npm start`

If schema is irreparably dirty and data can be removed:
- `npm run db:reset`
  - This drops and recreates `users`, `searches`, `channel_matches`, `crawl_steps`, `search_links`.

## Features
- `/start` to register
- `/seed <keyword> [iterations]` crawl Telegram results iteratively
- Results are paginated in Telegram
- Match cache is kept in Redis, then flushed to Oracle (`channel_matches`, `search_links`)
- Search history is stored per Telegram user in Oracle (`users` -> `searches`)
- `channel_matches.search_id` is linked to `searches.id` for keyword ownership tracking
- `/history` shows each user their own recent searches

## Environment Commands
- Global dev (non-Iran): `npm run start:dev:global`
- Iran dev (proxy/VPN): `npm run start:dev:iran`
- Production-like local run from build: `npm run start:prod:local`
- Default run (legacy `.env`): `npm start`

## Git Release (v2)
Use this when you want to freeze current code as `v2`:
1. `git add src package.json package-lock.json README.md .gitignore`
2. `git commit -m "release: v2 multi-environment setup"`
3. `git tag -a v2 -m "v2 stable"`
4. `git push origin develop --tags`

## Logging
Uses Winston; configure log level via `LOG_LEVEL`.

## Notes
- `npm start` and `npm run start:dev` already set a valid `--localstorage-file` path for GramJS (`.node-localstorage`).
- `npm run db:cleanup` is idempotent; it removes dirty legacy columns, fixes FKs/indexes, and creates `search_links` if missing.
- Keyword matching normalizes Arabic/Persian text (`وحشی`, `وح شی`, `وح.شی`, `و ح ش ی`, `وحـ شی` all match).
- Fuzzy matching is enabled by default and configurable:
  - `CRAWL_FUZZY_ENABLED=true|false`
  - `CRAWL_FUZZY_MAX_DISTANCE=1`
  - `CRAWL_FUZZY_MIN_TERM_LENGTH=4`
