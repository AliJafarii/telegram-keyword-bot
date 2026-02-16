# Telegram Search Bot

## Setup
1. Start Redis with Docker:
   - `docker compose up -d redis`
2. Configure environment files with Telegram API/Bot, Oracle, and Redis values.
   - For app on host: `REDIS_URL=redis://127.0.0.1:6379`
   - For app in Docker network: `REDIS_URL=redis://redis:6379`
   - Two profiles:
     - `.env.development` (uses proxy)
     - `.env.production` (no proxy)
   - You can copy from:
     - `.env.development.example`
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
- Development (proxy): `npm run start:dev`
- Production (no proxy): `npm run start` or `npm run start:prod`

## Logging
Uses Winston; configure log level via `LOG_LEVEL`.

## Notes
- `npm start` and `npm run start:dev` already set a valid `--localstorage-file` path for GramJS (`.node-localstorage`).
- `start:dev` always reads `.env.development`.
- `start` and `start:prod` always read `.env.production`.
- `npm run db:cleanup` is idempotent; it removes dirty legacy columns, fixes FKs/indexes, and creates `search_links` if missing.
- Keyword matching normalizes Arabic/Persian text (`وحشی`, `وح شی`, `وح.شی`, `و ح ش ی`, `وحـ شی` all match).
- Fuzzy matching is enabled by default and configurable:
  - `CRAWL_FUZZY_ENABLED=true|false`
  - `CRAWL_FUZZY_MAX_DISTANCE=1`
  - `CRAWL_FUZZY_MIN_TERM_LENGTH=4`
- Messages are stored when:
  - keyword + at least one Telegram link, or
  - keyword in caption of a video/clip (even with no link)
  - configurable by `CRAWL_ALLOW_VIDEO_CAPTION_WITHOUT_LINK=true|false`
