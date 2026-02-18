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
- Main menu has 2 buttons:
  - `Search One Keyword`
  - `Import Excel File`
- Send keyword text to crawl after choosing `Search One Keyword`
- Upload Excel (`.xlsx` / `.xls`) with keywords and receive one result Excel:
  - keywords are read per input sheet (one or more keywords per sheet)
  - output keeps sheet grouping (one output sheet per input sheet)
  - each row includes `type`, `channel`, `uid`, `link` (merged by sheet, not split by keyword)
- Results are paginated in Telegram (links default: 50 per page via `LINKS_PAGE_SIZE`)
- Link output format:
  - bot links are listed first as clickable links (including deep links like `?start=...`)
  - non-message root links (public roots and invites) are listed as clickable links
  - message links are grouped by `Telegram UID` (channel id/username) and listed as full clickable links under each group
- Match cache is kept in Redis, then flushed to Oracle (`channel_matches`, `search_links`)
- Search history is stored per Telegram user in Oracle (`users` -> `searches`)
- `channel_matches.search_id` is linked to `searches.id` for keyword ownership tracking

## Environment Commands
- Development (proxy): `npm run start:dev`
- Production (no proxy): `npm run start` or `npm run start:prod`

## Git Branch Strategy
- `main` = production branch
- `develop` = development branch
- Proxy is only for development profile (`.env.development` / `.env.development.example`)
- Production profile has no proxy (`.env.production` / `.env.production.example`)

Sync both branches from latest `main`:
1. `git checkout main`
2. `git pull origin main`
3. `git checkout develop`
4. `git rebase main`
5. `git push -u origin develop`

Release develop to production branch:
1. `git checkout main`
2. `git merge --ff-only develop`
3. `git push origin main`

## Kubernetes (Production)
Files are under `k8s/production`.

What is included:
- Telegram bot deployment with liveness/readiness probes
- Redis deployment + PVC + service
- Bot state PVC for GramJS local storage
- PDB and HPA (min 1, max 2)

Build and push image:
1. `docker build -t ghcr.io/<your-user>/telegram-keyword-bot:v2 .`
2. `docker push ghcr.io/<your-user>/telegram-keyword-bot:v2`
3. Update image in `k8s/production/deployment.yaml`

Create Kubernetes secret from production env values:
1. `kubectl -n telegram-keyword-bot create secret generic telegram-keyword-bot-secret --from-env-file=.env.production --dry-run=client -o yaml | kubectl apply -f -`

Apply manifests:
1. `kubectl apply -k k8s/production`

Check rollout:
1. `kubectl -n telegram-keyword-bot get pods`
2. `kubectl -n telegram-keyword-bot rollout status deploy/telegram-keyword-bot`
3. `kubectl -n telegram-keyword-bot logs -f deploy/telegram-keyword-bot`

## Logging
Uses Winston; configure log level via `LOG_LEVEL`.

## Notes
- `npm start` and `npm run start:dev` already set a valid `--localstorage-file` path for GramJS (`.node-localstorage`).
- `npm run start:prod` runs `prestart:prod`, so build is always refreshed before production start.
- `start:dev` always reads `.env.development`.
- `start` and `start:prod` always read `.env.production`.
- `db:cleanup`, `db:reset`, and `migrate:oracle` default to `.env.production` unless you override `ENV_FILE`.
- `npm run db:cleanup` is idempotent; it removes dirty legacy columns, fixes FKs/indexes, and creates `search_links` if missing.
- DB operation timeout is configurable with `DB_OP_TIMEOUT_MS` (default: `120000`).
- Initial search row insert timeout is configurable with `SEARCH_CREATE_TIMEOUT_MS` (default: `5000`).
- MTProto user-session init timeout is configurable with `MTPROTO_INIT_TIMEOUT_MS` (default: `30000`).
- Generate a fresh MTProto session string with `npm run session:generate`.
- `channel_matches` stores provenance fields for each matched message:
  - `match_reason`: `keyword_hyperlink` or `keyword_video`
  - `iteration_no`
  - `discovered_via_link`
  - `discovered_from_message_link`
  - `discovered_from_channel`
- Keyword matching is applied on:
  - message/caption text
  - chat metadata (title, name, username, additional usernames)
  - bot/channel/group bio/about
- Iterative link-following for channels/groups is strict:
  - bot links are kept as bot links
  - channel/group root links are crawled in next iteration and evaluated by normal message conditions
  - when a link points to a specific message (`.../<uid>`), that target message must contain the keyword (or fuzzy match), otherwise link is ignored
- Join policy:
  - `CRAWL_AUTO_JOIN=true` controls joining via private invite links (`t.me/+...`) for access
  - `CRAWL_JOIN_PUBLIC=true` (default) joins public channels/groups during crawl
  - `CRAWL_START_BOTS=true` (default) sends `/start` to discovered bots (keeps deep-link `start` params)
  - `CRAWL_LEAVE_JOINED_PRIVATE=false` (default) keeps privately joined channels after crawl
- Excel import tuning:
  - `EXCEL_CRAWL_CONCURRENCY=2` controls parallel keyword workers for Excel batch mode
  - `SEARCH_CREATE_MAX_ATTEMPTS=3` retries search-row creation for reliable Oracle persistence
- Keyword matching normalizes Arabic/Persian text (`وحشی`, `وح شی`, `وح.شی`, `و ح ش ی`, `وحـ شی` all match).
- Fuzzy matching is enabled by default and configurable:
  - `CRAWL_FUZZY_ENABLED=true|false`
  - `CRAWL_FUZZY_MAX_DISTANCE=1`
  - `CRAWL_FUZZY_MIN_TERM_LENGTH=4`
- Messages are stored when:
  - keyword + at least one Telegram link, or
  - keyword in caption of a video/clip (even with no link)
  - configurable by `CRAWL_ALLOW_VIDEO_CAPTION_WITHOUT_LINK=true|false`
