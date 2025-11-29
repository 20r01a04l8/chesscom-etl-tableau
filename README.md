# ChessAnalytics — fetch & post Chess.com monthly archives to Make → Google Sheets

## Purpose
Daily GitHub Action fetcher that:
- Polls Chess.com archive list for usernames,
- Downloads new monthly archives,
- Converts games to rows,
- POSTS a single batched payload per archive to a Make webhook,
- Make appends rows to Google Sheets in one batch and logs processed archives.

## Setup (GitHub repo)
1. Add files to your repo:
   - fetch_and_post.py
   - requirements.txt
   - state.json (leave as `{}`)
   - .github/workflows/fetch.yml

2. Create GitHub Secrets (repo Settings → Secrets):
   - `MAKE_WEBHOOK` → your Make webhook URL
   - `MAKE_SECRET` → shared secret (optional)
   - `CHESS_USERNAMES` → comma-separated usernames (e.g. `konduvinay,anotheruser`)

3. (Optional) customize `DEFAULT_USER_AGENT` in fetch_and_post.py to include your contact email.

## Make.com scenario (webhook side)
1. Create a new Scenario.
2. Add module: **Webhooks → Custom webhook** and copy URL (use that URL for MAKE_WEBHOOK).
3. Immediately after webhook, add a **Filter** that checks header `X-Hook-Token` equals your MAKE_SECRET (if you set one).
4. Next module: **Google Sheets → Add a row(s)** (batch append). Map incoming payload `rows` to batch rows.
   - Spreadsheet tabs:
     - `Games` — columns A..M:
       ingest_time, username, archive_url, game_url, time_control, end_time_utc, date_ymd, white_username, white_rating, black_username, black_rating, result, pgn
     - `ProcessedArchives` — username, archive_url, processed_at_utc, game_count
     - `StatusLog` — run_id, username, stage, message, http_status, timestamp_utc
5. After Add row(s), append to `ProcessedArchives` and `StatusLog` as described in the project notes.

## Local testing
Run locally:

