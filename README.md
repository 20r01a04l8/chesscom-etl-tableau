# ChessAnalytics — fetch & post Chess.com monthly archives to Github Actions → Google Sheets

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
   - Spreadsheet tabs:
     - `Games` — columns A..M:
       ingest_time, username, archive_url, game_url, time_control, end_time_utc, date_ymd, white_username, white_rating, black_username, black_rating, result, pgn
     - `StatusLog` — run_id, username, stage, message, http_status, timestamp_utc
5. After Add row(s), append to `StatusLog` as described in the project notes.

## Local testing
Run locally:

