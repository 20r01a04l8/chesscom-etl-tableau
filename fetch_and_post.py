#!/usr/bin/env python3
"""
fetch_and_post.py — Incremental fetcher for Chess.com archives that writes to Google Sheets,
with idempotency by game_url (avoids duplicate rows when re-processing the latest archive).

Behavior:
- Reads existing game_url values from the Games sheet and skips appending any row whose
  game_url already exists.
- Writes processed archive records to ProceeedArchives with game_count = number appended.
- Appends status messages to StatusLog.
- Uses state.json in repo (workflow may pop the last processed archive each run).
- Extracts canonical result (1-0, 0-1, 1/2-1/2) from PGN when available, otherwise falls back
  to white_result / black_result.
"""

from __future__ import annotations
import os
import sys
import time
import json
import base64
import requests
import re
from datetime import datetime
from typing import Any, Dict, List, Set

# Google libs
import gspread
from google.oauth2 import service_account

# === CONFIG ===
DEFAULT_USER_AGENT = "ChessAnalytics/1.0 (+your-email@example.com)"
USER_AGENT = os.environ.get("MAKE_USER_AGENT", DEFAULT_USER_AGENT)
DELAY = float(os.environ.get("CHESS_REQUEST_DELAY", "1.0"))
MAX_RETRIES = int(os.environ.get("CHESS_MAX_RETRIES", "3"))
STATE_FILE = os.environ.get("STATE_FILE", "state.json")
GSPREAD_SA_JSON_PATH = os.environ.get("GSPREAD_SA_JSON_PATH", "")  # set by workflow (./sa.json)
SHEET_ID = os.environ.get("SHEET_ID", "")                          # required
SHEET_NAME_PREFIX = os.environ.get("SHEET_NAME_PREFIX", "")        # optional

# Sheet tab names (exact)
GAMES_SHEET = "Games"
PROCESSED_SHEET = "ProceeedArchives"
STATUS_SHEET = "StatusLog"

# Headers
GAMES_HEADERS = [
    "ingest_time", "username", "archive_url", "game_url", "time_control",
    "end_time_utc", "date_ymd", "white_username", "white_rating",
    "black_username", "black_rating", "result", "pgn"
]
PROCESSED_HEADERS = ["username", "archive_url", "processed_at_utc", "game_count"]
STATUS_HEADERS = ["run_id", "username", "stage", "message", "http_status", "timestamp_utc"]


# === Helpers: Chess API ===
def safe_get_json(url: str) -> Any:
    wait = 2.0
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=headers, timeout=30)
        except requests.RequestException as e:
            _log_console(f"[attempt {attempt}] Request error for {url}: {e}. Sleeping {wait}s")
            if attempt == MAX_RETRIES:
                raise
            time.sleep(wait)
            wait *= 2
            continue

        if r.status_code == 200:
            try:
                return r.json()
            except Exception as e:
                raise RuntimeError(f"Invalid JSON from {url}: {e}")

        if r.status_code in (429, 500, 502, 503, 504):
            _log_console(f"[attempt {attempt}] Retryable status {r.status_code} for {url}. Backoff {wait}s")
            if attempt == MAX_RETRIES:
                r.raise_for_status()
            time.sleep(wait)
            wait *= 2
            continue

        r.raise_for_status()

    raise RuntimeError(f"Failed to GET {url} after {MAX_RETRIES} retries")


def parse_pgn_result(pgn: str) -> str:
    """
    Attempt to extract the game result from PGN.
    Priority:
      1) [Result "1-0"] header tag
      2) trailing game token at the end of the PGN (e.g. '1-0', '0-1', '1/2-1/2')
      3) empty string if nothing found
    Returns canonical: "1-0", "0-1", or "1/2-1/2" (or empty string)
    """
    if not pgn:
        return ""
    # 1) look for Result header
    m = re.search(r'\[Result\s+"([^"]+)"\]', pgn)
    if m:
        return m.group(1).strip()

    # 2) search for a stand-alone result token anywhere (prefer the last occurrence)
    tokens = re.findall(r'\b(1-0|0-1|1/2-1/2)\b', pgn)
    if tokens:
        return tokens[-1].strip()

    return ""


def convert_game_to_row(username: str, archive_url: str, game: Dict[str, Any]) -> List[Any]:
    """
    Build a row for the Games sheet. Result is extracted from PGN when possible.
    """
    end_time = game.get("end_time")
    if end_time:
        try:
            dt = datetime.utcfromtimestamp(int(end_time))
            end_time_iso = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            date_ymd = dt.strftime("%Y-%m-%d")
        except Exception:
            end_time_iso = ""
            date_ymd = ""
    else:
        end_time_iso = ""
        date_ymd = ""

    pgn = game.get("pgn") or ""
    # try to extract canonical result from PGN (1-0, 0-1, 1/2-1/2)
    pgn_result = parse_pgn_result(pgn)

    if pgn_result:
        result_field = pgn_result
    else:
        # fallback to previous approach (white_result / black_result) to preserve behavior
        white_res = (game.get("white", {}) or {}).get("result") or ""
        black_res = (game.get("black", {}) or {}).get("result") or ""
        if white_res or black_res:
            result_field = f"{white_res} / {black_res}"
        else:
            result_field = ""

    row = [
        datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        username,
        archive_url,
        game.get("url") or "",
        game.get("time_control") or "",
        end_time_iso,
        date_ymd,
        (game.get("white", {}) or {}).get("username") or "",
        (game.get("white", {}) or {}).get("rating") or "",
        (game.get("black", {}) or {}).get("username") or "",
        (game.get("black", {}) or {}).get("rating") or "",
        result_field,
        pgn
    ]
    return row


# === Logging helpers ===
def _log_console(msg: str) -> None:
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{ts}] {msg}")


# === Google Sheets helpers ===
def _load_service_account_info() -> Dict[str, Any]:
    if GSPREAD_SA_JSON_PATH and os.path.exists(GSPREAD_SA_JSON_PATH):
        with open(GSPREAD_SA_JSON_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    b64 = os.environ.get("GSPREAD_SERVICE_ACCOUNT_JSON_B64", "")
    if b64:
        try:
            raw = base64.b64decode(b64)
            return json.loads(raw)
        except Exception as e:
            raise RuntimeError(f"Failed to decode GSPREAD_SERVICE_ACCOUNT_JSON_B64: {e}")
    raise RuntimeError("Service account JSON not found. Set GSPREAD_SA_JSON_PATH or GSPREAD_SERVICE_ACCOUNT_JSON_B64")


def get_gspread_client() -> gspread.Client:
    if not SHEET_ID:
        raise RuntimeError("SHEET_ID env var is not set")
    sa_info = _load_service_account_info()
    creds = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    client = gspread.authorize(creds)
    return client


def ensure_sheet_tabs(spreadsheet: gspread.Spreadsheet) -> None:
    existing = {ws.title for ws in spreadsheet.worksheets()}
    if GAMES_SHEET not in existing:
        ws = spreadsheet.add_worksheet(title=GAMES_SHEET, rows="1000", cols=str(len(GAMES_HEADERS)))
        ws.append_row(GAMES_HEADERS, value_input_option="RAW")
    else:
        ws = spreadsheet.worksheet(GAMES_SHEET)
        header = ws.row_values(1)
        if not header or header[: len(GAMES_HEADERS)] != GAMES_HEADERS:
            ws.resize(rows=1)
            ws.append_row(GAMES_HEADERS, value_input_option="RAW")

    if PROCESSED_SHEET not in existing:
        ws2 = spreadsheet.add_worksheet(title=PROCESSED_SHEET, rows="1000", cols=str(len(PROCESSED_HEADERS)))
        ws2.append_row(PROCESSED_HEADERS, value_input_option="RAW")
    else:
        ws2 = spreadsheet.worksheet(PROCESSED_SHEET)
        header2 = ws2.row_values(1)
        if not header2 or header2[: len(PROCESSED_HEADERS)] != PROCESSED_HEADERS:
            ws2.resize(rows=1)
            ws2.append_row(PROCESSED_HEADERS, value_input_option="RAW")

    if STATUS_SHEET not in existing:
        ws3 = spreadsheet.add_worksheet(title=STATUS_SHEET, rows="1000", cols=str(len(STATUS_HEADERS)))
        ws3.append_row(STATUS_HEADERS, value_input_option="RAW")
    else:
        ws3 = spreadsheet.worksheet(STATUS_SHEET)
        header3 = ws3.row_values(1)
        if not header3 or header3[: len(STATUS_HEADERS)] != STATUS_HEADERS:
            ws3.resize(rows=1)
            ws3.append_row(STATUS_HEADERS, value_input_option="RAW")


def read_existing_game_urls(spreadsheet: gspread.Spreadsheet) -> Set[str]:
    """
    Read the existing game_url column from the Games sheet and return a set.
    This makes append idempotent.
    """
    try:
        ws = spreadsheet.worksheet(GAMES_SHEET)
    except gspread.WorksheetNotFound:
        return set()
    # Get all values in column D (game_url). If header present, skip it.
    # gspread uses 1-indexed columns, column 4 is D
    try:
        col = ws.col_values(4)
    except Exception:
        return set()
    # Remove header if present
    if col and col[0].strip().lower() == "game_url":
        col = col[1:]
    # return set of non-empty urls
    return set([c.strip() for c in col if c.strip()])


def append_games_rows(spreadsheet: gspread.Spreadsheet, rows: List[List[Any]]) -> None:
    if not rows:
        return
    ws = spreadsheet.worksheet(GAMES_SHEET)
    ws.append_rows(rows, value_input_option="USER_ENTERED")


def append_processed_record(spreadsheet: gspread.Spreadsheet, username: str, archive_url: str, game_count: int) -> None:
    ws = spreadsheet.worksheet(PROCESSED_SHEET)
    processed_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    ws.append_row([username, archive_url, processed_at, str(game_count)], value_input_option="RAW")


def append_status(spreadsheet: gspread.Spreadsheet, run_id: str, username: str, stage: str, message: str, http_status: str = "") -> None:
    try:
        ws = spreadsheet.worksheet(STATUS_SHEET)
        ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        ws.append_row([run_id, username or "", stage, message, http_status or "", ts], value_input_option="RAW")
    except Exception as e:
        _log_console(f"[WARN] Could not write to status sheet: {e}")


# === State helpers (read/write local state.json) ===
def load_state() -> Dict[str, Any]:
    p = STATE_FILE
    if not os.path.exists(p):
        return {}
    try:
        return json.loads(open(p, "r", encoding="utf-8").read() or "{}")
    except Exception as e:
        _log_console(f"[WARN] Could not load state from {p}: {e}")
        return {}


def save_state(state: Dict[str, Any]) -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
    except Exception as e:
        _log_console(f"[ERROR] Failed to write state to {STATE_FILE}: {e}")


# === Main flow ===
def fetch_and_write(usernames_csv: str) -> None:
    usernames = [u.strip() for u in usernames_csv.split(",") if u.strip()]
    if not usernames:
        raise SystemExit("No usernames provided.")

    client = get_gspread_client()
    spreadsheet = client.open_by_key(SHEET_ID)
    ensure_sheet_tabs(spreadsheet)

    # load existing game urls to avoid duplicates
    existing_urls = read_existing_game_urls(spreadsheet)
    _log_console(f"Existing games in sheet: {len(existing_urls)} URLs loaded")

    # load local state (state.json)
    state = load_state()

    run_id = os.environ.get("GITHUB_RUN_ID", str(int(time.time())))
    for username in usernames:
        _log_console(f"Processing user: {username}")
        user_state = state.get(username, {})
        last_end_time = int(user_state.get("last_end_time", 0))
        processed_archives = set(user_state.get("processed_archives", []))
        _log_console(f"User {username} last_end_time={last_end_time} processed_archives={len(processed_archives)}")

        archives_url = f"https://api.chess.com/pub/player/{username}/games/archives"
        try:
            archives_json = safe_get_json(archives_url)
        except Exception as e:
            _log_console(f"[ERROR] Could not fetch archives for {username}: {e}")
            append_status(spreadsheet, run_id, username, "error_fetch_archives", str(e))
            continue

        archives = archives_json.get("archives", []) or []
        _log_console(f"Found {len(archives)} archives for {username}")

        # process archives in chronological order
        for archive in archives:
            if archive in processed_archives:
                continue

            _log_console(f"Fetching archive {archive}")
            time.sleep(DELAY)
            try:
                archive_json = safe_get_json(archive)
            except Exception as e:
                _log_console(f"[ERROR] Failed to download {archive}: {e}")
                append_status(spreadsheet, run_id, username, "error_archive_download", f"{archive} | {e}")
                continue

            games = archive_json.get("games", []) or []
            if not games:
                _log_console(f"No games in archive {archive}; marking processed with 0 appended")
                append_processed_record(spreadsheet, username, archive, 0)
                processed_archives.add(archive)
                append_status(spreadsheet, run_id, username, "archive_no_games", archive)
                continue

            # sort ascending by end_time
            try:
                games_sorted = sorted(games, key=lambda x: int(x.get("end_time") or 0))
            except Exception:
                games_sorted = games

            # build candidate rows but skip duplicates by game_url
            rows_to_append = []
            new_urls_count = 0
            for g in games_sorted:
                url = g.get("url") or ""
                if url and url in existing_urls:
                    # already present — skip
                    continue
                row = convert_game_to_row(username, archive, g)
                rows_to_append.append(row)
                if url:
                    existing_urls.add(url)
                    new_urls_count += 1

            if not rows_to_append:
                _log_console(f"No new (unique) games to append from {archive}; marking processed")
                append_processed_record(spreadsheet, username, archive, 0)
                processed_archives.add(archive)
                append_status(spreadsheet, run_id, username, "no_new_unique_games", archive)
                continue

            # append unique rows in one batch
            try:
                append_games_rows(spreadsheet, rows_to_append)
                append_status(spreadsheet, run_id, username, "games_appended", f"{len(rows_to_append)}")
                _log_console(f"Appended {len(rows_to_append)} unique games from {archive}")
            except Exception as e:
                _log_console(f"[ERROR] Failed to append games to sheet: {e}")
                append_status(spreadsheet, run_id, username, "error_append_games", f"{archive} | {e}")
                # do not mark processed so next run will retry
                continue

            # mark processed with the number of appended rows
            try:
                append_processed_record(spreadsheet, username, archive, len(rows_to_append))
                processed_archives.add(archive)
                append_status(spreadsheet, run_id, username, "archive_processed", f"{archive} | appended={len(rows_to_append)}")
            except Exception as e:
                _log_console(f"[WARN] Failed to append processed record: {e}")
                append_status(spreadsheet, run_id, username, "error_append_processed", f"{archive} | {e}")

            # update last_end_time to max end_time among appended games
            max_end = last_end_time
            for g in games_sorted:
                et = g.get("end_time")
                try:
                    if et is not None and int(et) > max_end:
                        max_end = int(et)
                except Exception:
                    continue

            state[username] = {
                "last_end_time": max_end,
                "processed_archives": sorted(list(processed_archives))
            }
            save_state(state)

            time.sleep(DELAY)

    _log_console("Run complete.")


# CLI
if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1].strip():
        usernames_arg = sys.argv[1]
    else:
        usernames_arg = os.environ.get("CHESS_USERNAMES", "").strip()

    if not usernames_arg:
        print("No usernames provided. Provide as CLI arg or set CHESS_USERNAMES env var.")
        print('Example: python fetch_and_post.py "konduvinay,anotheruser"')
        sys.exit(1)

    fetch_and_write(usernames_arg)
