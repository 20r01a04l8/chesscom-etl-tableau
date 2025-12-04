#!/usr/bin/env python3
"""
fetch_and_post.py — Incremental fetcher for Chess.com archives that writes to Google Sheets.

Requirements:
  pip install requests gspread google-auth

How it writes to Sheets:
- Uses a service account JSON file path provided via env GSPREAD_SA_JSON_PATH (recommended)
  or via secret `GSPREAD_SERVICE_ACCOUNT_JSON_B64` decoded in the Action (workflow does that).
- SHEET_ID env var must be set (the Google Sheet ID).
- For each username, it writes rows into a worksheet named by `SHEET_NAME_PREFIX + username`
  (if SHEET_NAME_PREFIX is empty, uses the username as sheet title).
- If the worksheet doesn't exist it is created and the header row (SHEET_COLS) is written.
"""

from __future__ import annotations
import os
import sys
import time
import json
import base64
import requests
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional

# gspread / google auth
import gspread
from google.oauth2 import service_account

# === CONFIG (change as needed) ===
DEFAULT_USER_AGENT = "ChessAnalytics/1.0 (+your-email@example.com)"  # change email
USER_AGENT = os.environ.get("MAKE_USER_AGENT", DEFAULT_USER_AGENT)
DELAY = float(os.environ.get("CHESS_REQUEST_DELAY", "1.0"))         # seconds between chess.com requests
MAX_RETRIES = int(os.environ.get("CHESS_MAX_RETRIES", "3"))
STATE_FILE = os.environ.get("STATE_FILE", "state.json")
GSPREAD_SA_JSON_PATH = os.environ.get("GSPREAD_SA_JSON_PATH", "")   # e.g. ./sa.json (workflow writes this)
SHEET_ID = os.environ.get("SHEET_ID", "")                          # required
SHEET_NAME_PREFIX = os.environ.get("SHEET_NAME_PREFIX", "")        # optional prefix for worksheet names

# Columns (must match your downstream target headers)
SHEET_COLS = [
    "ingest_time", "username", "archive_url", "game_url", "time_control",
    "end_time_utc", "date_ymd", "white_username", "white_rating",
    "black_username", "black_rating", "result", "pgn"
]

# === Helpers ===
def load_state() -> Dict[str, Any]:
    p = Path(STATE_FILE)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8") or "{}")
    except Exception as e:
        print(f"[WARN] Could not load state from {STATE_FILE}: {e}")
        return {}

def atomic_write_json(path: str, obj: Any) -> None:
    tmp = Path(path + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(Path(path))

def save_state(state: Dict[str, Any]) -> None:
    try:
        atomic_write_json(STATE_FILE, state)
    except Exception as e:
        print(f"[ERROR] Failed to write state to {STATE_FILE}: {e}")

def safe_get_json(url: str) -> Any:
    wait = 2.0
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=headers, timeout=30)
        except requests.RequestException as e:
            print(f"[attempt {attempt}] RequestException for {url}: {e}. Sleeping {wait}s")
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
            print(f"[attempt {attempt}] Retryable status {r.status_code} for {url}. Backoff {wait}s")
            if attempt == MAX_RETRIES:
                r.raise_for_status()
            time.sleep(wait)
            wait *= 2
            continue

        r.raise_for_status()

    raise RuntimeError(f"Failed to GET {url} after {MAX_RETRIES} retries")

def convert_game_to_row(username: str, archive_url: str, game: Dict[str, Any]) -> List[Any]:
    end_time = game.get("end_time")
    if end_time:
        dt = datetime.utcfromtimestamp(int(end_time))
        end_time_iso = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        date_ymd = dt.strftime("%Y-%m-%d")
    else:
        end_time_iso = ""
        date_ymd = ""

    row = [
        datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),  # ingest_time
        username,
        archive_url,
        game.get("url") or "",
        game.get("time_control") or "",
        end_time_iso,
        date_ymd,
        game.get("white", {}).get("username") or "",
        game.get("white", {}).get("rating") or "",
        game.get("black", {}).get("username") or "",
        game.get("black", {}).get("rating") or "",
        (game.get("white", {}).get("result") or "") + " / " + (game.get("black", {}).get("result") or ""),
        game.get("pgn") or ""
    ]
    return row

def convert_game_to_obj(username: str, archive_url: str, game: Dict[str, Any]) -> Dict[str, Any]:
    row = convert_game_to_row(username, archive_url, game)
    return {SHEET_COLS[i]: row[i] for i in range(len(SHEET_COLS))}

def archive_name_from_url(url: str) -> str:
    name = url.rstrip("/").split("/")[-1]
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in name)

# --- Google Sheets helpers ---
def _load_service_account_info() -> Dict[str, Any]:
    """
    Loads service account info either from a file path (GSPREAD_SA_JSON_PATH)
    or from env var GSPREAD_SERVICE_ACCOUNT_JSON_B64 (base64) if provided.
    Workflow already writes a file; prefer that.
    """
    if GSPREAD_SA_JSON_PATH and Path(GSPREAD_SA_JSON_PATH).exists():
        return json.loads(Path(GSPREAD_SA_JSON_PATH).read_text(encoding="utf-8"))
    # fallback: try env var (raw JSON)
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

def ensure_worksheet(spreadsheet: gspread.Spreadsheet, worksheet_name: str) -> gspread.Worksheet:
    try:
        ws = spreadsheet.worksheet(worksheet_name)
        # check header
        existing = ws.row_values(1)
        if not existing or existing[: len(SHEET_COLS)] != SHEET_COLS:
            # overwrite header row to match expected columns
            ws.resize(rows=1)
            ws.append_row(SHEET_COLS, value_input_option="RAW")
        return ws
    except gspread.WorksheetNotFound:
        # create with a reasonable row/col size
        ws = spreadsheet.add_worksheet(title=worksheet_name, rows="1000", cols=str(len(SHEET_COLS)))
        ws.append_row(SHEET_COLS, value_input_option="RAW")
        return ws

def append_rows_to_sheet(username: str, rows_objs: List[Dict[str, Any]]) -> None:
    if not rows_objs:
        return
    client = get_gspread_client()
    spreadsheet = client.open_by_key(SHEET_ID)
    worksheet_name = f"{SHEET_NAME_PREFIX}{username}" if SHEET_NAME_PREFIX else username
    ws = ensure_worksheet(spreadsheet, worksheet_name)
    # convert list of dicts into list of lists in correct order
    rows_data = []
    for obj in rows_objs:
        rows_data.append([obj.get(c, "") for c in SHEET_COLS])
    # Use append_rows for batch append
    # gspread append_rows will add rows after the last row
    ws.append_rows(rows_data, value_input_option="USER_ENTERED")

# === Main logic ===
def fetch_and_post(usernames_csv: str) -> None:
    usernames = [u.strip() for u in usernames_csv.split(",") if u.strip()]
    if not usernames:
        raise SystemExit("No usernames provided.")

    state = load_state()
    for username in usernames:
        user_state = state.get(username, {})
        last_end_time = int(user_state.get("last_end_time", 0))
        processed_archives = set(user_state.get("processed_archives", []))
        print(f"\n=== User: {username} (last_end_time={last_end_time}) ===")

        archives_url = f"https://api.chess.com/pub/player/{username}/games/archives"
        try:
            archives_json = safe_get_json(archives_url)
        except Exception as e:
            print(f"[ERROR] Could not fetch archives for {username}: {e}")
            continue

        archives = archives_json.get("archives", []) or []
        print(f"Found {len(archives)} archives total")

        # Process archives in chronological order (older -> newer)
        for archive in archives:
            if archive in processed_archives:
                continue

            print(f"Fetching archive: {archive}")
            time.sleep(DELAY)
            try:
                archive_json = safe_get_json(archive)
            except Exception as e:
                print(f"[ERROR] Failed to download {archive}: {e}")
                continue

            games = archive_json.get("games", []) or []
            new_games = []
            for g in games:
                et = g.get("end_time")
                if et is None:
                    new_games.append(g)
                else:
                    try:
                        if int(et) > last_end_time:
                            new_games.append(g)
                    except Exception:
                        new_games.append(g)

            if not new_games:
                print(f"No new games in archive (marking processed): {archive}")
                processed_archives.add(archive)
                state[username] = {
                    "last_end_time": last_end_time,
                    "processed_archives": sorted(list(processed_archives))
                }
                save_state(state)
                continue

            try:
                new_games_sorted = sorted(new_games, key=lambda x: int(x.get("end_time", 0) or 0))
            except Exception:
                new_games_sorted = new_games

            rows_payload = [convert_game_to_obj(username, archive, g) for g in new_games_sorted]

            # Append to Google Sheets
            try:
                print(f"Appending {len(rows_payload)} rows to Google Sheet (sheet id: {SHEET_ID})")
                append_rows_to_sheet(username, rows_payload)
                print("Append to Google Sheets succeeded")
            except Exception as e:
                print(f"[ERROR] Failed to append to Google Sheets for {archive}: {e}")
                # do not mark archive processed so it can be retried
                continue

            # Optionally save outputs locally too (keeps previous behavior)
            try:
                out_dir = Path("outputs") / username
                out_dir.mkdir(parents=True, exist_ok=True)
                out_path = out_dir / f"{archive_name_from_url(archive)}.json"
                data = {"username": username, "archive_url": archive, "rows": rows_payload}
                atomic_write_json(str(out_path), data)
            except Exception as e:
                print(f"[WARN] Failed to write local outputs: {e}")

            # Update last_end_time
            max_end = last_end_time
            for g in new_games_sorted:
                et = g.get("end_time")
                try:
                    if et is not None and int(et) > max_end:
                        max_end = int(et)
                except Exception:
                    continue

            processed_archives.add(archive)
            state[username] = {
                "last_end_time": max_end,
                "processed_archives": sorted(list(processed_archives))
            }
            save_state(state)
            time.sleep(DELAY)

    print("\nDone. State saved to", STATE_FILE)

# CLI / env fallback
if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1].strip():
        usernames_arg = sys.argv[1]
    else:
        usernames_arg = os.environ.get("CHESS_USERNAMES", "").strip()

    if not usernames_arg:
        print("No usernames provided. Provide usernames as CLI arg or set CHESS_USERNAMES env var.")
        print('Example: python fetch_and_post.py "konduvinay,anotheruser"')
        sys.exit(1)

    fetch_and_post(usernames_arg)
