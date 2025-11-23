#!/usr/bin/env python3
"""
etl_script.py
Production-ready ETL that:
- reads config.yml for usernames
- fetches Chess.com monthly archives incrementally using state.json
- flattens games and writes a local CSV backup per user
- appends only new games to a Google Sheet per user using a service-account JSON
- uses retries, exponential backoff, polite User-Agent, and logging
"""
import os
import sys
import time
import json
import argparse
import logging
from typing import List, Dict, Any, Optional

import requests
import pandas as pd
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import gspread
from google.oauth2.service_account import Credentials
import yaml
from datetime import datetime

# ---------- Configurable constants ----------
DEFAULT_STATE_PATH = os.environ.get("CHESS_STATE_PATH", "state.json")
DEFAULT_CONFIG_PATH = os.environ.get("CHESS_CONFIG_PATH", "config.yml")
SERVICE_ACCOUNT_PATH = os.environ.get("GC_SA_PATH", "service-account.json")
USER_AGENT = os.environ.get("CHESS_USER_AGENT", "chess-etl-script/1.0 (+contact@example.com)")
POLITE_DELAY_DEFAULT = float(os.environ.get("CHESS_POLITE_DELAY", "0.5"))

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("chess-etl")

# ---------- HTTP with retry ----------
@retry(wait=wait_exponential(multiplier=1, min=1, max=30),
       stop=stop_after_attempt(5),
       retry=retry_if_exception_type(Exception))
def fetch_json(url: str, headers: Dict[str, str]) -> Dict[str, Any]:
    logger.info(f"GET {url}")
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

# ---------- State management ----------
def read_state(path: str = DEFAULT_STATE_PATH) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def write_state(state: Dict[str, Any], path: str = DEFAULT_STATE_PATH) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    os.replace(tmp, path)
    logger.info(f"Updated state file: {path}")

def get_last_archive_for(username: str, state: Dict[str, Any]) -> Optional[str]:
    return state.get(username, {}).get("last_archive")

def set_last_archive_for(username: str, archive_url: str, state: Dict[str, Any]) -> None:
    if username not in state:
        state[username] = {}
    state[username]["last_archive"] = archive_url

# ---------- Google Sheets helpers ----------
def gc_from_service_account_json(path: str = SERVICE_ACCOUNT_PATH):
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(path, scopes=scopes)
    client = gspread.authorize(creds)
    return client

def ensure_spreadsheet_for_user(gc, username: str, sheet_title: Optional[str] = None):
    title = sheet_title or f"chess_{username}_games"
    try:
        sh = gc.open(title)
        logger.info(f"Opened existing spreadsheet: {title}")
    except Exception:
        logger.info(f"Spreadsheet '{title}' not found; creating new.")
        sh = gc.create(title)
        # try to share as public read (best-effort)
        try:
            sh.share(None, perm_type='anyone', role='reader')
            logger.info(f"Shared spreadsheet '{title}' as public reader.")
        except Exception as e:
            logger.warning(f"Could not auto-share spreadsheet: {e}")
    ws = sh.sheet1
    return sh, ws

def read_sheet_to_df(ws):
    try:
        records = ws.get_all_records()
    except Exception as e:
        logger.error(f"Failed to read sheet: {e}")
        records = []
    if not records:
        cols = ["date_utc","date_iso","time_class","white","black","white_rating",
                "black_rating","moves","move_count","result","eco","pgn","game_url"]
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(records)
    expected = ["date_utc","date_iso","time_class","white","black","white_rating",
                "black_rating","moves","move_count","result","eco","pgn","game_url"]
    for c in expected:
        if c not in df.columns:
            df[c] = None
    return df[expected]

def append_new_games_to_sheet(ws, new_df: pd.DataFrame) -> int:
    existing_df = read_sheet_to_df(ws)
    existing_urls = set(existing_df['game_url'].astype(str).tolist())
    new_df = new_df.copy()
    new_df['game_url'] = new_df['game_url'].astype(str)
    to_append = new_df[~new_df['game_url'].isin(existing_urls)]
    if to_append.empty:
        logger.info("No new games to append to sheet.")
        return 0
    rows = to_append.fillna("").astype(str).values.tolist()
    # If existing is empty -> write headers + rows
    if existing_df.empty:
        header = list(new_df.columns)
        ws.clear()
        ws.update([header] + rows)
        logger.info(f"Wrote {len(rows)} rows (full write).")
        return len(rows)
    else:
        try:
            ws.append_rows(rows)
            logger.info(f"Appended {len(rows)} rows.")
            return len(rows)
        except Exception as e:
            logger.warning(f"append_rows failed: {e}. Falling back to manual update.")
            last_row = len(existing_df) + 2  # +1 header + 1 for 1-indexing
            ws.update(f"A{last_row}", rows)
            return len(rows)

# ---------- Chess.com fetch + flatten ----------
BASE_PLAYER = "https://api.chess.com/pub/player/{username}"

def epoch_to_iso(epoch_seconds):
    if not epoch_seconds:
        return None
    return datetime.utcfromtimestamp(int(epoch_seconds)).isoformat() + "Z"

def get_archives(username: str, headers: Dict[str,str]) -> List[str]:
    url = BASE_PLAYER.format(username=username) + "/games/archives"
    js = fetch_json(url, headers=headers)
    return js.get("archives", [])

def get_games_from_archive(archive_url: str, headers: Dict[str,str]) -> List[Dict[str, Any]]:
    js = fetch_json(archive_url, headers=headers)
    return js.get("games", [])

def flatten_game(g: Dict[str, Any]) -> Dict[str, Any]:
    white = g.get("white", {}) or {}
    black = g.get("black", {}) or {}
    pgn = g.get("pgn") or ""
    result = white.get("result") or black.get("result") or ""
    return {
        "date_utc": g.get("end_time"),
        "date_iso": epoch_to_iso(g.get("end_time")),
        "time_class": g.get("time_class"),
        "white": white.get("username"),
        "black": black.get("username"),
        "white_rating": white.get("rating"),
        "black_rating": black.get("rating"),
        "move_count": len(pgn.split()) if pgn else None,
        "result": result,
        "eco": g.get("eco"),
        "pgn": pgn,
        "game_url": g.get("url")
    }

def fetch_new_games_for_user(username: str, headers: Dict[str,str], polite_delay: float, max_archives: Optional[int], state: Dict[str, Any]):
    archives = get_archives(username, headers)
    logger.info(f"Total archives found: {len(archives)} for {username}")
    last = get_last_archive_for(username, state)
    if last and last in archives:
        idx = archives.index(last)
        new_archives = archives[idx+1:]
    else:
        new_archives = archives
    if max_archives:
        new_archives = new_archives[-max_archives:]
    logger.info(f"Archives to fetch for {username}: {len(new_archives)}")
    rows = []
    for a in new_archives:
        try:
            logger.info(f"Fetching archive {a}")
            games = get_games_from_archive(a, headers)
            for g in games:
                rows.append(flatten_game(g))
        except Exception as e:
            logger.error(f"Failed archive {a}: {e}")
        time.sleep(polite_delay)
    # Do not update state here; caller will update after successful upload
    return rows, new_archives

# ---------- Orchestration ----------
def load_config(path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def main(argv=None):
    parser = argparse.ArgumentParser(description="Chess.com ETL -> Google Sheets")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Path to config.yml")
    parser.add_argument("--state", default=DEFAULT_STATE_PATH, help="Path to state.json")
    parser.add_argument("--service-account", default=SERVICE_ACCOUNT_PATH, help="Path to service-account.json")
    parser.add_argument("--max-archives", type=int, default=None, help="Limit number of archives to fetch (for testing)")
    args = parser.parse_args(argv)

    cfg = load_config(args.config)

    # -- validate cfg is a dict
    if not isinstance(cfg, dict):
        raise ValueError(f"Config file {args.config} did not load as a mapping (got {type(cfg)}). Please check YAML.")

    polite_delay = cfg.get("polite_delay", POLITE_DELAY_DEFAULT)
    max_archives = args.max_archives if args.max_archives is not None else cfg.get("max_archives")
    usernames = cfg.get("usernames") or []
    if not isinstance(usernames, list) or not usernames:
        raise ValueError(f"No usernames found in config (config path: {args.config}). Please add at least one username under 'usernames'.")

    sheet_map = cfg.get("sheet_names") or {}
    if not isinstance(sheet_map, dict):
        logger.warning("config.yml contains 'sheet_names' but it is not a mapping/dict. Ignoring and using defaults.")
        sheet_map = {}


    # Validate service account file exists
    if not os.path.exists(args.service_account):
        raise FileNotFoundError(f"Service account JSON not found at: {args.service_account}")

    # gspread client
    gc = gc_from_service_account_json(args.service_account)

    # headers
    headers = {"User-Agent": USER_AGENT}

    # read state
    state = read_state(args.state)

    results = {}
    for username in usernames:
        logger.info(f"=== Processing username: {username} ===")
        try:
            rows, new_archives = fetch_new_games_for_user(username, headers, polite_delay, max_archives, state)
            df = pd.DataFrame(rows)
            # ensure columns
            expected_cols = ["date_utc","date_iso","time_class","white","black","white_rating",
                             "black_rating","moves","move_count","result","eco","pgn","game_url"]
            for c in expected_cols:
                if c not in df.columns:
                    df[c] = None
            df = df[expected_cols]
            # local backup CSV
            csv_path = f"{username}.csv"
            df.to_csv(csv_path, index=False)
            logger.info(f"Wrote local CSV backup: {csv_path} ({len(df)} rows).")

            # upload to Google Sheets (append only)
            sheet_title = sheet_map.get(username)
            sh, ws = ensure_spreadsheet_for_user(gc, username, sheet_title)
            appended = append_new_games_to_sheet(ws, df)

            # update state: only if we fetched at least one archive successfully
            if new_archives:
                most_recent = new_archives[-1]
                set_last_archive_for(username, most_recent, state)
                write_state(state, args.state)

            results[username] = {"fetched": len(df), "appended": appended, "sheet": sh.title}
            logger.info(f"Finished {username}: fetched={len(df)} appended={appended} sheet={sh.title}")
        except Exception as e:
            logger.exception(f"Processing failed for {username}: {e}")
            results[username] = {"error": str(e)}

    logger.info("ALL DONE.")
    logger.info(json.dumps(results, indent=2))
    return results

if __name__ == "__main__":
    main()
