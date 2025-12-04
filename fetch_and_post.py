#!/usr/bin/env python3
"""
fetch_and_post.py — Incremental fetcher for Chess.com archives (Make removed).

Behavior changes:
- No Make / webhook posting.
- For each archive that contains new games, saves a JSON file to:
    outputs/<username>/<archive_name>.json
  where <archive_name> is the last path segment of the archive URL (e.g. 2025/11).
- Persists state (processed_archives + last_end_time) after each archive.
- Respects Chess.com politeness: delay between requests, exponential backoff for 429/5xx.
- Dry-run concept removed; instead data is always saved locally to outputs.
"""

from __future__ import annotations
import os
import sys
import time
import json
import requests
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List

# === CONFIG (change as needed) ===
DEFAULT_USER_AGENT = "ChessAnalytics/1.0 (+your-email@example.com)"  # change email
USER_AGENT = os.environ.get("MAKE_USER_AGENT", DEFAULT_USER_AGENT)
DELAY = float(os.environ.get("CHESS_REQUEST_DELAY", "1.0"))         # seconds between chess.com requests
MAX_RETRIES = int(os.environ.get("CHESS_MAX_RETRIES", "3"))
STATE_FILE = os.environ.get("STATE_FILE", "state.json")            # path to state file (repo root)

# Columns (must match your downstream target headers if you later import)
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
    # take last segment of URL and sanitize
    name = url.rstrip("/").split("/")[-1]
    # replace any characters that might be problematic in filenames
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in name)

def save_rows_to_file(username: str, archive_url: str, rows: List[Dict[str, Any]]) -> str:
    """
    Save rows (list of objects) to outputs/<username>/<archive_name>.json
    Returns the path where data was saved.
    """
    archive_name = archive_name_from_url(archive_url)
    out_dir = Path("outputs") / username
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{archive_name}.json"
    data = {
        "username": username,
        "archive_url": archive_url,
        "game_count": len(rows),
        "rows": rows,
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    try:
        atomic_write_json(str(out_path), data)
        return str(out_path)
    except Exception as e:
        raise RuntimeError(f"Failed to write output file {out_path}: {e}")

# === Main logic ===
def fetch_and_post(usernames_csv: str) -> None:
    usernames = [u.strip() for u in usernames_csv.split(",") if u.strip()]
    if not usernames:
        raise SystemExit("No usernames provided.")

    state = load_state()
    # state structure per user:
    # state[username] = {
    #    "last_end_time": 0,
    #    "processed_archives": [archive_url,...]
    # }
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
            # Skip if archive already processed
            if archive in processed_archives:
                continue

            print(f"Fetching archive: {archive}")
            time.sleep(DELAY)
            try:
                archive_json = safe_get_json(archive)
            except Exception as e:
                print(f"[ERROR] Failed to download {archive}: {e}")
                # Do not mark processed — try again next run
                continue

            games = archive_json.get("games", []) or []
            # Gather games that are newer than last_end_time
            new_games = []
            for g in games:
                et = g.get("end_time")
                if et is None:
                    # if no end_time treat as new (edge case)
                    new_games.append(g)
                else:
                    try:
                        if int(et) > last_end_time:
                            new_games.append(g)
                    except Exception:
                        new_games.append(g)

            if not new_games:
                print(f"No new games in archive (marking processed): {archive}")
                # mark archive processed (no new games)
                processed_archives.add(archive)
                # update state and persist
                state[username] = {
                    "last_end_time": last_end_time,
                    "processed_archives": sorted(list(processed_archives))
                }
                save_state(state)
                continue

            # Sort new_games ascending by end_time so we save older-first
            try:
                new_games_sorted = sorted(new_games, key=lambda x: int(x.get("end_time", 0) or 0))
            except Exception:
                new_games_sorted = new_games

            # Build payload rows (objects)
            rows_payload = [convert_game_to_obj(username, archive, g) for g in new_games_sorted]

            # Save to outputs folder
            try:
                out_file = save_rows_to_file(username, archive, rows_payload)
                print(f"Saved {len(rows_payload)} new games from {archive} -> {out_file}")
            except Exception as e:
                print(f"[ERROR] Failed to save output for {archive}: {e}")
                # Do not mark archive processed so it'll be retried
                continue

            # Update last_end_time: max end_time among sent games
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

            # polite delay before next archive
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
