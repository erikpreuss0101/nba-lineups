"""
NBA ingestion for the nba-lineups dbt project.

Pulls three feeds from NBA's public CDN (cdn.nba.com), which works from home
connections and from CI runners alike:

  schedule    staticData/scheduleLeagueV2.json        (current season only)
  boxscore    liveData/boxscore/boxscore_{game_id}.json
  playbyplay  liveData/playbyplay/playbyplay_{game_id}.json

Every response is saved untouched as gzipped JSON under data/raw_json/, then
flattened into parquet under data/raw/<table>/season=<season>/. dbt reads the
parquet; nothing else is transformed here. Reruns skip games already landed.

Usage (from the repo root):
  python ingest/nba_ingest.py backfill --season 2025-26 --limit 5   # smoke test
  python ingest/nba_ingest.py backfill --season 2025-26   # a past regular season
  python ingest/nba_ingest.py current                     # finished games this season
  python ingest/nba_ingest.py rebuild                     # re-flatten saved JSON, no network
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
RAW_JSON = ROOT / "data" / "raw_json"
RAW = ROOT / "data" / "raw"

CDN = "https://cdn.nba.com/static/json"
SCHEDULE_URL = f"{CDN}/staticData/scheduleLeagueV2.json"
BOXSCORE_URL = f"{CDN}/liveData/boxscore/boxscore_{{game_id}}.json"
PBP_URL = f"{CDN}/liveData/playbyplay/playbyplay_{{game_id}}.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
}

REGULAR_SEASON_GAMES = 1230  # 30 teams x 82 games / 2
SLEEP_SECONDS = 0.8

log = logging.getLogger("nba_ingest")
session = requests.Session()
session.headers.update(HEADERS)


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def season_prefix(season: str) -> str:
    """'2025-26' -> '25' (the two digits NBA game IDs use for a season)."""
    return season[2:4]


def season_from_game_id(game_id: str) -> str:
    """'0022500001' -> '2025-26'."""
    start = 2000 + int(game_id[3:5])
    return f"{start}-{str(start + 1)[2:]}"


def regular_season_game_ids(season: str) -> list[str]:
    p = season_prefix(season)
    return [f"002{p}{n:05d}" for n in range(1, REGULAR_SEASON_GAMES + 1)]


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_json(url: str, retries: int = 4) -> dict | None:
    """GET a JSON document. Returns None on 403/404 (game not published)."""
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code in (403, 404):
                return None
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            wait = 2**attempt + random.random()
            log.warning("attempt %s failed for %s (%s); retrying in %.1fs",
                        attempt + 1, url, exc, wait)
            time.sleep(wait)
    raise RuntimeError(f"gave up on {url}")


def save_raw_json(feed: str, season: str, key: str, payload: dict) -> Path:
    path = RAW_JSON / feed / season / f"{key}.json.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        json.dump(payload, fh)
    return path


def load_raw_json(path: Path) -> dict:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return json.load(fh)


def write_parquet(table: str, season: str, key: str, rows: list[dict]) -> None:
    if not rows:
        return
    df = pd.DataFrame(rows)
    # Lists and dicts become JSON strings so every file has a stable, simple schema.
    for col in df.columns:
        if df[col].map(lambda v: isinstance(v, (list, dict))).any():
            df[col] = df[col].map(
                lambda v: json.dumps(v) if isinstance(v, (list, dict)) else v
            )
    path = RAW / table / f"season={season}" / f"{key}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def flat(prefix: str, obj: dict | None) -> dict:
    """Flatten one level of nesting: {'name': 'x'} -> {'arena_name': 'x'}."""
    return {f"{prefix}_{k}": v for k, v in (obj or {}).items()
            if not isinstance(v, (list, dict))}


# --------------------------------------------------------------------------- #
# flatteners: raw JSON -> rows. Keep these dumb; modeling happens in dbt.
# --------------------------------------------------------------------------- #
def flatten_boxscore(payload: dict, ingested_at: str) -> dict[str, list[dict]]:
    g = payload["game"]
    game_id = g["gameId"]

    game_row = {k: v for k, v in g.items() if not isinstance(v, (list, dict))}
    game_row.update(flat("arena", g.get("arena")))
    for side in ("homeTeam", "awayTeam"):
        t = g[side]
        short = "home" if side == "homeTeam" else "away"
        game_row[f"{short}_team_id"] = t.get("teamId")
        game_row[f"{short}_team_tricode"] = t.get("teamTricode")
        game_row[f"{short}_score"] = t.get("score")
    game_row["officials"] = g.get("officials", [])
    game_row["ingested_at"] = ingested_at

    team_rows, player_rows = [], []
    for side in ("homeTeam", "awayTeam"):
        t = g[side]
        base = {
            "game_id": game_id,
            "team_id": t.get("teamId"),
            "team_tricode": t.get("teamTricode"),
            "is_home": side == "homeTeam",
            "ingested_at": ingested_at,
        }
        team_rows.append({
            **base,
            "team_name": t.get("teamName"),
            "team_city": t.get("teamCity"),
            "score": t.get("score"),
            "periods": t.get("periods", []),
            **(t.get("statistics") or {}),
        })
        for p in t.get("players", []):
            row = {**base}
            row.update({k: v for k, v in p.items() if k != "statistics"})
            row.update(p.get("statistics") or {})
            player_rows.append(row)

    return {"games": [game_row], "box_teams": team_rows, "box_players": player_rows}


def flatten_pbp(payload: dict, ingested_at: str) -> dict[str, list[dict]]:
    g = payload["game"]
    rows = [
        {"game_id": g["gameId"], **action, "ingested_at": ingested_at}
        for action in g.get("actions", [])
    ]
    return {"pbp_actions": rows}


def flatten_schedule(payload: dict, ingested_at: str) -> list[dict]:
    sched = payload["leagueSchedule"]
    rows = []
    for day in sched.get("gameDates", []):
        for game in day.get("games", []):
            row = {k: v for k, v in game.items() if not isinstance(v, (list, dict))}
            row.update(flat("home", game.get("homeTeam")))
            row.update(flat("away", game.get("awayTeam")))
            row["season_year"] = sched.get("seasonYear")
            row["ingested_at"] = ingested_at
            rows.append(row)
    return rows


# --------------------------------------------------------------------------- #
# commands
# --------------------------------------------------------------------------- #
def already_landed(game_id: str) -> bool:
    season = season_from_game_id(game_id)
    return (RAW / "pbp_actions" / f"season={season}" / f"{game_id}.parquet").exists()


def ingest_game(game_id: str) -> bool:
    """Fetch, save and flatten one game. Returns False if the game isn't published."""
    season = season_from_game_id(game_id)
    box = fetch_json(BOXSCORE_URL.format(game_id=game_id))
    time.sleep(SLEEP_SECONDS)
    if box is None:
        return False
    pbp = fetch_json(PBP_URL.format(game_id=game_id))
    time.sleep(SLEEP_SECONDS)
    if pbp is None:
        return False

    save_raw_json("boxscore", season, game_id, box)
    save_raw_json("playbyplay", season, game_id, pbp)

    ts = now_utc()
    tables = {**flatten_boxscore(box, ts), **flatten_pbp(pbp, ts)}
    for table, rows in tables.items():
        write_parquet(table, season, game_id, rows)
    return True


def run_games(game_ids: list[str]) -> None:
    todo = [gid for gid in game_ids if not already_landed(gid)]
    log.info("%s games requested, %s new", len(game_ids), len(todo))
    missing = 0
    for i, gid in enumerate(todo, 1):
        if not ingest_game(gid):
            missing += 1
            log.warning("%s not available", gid)
        if i % 25 == 0:
            log.info("%s / %s done", i, len(todo))
    log.info("finished: %s landed, %s unavailable", len(todo) - missing, missing)


def cmd_backfill(season: str, limit: int | None = None) -> None:
    ids = regular_season_game_ids(season)
    run_games(ids[:limit] if limit else ids)


def cmd_current() -> None:
    payload = fetch_json(SCHEDULE_URL)
    if payload is None:
        raise RuntimeError("schedule not available")
    ts = now_utc()
    season = payload["leagueSchedule"]["seasonYear"]
    save_raw_json("schedule", season, "schedule", payload)
    rows = flatten_schedule(payload, ts)
    write_parquet("schedule", season, "schedule", rows)

    # gameStatus 3 = final. Game IDs starting 002 = regular season.
    finals = [r["gameId"] for r in rows
              if r.get("gameStatus") == 3 and str(r["gameId"]).startswith("002")]
    run_games(finals)


def cmd_rebuild() -> None:
    """Re-flatten every saved JSON file. Use after changing a flattener."""
    for path in sorted((RAW_JSON / "boxscore").glob("*/*.json.gz")):
        season = path.parent.name
        game_id = path.name.split(".")[0]
        ts = now_utc()
        for table, rows in flatten_boxscore(load_raw_json(path), ts).items():
            write_parquet(table, season, game_id, rows)
        pbp_path = RAW_JSON / "playbyplay" / season / path.name
        if pbp_path.exists():
            for table, rows in flatten_pbp(load_raw_json(pbp_path), ts).items():
                write_parquet(table, season, game_id, rows)
    for path in sorted((RAW_JSON / "schedule").glob("*/schedule.json.gz")):
        season = path.parent.name
        write_parquet("schedule", season, "schedule",
                      flatten_schedule(load_raw_json(path), now_utc()))
    log.info("rebuild complete")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    bf = sub.add_parser("backfill", help="pull every regular-season game for a past season")
    bf.add_argument("--season", required=True, help="e.g. 2025-26")
    bf.add_argument("--limit", type=int, help="only the first N games (smoke test)")
    sub.add_parser("current", help="pull the schedule and every finished game this season")
    sub.add_parser("rebuild", help="re-flatten saved JSON into parquet (no network)")
    args = parser.parse_args()

    if args.command == "backfill":
        cmd_backfill(args.season, args.limit)
    elif args.command == "current":
        cmd_current()
    else:
        cmd_rebuild()


if __name__ == "__main__":
    main()
