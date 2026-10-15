# nba-lineups

A dbt + DuckDB warehouse that rebuilds every NBA lineup from raw play-by-play,
then models lineup net rating, player on/off, and rest effects.

> Status: ingestion and staging layer complete. Stint reconstruction is next.

## Stack

| Layer | Tool |
|---|---|
| Ingestion | Python (`ingest/nba_ingest.py`) pulling NBA's public CDN feeds |
| Storage | Parquet files, one per game, partitioned by season |
| Warehouse | DuckDB (reads the parquet in place) |
| Transformation | dbt Core 1.x with `dbt-duckdb` |
| Presentation | Evidence.dev (planned) |

## Data sources

All three feeds come from `cdn.nba.com`, which serves the same JSON that
powers NBA.com's live game pages:

| Feed | Grain | Landed as |
|---|---|---|
| League schedule | game | `data/raw/schedule/` |
| Live box score | game, team, player | `data/raw/games/`, `box_teams/`, `box_players/` |
| Live play-by-play | action | `data/raw/pbp_actions/` |

Raw responses are kept untouched under `data/raw_json/`, so the parquet can be
rebuilt at any time without hitting the network (`python ingest/nba_ingest.py rebuild`).

## Getting started

Requires Python 3.10+.

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 1. Smoke test: pull 5 games
python ingest/nba_ingest.py backfill --season 2025-26 --limit 5

# 2. Build and test the models
cd dbt
dbt build --profiles-dir .

# 3. Full backfill of a season (~1,230 games, roughly 45 minutes)
cd ..
python ingest/nba_ingest.py backfill --season 2025-26

# 4. During the season: pull every finished game not yet landed
python ingest/nba_ingest.py current
```

To explore the data directly:

```bash
cd dbt
duckdb nba.duckdb
```
```sql
select * from stg_nba__pbp_actions where action_type = 'substitution' limit 20;
```

## Project structure

```
ingest/              Python ingestion (no transformation)
data/raw_json/       untouched API responses (gitignored)
data/raw/            flattened parquet, one file per game (gitignored)
dbt/
  models/staging/    one model per source: rename, cast, parse durations
  models/intermediate/  stints, possessions, rest features (next)
  models/marts/      lineup and on/off facts, dimensions (next)
  macros/            iso_duration_to_seconds
  tests/             singular data tests
```

## Design notes

- **Ingestion stays dumb.** Python only fetches and flattens one level of JSON.
  All typing, renaming, and logic live in dbt so they're versioned, tested, and
  visible in the lineage graph.
- **One parquet file per game** makes reruns idempotent: a game is skipped if
  its file already exists, and DuckDB reads the folder as one table.
- **Validation against official minutes.** `assert_team_minutes_match_game_length`
  checks that each team's player minutes sum to five times the game length.
  The same check will validate reconstructed stints.

## Roadmap

- [x] Ingestion for schedule, box scores, and play-by-play
- [x] Staging models with tests and source freshness
- [ ] `int_stints`: five-man units rebuilt from substitution events
- [ ] `int_possessions`: possession boundaries and offensive team
- [ ] Player-team snapshots (SCD Type 2) for trades and signings
- [ ] Marts: lineup stints, player on/off, rest and travel
- [ ] Nightly GitHub Actions build and Evidence site
