with source as (
    select * from {{ source('nba', 'schedule') }}
)

select
    gameId                                          as game_id,
    season_year                                     as season,
    gameCode                                        as game_code,
    gameStatus                                      as game_status,
    gameStatusText                                  as game_status_text,
    cast(gameDateTimeUTC as timestamptz)            as game_start_utc,
    cast(left(gameDateEst, 10) as date)             as game_date_et,
    weekNumber                                      as week_number,
    gameLabel                                       as game_label,
    gameSubLabel                                    as game_sub_label,
    arenaName                                       as arena_name,
    arenaCity                                       as arena_city,
    isNeutral                                       as is_neutral_site,
    home_teamId                                     as home_team_id,
    home_teamTricode                                as home_team_tricode,
    home_score,
    away_teamId                                     as away_team_id,
    away_teamTricode                                as away_team_tricode,
    away_score,
    cast(ingested_at as timestamptz)                as ingested_at
from source
