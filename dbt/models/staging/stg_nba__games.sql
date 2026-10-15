with source as (
    select * from {{ source('nba', 'games') }}
)

select
    gameId                                          as game_id,
    '20' || substr(gameId, 4, 2) || '-'
        || lpad(cast(cast(substr(gameId, 4, 2) as integer) + 1 as varchar), 2, '0')
                                                    as season,
    case substr(gameId, 3, 1)
        when '1' then 'preseason'
        when '2' then 'regular_season'
        when '4' then 'playoffs'
        when '5' then 'play_in'
        when '6' then 'nba_cup_final'
        else 'other'
    end                                             as season_type,
    gameCode                                        as game_code,
    cast(gameTimeUTC as timestamptz)                as game_start_utc,
    cast(left(gameEt, 10) as date)                  as game_date_et,
    gameStatus                                      as game_status,
    period                                          as periods_played,
    greatest(period - 4, 0)                         as overtime_periods,
    2880 + greatest(period - 4, 0) * 300            as game_length_seconds,
    duration                                        as duration_minutes,
    attendance,
    sellout = '1'                                   as is_sellout,
    arena_arenaName                                 as arena_name,
    arena_arenaCity                                 as arena_city,
    arena_arenaState                                as arena_state,
    home_team_id,
    home_team_tricode,
    home_score,
    away_team_id,
    away_team_tricode,
    away_score,
    cast(ingested_at as timestamptz)                as ingested_at
from source
