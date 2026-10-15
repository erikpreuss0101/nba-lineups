with source as (
    select * from {{ source('nba', 'box_teams') }}
)

select
    game_id || '-' || cast(team_id as varchar)      as box_team_key,
    game_id,
    team_id,
    team_tricode,
    team_city,
    team_name,
    is_home,
    score                                           as points,
    fieldGoalsMade                                  as fgm,
    fieldGoalsAttempted                             as fga,
    threePointersMade                               as fg3m,
    threePointersAttempted                          as fg3a,
    freeThrowsMade                                  as ftm,
    freeThrowsAttempted                             as fta,
    reboundsOffensive                               as oreb,
    reboundsDefensive                               as dreb,
    assists                                         as ast,
    steals                                          as stl,
    blocks                                          as blk,
    turnovers                                       as tov,
    foulsPersonal                                   as pf,
    periods                                         as periods_json,
    cast(ingested_at as timestamptz)                as ingested_at
from source
