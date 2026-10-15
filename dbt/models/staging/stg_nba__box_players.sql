with source as (
    select * from {{ source('nba', 'box_players') }}
)

select
    game_id || '-' || cast(personId as varchar)     as box_player_key,
    game_id,
    team_id,
    team_tricode,
    is_home,
    personId                                        as player_id,
    name                                            as player_name,
    firstName                                       as first_name,
    familyName                                      as last_name,
    jerseyNum                                       as jersey_number,
    position,
    starter = '1'                                   as is_starter,
    played = '1'                                    as did_play,
    status                                          as roster_status,
    notPlayingReason                                as not_playing_reason,
    {{ iso_duration_to_seconds('minutes') }}        as seconds_played,
    points,
    fieldGoalsMade                                  as fgm,
    fieldGoalsAttempted                             as fga,
    threePointersMade                               as fg3m,
    threePointersAttempted                          as fg3a,
    freeThrowsMade                                  as ftm,
    freeThrowsAttempted                             as fta,
    reboundsOffensive                               as oreb,
    reboundsDefensive                               as dreb,
    reboundsTotal                                   as reb,
    assists                                         as ast,
    steals                                          as stl,
    blocks                                          as blk,
    turnovers                                       as tov,
    foulsPersonal                                   as pf,
    plusMinusPoints                                 as plus_minus,
    cast(ingested_at as timestamptz)                as ingested_at
from source
