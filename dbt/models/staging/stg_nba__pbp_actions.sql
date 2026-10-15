with source as (
    select * from {{ source('nba', 'pbp_actions') }}
),

renamed as (
    select
        game_id || '-' || lpad(cast(actionNumber as varchar), 5, '0')
                                                    as pbp_action_key,
        game_id,
        actionNumber                                as action_number,
        orderNumber                                 as order_number,
        period,
        periodType                                  as period_type,
        {{ iso_duration_to_seconds('clock') }}      as clock_seconds_remaining,
        cast(timeActual as timestamptz)             as time_actual_utc,
        nullif(teamId, 0)                           as team_id,
        teamTricode                                 as team_tricode,
        nullif(personId, 0)                         as player_id,
        actionType                                  as action_type,
        subType                                     as sub_type,
        descriptor,
        qualifiers                                  as qualifiers_json,
        description,
        isFieldGoal = 1                             as is_field_goal,
        shotResult                                  as shot_result,
        shotDistance                                as shot_distance,
        x                                           as shot_x,
        y                                           as shot_y,
        nullif(possession, 0)                       as possession_team_id,
        try_cast(scoreHome as integer)              as score_home,
        try_cast(scoreAway as integer)              as score_away,
        nullif(assistPersonId, 0)                   as assist_player_id,
        nullif(stealPersonId, 0)                    as steal_player_id,
        nullif(blockPersonId, 0)                    as block_player_id,
        nullif(foulDrawnPersonId, 0)                as foul_drawn_player_id,
        personIdsFilter                             as person_ids_json,
        cast(ingested_at as timestamptz)            as ingested_at
    from source
)

select
    *,
    -- Seconds since tip-off: 12-minute quarters, 5-minute overtimes.
    case
        when period <= 4 then (period - 1) * 720 + (720 - clock_seconds_remaining)
        else 2880 + (period - 5) * 300 + (300 - clock_seconds_remaining)
    end                                             as game_seconds_elapsed
from renamed
