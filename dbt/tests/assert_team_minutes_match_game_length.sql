-- Each team's player minutes should add up to 5 players x game length
-- (48 minutes plus 5 per overtime). This is also the check you'll reuse to
-- validate stint reconstruction later. Returns failing team-games.

with team_minutes as (
    select game_id, team_id, sum(seconds_played) as player_seconds
    from {{ ref('stg_nba__box_players') }}
    group by 1, 2
)

select
    t.game_id,
    t.team_id,
    t.player_seconds,
    g.game_length_seconds * 5 as expected_seconds
from team_minutes t
join {{ ref('stg_nba__games') }} g using (game_id)
where abs(t.player_seconds - g.game_length_seconds * 5) > 60
