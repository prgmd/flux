{{ config(meta={'rule_ids': ['DQ011']}) }}
select period_start, song_key from {{ ref('monthly_song_metrics') }}
where listeners > listens or top_user_listens < 1 or top_user_listens > listens
   or not isfinite(top_user_share) or top_user_share <= 0 or top_user_share > 1
   or not isfinite(plays_per_listener) or plays_per_listener < 1
   or not isfinite(concentration_hhi) or concentration_hhi <= 0 or concentration_hhi > 1 + 1e-12
   or not isfinite(effective_listeners) or effective_listeners < 1 - 1e-12 or effective_listeners > listeners + 1e-12
   or top_user_share is null or plays_per_listener is null or concentration_hhi is null or effective_listeners is null
   or top_user_listens is null
