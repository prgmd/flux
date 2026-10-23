{{ config(meta={'rule_ids': ['DQ012']}) }}
select period_start, song_key from (
    select *, rank() over (partition by period_start order by listeners desc) as expected_listeners,
        rank() over (partition by period_start order by listens desc) as expected_listens
    from {{ ref('yearly_song_metrics') }}
)
where listener_rank is distinct from expected_listeners or listen_rank is distinct from expected_listens
