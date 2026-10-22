{{ config(meta={'rule_ids': ['DQ013']}) }}
select song_key from {{ ref('stg_song_labels') }} l
join (
    select song_key, case when count(distinct variant) = 1 then min(variant) else 'conflicting_labels' end as expected
    from {{ ref('stg_song_dimension') }} group by song_key
) d using (song_key)
where l.variant is distinct from d.expected
