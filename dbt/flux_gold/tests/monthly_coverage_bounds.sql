{{ config(meta={'rule_ids': ['DQ011']}) }}
select period_start from {{ ref('monthly_coverage') }}
where linked_listens is null or linked_users is null or artist_coverage is null
   or linked_listens < 0 or linked_listens > period_listens
   or linked_users < 0 or linked_users > period_users or linked_users > linked_listens
   or not isfinite(artist_coverage) or artist_coverage < 0 or artist_coverage > 1
   or abs(artist_coverage - linked_listens::double / period_listens) > 1e-12
