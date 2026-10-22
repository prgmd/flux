{{ config(meta={'rule_ids': ['DQ003']}) }}
select * from {{ ref('stg_monthly') }}
where not regexp_full_match(listened_month, '[0-9]{4}-[0-9]{2}')
   or try_cast(listened_month || '-01' as date) is null
