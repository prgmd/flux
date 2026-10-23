{% macro flux_dates_sql(period) %}
    {% set name = 'flux_months' if period == 'month' else 'flux_years' %}
    {% set values = var(name, none) %}
    {% if values is not sequence or values is string or values is mapping %}
        {{ exceptions.raise_compiler_error(name ~ ' must be a list') }}
    {% endif %}
    {% for value in values %}
        {% set pattern = '(?!0000)[0-9]{4}-(?:0[1-9]|1[0-2])' if period == 'month' else '(?!0000)[0-9]{4}' %}
        {% if value is not string or not modules.re.fullmatch(pattern, value) %}
            {{ exceptions.raise_compiler_error('Invalid affected UTC period') }}
        {% endif %}
    {% endfor %}
    {% if values | length == 0 %}
        select cast(null as date) where false
    {% else %}
        {% for value in values %}
            '{{ value }}{{ '-01' if period == 'month' else '-01-01' }}'::date{{ ',' if not loop.last }}
        {% endfor %}
    {% endif %}
{% endmacro %}

{% macro flux_period_config(period) %}
    {% set enabled = var('flux_incremental', false) %}
    {% if enabled is not boolean %}
        {{ exceptions.raise_compiler_error('flux_incremental must be boolean') }}
    {% endif %}
    {% if enabled %}
        {{ config(materialized='incremental', incremental_strategy='append',
                  pre_hook='delete from ' ~ this ~ ' where period_start in (' ~ flux_dates_sql(period) ~ ')') }}
    {% endif %}
{% endmacro %}
