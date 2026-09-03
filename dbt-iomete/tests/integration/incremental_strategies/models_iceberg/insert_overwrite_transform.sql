{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = 'days(ts)',
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, cast('2024-01-01 10:00:00' as timestamp) as ts, 'hello' as msg
union all
select cast(2 as bigint) as id, cast('2024-01-02 10:00:00' as timestamp) as ts, 'goodbye' as msg

{% else %}

-- only the 2024-01-02 partition is in the result, so the 2024-01-01 row must survive

select cast(3 as bigint) as id, cast('2024-01-02 11:00:00' as timestamp) as ts, 'yo' as msg

{% endif %}
