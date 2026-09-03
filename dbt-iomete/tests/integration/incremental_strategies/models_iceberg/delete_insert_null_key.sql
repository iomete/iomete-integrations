{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'id',
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(null as bigint) as id, 'null row' as msg

{% else %}

-- the null key must match the existing null key, or the target keeps both rows
select cast(null as bigint) as id, 'null updated' as msg
union all
select cast(2 as bigint) as id, 'new' as msg

{% endif %}
