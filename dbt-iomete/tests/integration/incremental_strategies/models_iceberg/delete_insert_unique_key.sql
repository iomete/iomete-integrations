{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'id',
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg

{% else %}

-- duplicate unique keys: merge would fail here, delete+insert keeps both rows
select cast(2 as bigint) as id, 'yo' as msg
union all
select cast(2 as bigint) as id, 'yo again' as msg
union all
select cast(3 as bigint) as id, 'anyway' as msg

{% endif %}
