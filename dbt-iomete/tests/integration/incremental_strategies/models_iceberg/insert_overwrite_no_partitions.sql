{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite'
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg

{% else %}

-- no partition_by: the overwrite replaces every row in the table

select cast(2 as bigint) as id, 'yo' as msg
union all
select cast(3 as bigint) as id, 'anyway' as msg

{% endif %}
