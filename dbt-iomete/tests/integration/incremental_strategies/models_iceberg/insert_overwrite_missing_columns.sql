{{ config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite'
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg, 'blue' as color
union all
select cast(2 as bigint) as id, 'goodbye' as msg, 'red' as color

{% else %}

-- reordered, and msg is gone: values map by name and msg becomes NULL

select 'green' as color, cast(3 as bigint) as id

{% endif %}
