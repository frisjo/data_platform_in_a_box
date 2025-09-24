{{ config(
    materialized='table',
    alias='test_table',
    schema='dbt_tables'
) }}

select *
from {{ source("air_quality_tf", "tv_traffic_flow_data") }}
where site_id = 2197