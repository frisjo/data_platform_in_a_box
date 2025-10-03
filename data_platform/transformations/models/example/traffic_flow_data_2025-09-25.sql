{{ config(
    materialized='table',
    alias='tf_data_sep25',
    schema='dbt_tables',
    post_hook="""
        COPY main_dbt_tables.tf_data_sep25
        TO '/opt/dagster/app/data/parquet_files/tf_data_sep25.parquet' (FORMAT PARQUET)
    """
) }}

select
    measurement_time at time zone 'Europe/Berlin' as measurement_time_local,
    *
from {{ source("air_quality_tf", "tv_traffic_flow_data") }}
where measurement_time >= timestamp '2025-09-25 00:00:00'
  and measurement_time < timestamp '2025-09-26 00:00:00'
