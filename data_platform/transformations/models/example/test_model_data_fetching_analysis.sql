{{ config(
    materialized='table',
    alias='test_table_data_fetching_analysis',
    schema='dbt_tables',
    post_hook="""
        COPY main_dbt_tables.test_table_data_fetching_analysis
        TO '/opt/dagster/app/data/parquet_files/test_table_data_fetching_analysis.parquet' (FORMAT PARQUET)
    """
) }}

select
    measurement_time at time zone 'Europe/Berlin' as measurement_time_local,
    *
from {{ source("air_quality_tf", "tv_traffic_flow_data") }}
where measurement_time >= timestamp '2025-09-23 00:00:00'
  and measurement_time < timestamp '2025-09-24 00:00:00'
