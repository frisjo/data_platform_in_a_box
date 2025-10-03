{{ config(
    materialized='table',
    alias='aq_data_sep25',
    schema='dbt_tables',
    post_hook="""
        COPY main_dbt_tables.aq_data_sep25
        TO '/opt/dagster/app/data/parquet_files/aq_data_sep25.parquet' (FORMAT PARQUET)
    """
) }}

select
    *
from {{ source("air_quality_aq", "gbgs_air_quality_data") }}
where date = '2025-09-25'