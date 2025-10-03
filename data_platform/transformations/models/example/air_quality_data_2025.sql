{{ config(
    materialized='table',
    alias='aq_data_2025',
    schema='dbt_tables',
    post_hook="""
        COPY main_dbt_tables.aq_data_2025
        TO '/opt/dagster/app/data/parquet_files/aq_data_2025.parquet' (FORMAT PARQUET)
    """
) }}

select
    *
from {{ source("air_quality_aq", "gbgs_air_quality_data") }}