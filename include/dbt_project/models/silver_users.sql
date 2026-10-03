{{ config(
    materialized='incremental',
    unique_key='_pipeline_record_hash'
) }}

SELECT 
    *,
    CURRENT_TIMESTAMP AS silver_processed_at
FROM public.raw_users_bronze

{% if is_incremental() %}
    -- Only process records that haven't been transformed yet
    -- We join against the existing silver table using the hash to find new rows
    WHERE _pipeline_record_hash NOT IN (SELECT _pipeline_record_hash FROM {{ this }})
{% endif %}