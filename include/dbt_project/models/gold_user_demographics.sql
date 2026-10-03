{{ config(
    materialized='table'
) }}

SELECT 
    location_country AS country,
    gender,
    COUNT(_pipeline_record_hash) AS total_users,
    ROUND(AVG(CAST(dob_age AS NUMERIC)), 1) AS average_age,
    CURRENT_TIMESTAMP AS gold_processed_at
FROM {{ ref('silver_users') }}
GROUP BY 
    location_country,
    gender