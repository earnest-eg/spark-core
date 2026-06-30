{{ config(
    materialized='table',
    cluster_by=['PRODUCT_NAME', 'PRODUCT_SELLER']
) }}

WITH cleaned AS (
    SELECT
        *,
        CASE 
            WHEN DATE > CURRENT_DATE() THEN CURRENT_DATE() 
            ELSE DATE 
        END AS DATE_FIXED
    FROM 
        {{ source('raw_data', 'STG_ALL_SELLERS_PRODUCTS') }}
    WHERE
        PRODUCT_NAME                    IS NOT NULL
        AND PRODUCT_SELLER              IS NOT NULL
        AND NOT (
            PRODUCT_CATEGORY            IS NULL 
                AND PRODUCT_SUBCATEGORY IS NULL
        )
)

SELECT DISTINCT
    * EXCLUDE (DATE, DATE_FIXED),
    DATE_FIXED AS DATE
FROM cleaned