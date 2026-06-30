{{ config(materialized='table') }}

SELECT
    md5(product_url) AS product_sk,
    product_name,
    product_brand,
    product_category,
    product_subcategory,
    product_url,
    product_has_image_url,
    product_image_url,
    product_has_ram,
    product_has_storage,
    product_ram,
    product_storage
FROM {{ ref('stg_all_sellers_products') }}

QUALIFY ROW_NUMBER() OVER(
    PARTITION BY product_url 
    ORDER BY date_id DESC, time_id DESC
) = 1