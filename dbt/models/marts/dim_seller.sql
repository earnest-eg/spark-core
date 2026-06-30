{{ config(materialized='table') }}

SELECT DISTINCT
    md5(product_seller) AS seller_sk,
    product_seller,
    product_is_talabat_seller
FROM 
    {{ ref('stg_all_sellers_products') }}
