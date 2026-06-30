{{ config(materialized='table') }}

SELECT
    md5(concat(s.product_url, cast(s.date_id as varchar), cast(s.time_id as varchar), s.product_seller)) AS fact_id,
    p.product_sk,
    sl.seller_sk,
    d.date_id,
    t.time_id,

    s.product_current_price,
    s.product_old_price,
    s.product_discount_amount,
    s.product_discount_percentage,
    s.product_has_discount,
    s.product_availability,
    s.product_count,
    s.product_weight,
    s.product_measuring_unit

FROM
    {{ ref('stg_all_sellers_products') }} s
JOIN {{ ref('dim_product') }} p
    ON s.product_url = p.product_url
JOIN {{ ref('dim_seller') }} sl
    ON s.product_seller = sl.product_seller
JOIN {{ ref('dim_date') }} d
    ON s.date_id = d.date_id
JOIN {{ ref('dim_time') }} t
    ON s.time_id = t.time_id

QUALIFY ROW_NUMBER() OVER(
    PARTITION BY s.product_url, s.date_id, s.time_id
    ORDER BY s.date_id DESC
) = 1