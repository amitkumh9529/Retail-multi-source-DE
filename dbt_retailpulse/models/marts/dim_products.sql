{{
    config(
        materialized='table'
    )
}}

with products as (
    select
        product_id,
        product_name,
        category,
        list_price
    from {{ ref('stg_pos__products') }}
),

sales_summary as (
    select
        product_id,
        sum(quantity) as total_units_sold,
        sum(gross_revenue) as total_gross_revenue,
        count(distinct store_id) as stores_selling_product
    from {{ ref('int_sales_omnichannel_unified') }}
    group by 1
)

select
    p.product_id,
    p.product_name,
    p.category,
    p.list_price,
    coalesce(s.total_units_sold, 0) as lifetime_units_sold,
    coalesce(s.total_gross_revenue, 0) as lifetime_gross_revenue,
    coalesce(s.stores_selling_product, 0) as store_distribution_count,
    case
        when coalesce(s.total_units_sold, 0) > 1000 then 'TOP_SELLER'
        when coalesce(s.total_units_sold, 0) > 100 then 'STEADY_PERFORMER'
        else 'LONG_TAIL'
    end as product_sales_velocity
from products p
left join sales_summary s on p.product_id = s.product_id
