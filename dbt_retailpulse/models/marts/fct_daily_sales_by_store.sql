{{
    config(
        materialized='table',
        cluster_by=['transaction_date', 'store_id']
    )
}}

with transactions as (
    select * from {{ ref('fct_omnichannel_transactions') }}
)

select
    {{ generate_surrogate_key(['transaction_date', 'store_id', 'sales_channel']) }} as daily_store_sales_key,
    transaction_date,
    store_id,
    store_name,
    store_region,
    sales_channel,
    count(distinct source_transaction_id) as total_transactions_count,
    count(distinct customer_id) as unique_shoppers_count,
    sum(quantity) as total_units_sold,
    sum(case when is_refund then 1 else 0 end) as total_returns_count,
    sum(gross_revenue) as total_gross_revenue,
    sum(net_revenue) as total_net_revenue,
    round(sum(net_revenue) / nullif(count(distinct source_transaction_id), 0), 2) as average_order_value
from transactions
group by 1, 2, 3, 4, 5, 6
