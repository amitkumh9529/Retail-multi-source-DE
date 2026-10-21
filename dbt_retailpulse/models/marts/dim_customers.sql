{{
    config(
        materialized='table'
    )
}}

with sales as (
    select * from {{ ref('fct_omnichannel_transactions') }}
),

customer_metrics as (
    select
        customer_id,
        min(transaction_date) as first_transaction_date,
        max(transaction_date) as last_transaction_date,
        count(distinct source_transaction_id) as lifetime_orders_count,
        sum(quantity) as lifetime_units_purchased,
        sum(net_revenue) as lifetime_net_spend,
        round(sum(net_revenue) / nullif(count(distinct source_transaction_id), 0), 2) as lifetime_average_order_value,
        count(distinct case when sales_channel = 'ONLINE' then source_transaction_id end) as online_orders_count,
        count(distinct case when sales_channel = 'IN_STORE' then source_transaction_id end) as instore_orders_count
    from sales
    where customer_id is not null
    group by 1
)

select
    customer_id,
    first_transaction_date,
    last_transaction_date,
    lifetime_orders_count,
    lifetime_units_purchased,
    lifetime_net_spend,
    lifetime_average_order_value,
    online_orders_count,
    instore_orders_count,
    case
        when online_orders_count > 0 and instore_orders_count > 0 then 'TRUE_OMNICHANNEL'
        when online_orders_count > 0 then 'DIGITAL_ONLY'
        else 'STORE_ONLY'
    end as customer_channel_segment,
    case
        when lifetime_net_spend >= 1500 then 'VIP_TIER'
        when lifetime_net_spend >= 500 then 'GOLD_TIER'
        else 'STANDARD_TIER'
    end as customer_value_tier
from customer_metrics
