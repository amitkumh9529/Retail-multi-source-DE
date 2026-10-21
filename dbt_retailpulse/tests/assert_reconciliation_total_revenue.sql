-- Financial reconciliation test:
-- Asserts that the sum of daily store net revenues equals the transactional net revenue in the fact table.
-- Any discrepancy indicates an aggregation or grouping leak.

with daily_store_revenue as (
    select
        round(sum(total_net_revenue), 2) as daily_net_rev
    from {{ ref('fct_daily_sales_by_store') }}
),

transactional_revenue as (
    select
        round(sum(net_revenue), 2) as txn_net_rev
    from {{ ref('fct_omnichannel_transactions') }}
)

select
    daily_net_rev,
    txn_net_rev,
    abs(daily_net_rev - txn_net_rev) as diff_amount
from daily_store_revenue, transactional_revenue
where abs(daily_net_rev - txn_net_rev) > 0.05
