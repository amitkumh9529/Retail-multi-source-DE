{{
    config(
        materialized='incremental',
        unique_key='omnichannel_transaction_key',
        incremental_strategy='merge',
        cluster_by=['transaction_date', 'store_id'],
        on_schema_change='sync_all_columns'
    )
}}

with incremental_sales as (
    select * from {{ ref('int_sales_omnichannel_unified') }}
    {% if is_incremental() %}
        -- Idempotent lookback window captures late updates while avoiding full-table scans
        where updated_timestamp >= (
            select dateadd(hour, -2, coalesce(max(updated_timestamp), '1970-01-01'::timestamp_ntz))
            from {{ this }}
        )
    {% endif %}
),

products as (
    select product_id, product_name, category, list_price
    from {{ ref('stg_pos__products') }}
),

stores as (
    select store_id, store_name, region
    from {{ ref('stg_pos__stores') }}
)

select
    s.omnichannel_transaction_key,
    s.transaction_item_id,
    s.source_transaction_id,
    s.sales_channel,
    s.store_id,
    coalesce(st.store_name, case when s.sales_channel = 'ONLINE' then 'E-Commerce Web & Mobile' else 'Unknown Store' end) as store_name,
    coalesce(st.region, 'DIGITAL') as store_region,
    s.customer_id,
    s.product_id,
    coalesce(p.product_name, 'Unknown Product') as product_name,
    coalesce(p.category, 'Uncategorized') as product_category,
    s.quantity,
    s.unit_price,
    s.gross_revenue,
    case
        when s.is_refund then -abs(s.gross_revenue)
        else s.gross_revenue
    end as net_revenue,
    s.payment_method,
    cast(s.transaction_timestamp as date) as transaction_date,
    s.transaction_timestamp,
    s.updated_timestamp,
    s.is_refund,
    s._extracted_at,
    s._batch_id,
    s._record_hash,
    current_timestamp() as _transformed_at
from incremental_sales s
left join products p on s.product_id = p.product_id
left join stores st on s.store_id = st.store_id
