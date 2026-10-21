{{
    config(
        materialized='table'
    )
}}

with orders as (
    select
        order_id,
        customer_id,
        order_timestamp,
        order_status
    from {{ ref('stg_ecommerce__orders') }}
),

shipments as (
    select
        shipment_id,
        sku as product_id,
        warehouse_id,
        carrier,
        shipment_status,
        shipped_timestamp
    from {{ ref('stg_threepl__shipments') }}
),

order_items as (
    select
        order_id,
        product_id,
        quantity
    from {{ ref('stg_ecommerce__order_items') }}
),

joined as (
    select
        oi.order_id,
        o.customer_id,
        oi.product_id,
        s.shipment_id,
        s.warehouse_id,
        s.carrier,
        o.order_timestamp,
        s.shipped_timestamp,
        s.shipment_status,
        datediff('hour', o.order_timestamp, s.shipped_timestamp) as fulfillment_duration_hours
    from order_items oi
    inner join orders o on oi.order_id = o.order_id
    left join shipments s on oi.product_id = s.product_id
)

select
    {{ generate_surrogate_key(['order_id', 'product_id']) }} as fulfillment_key,
    order_id,
    customer_id,
    product_id,
    coalesce(shipment_id, 'PENDING_DISPATCH') as shipment_id,
    warehouse_id,
    carrier,
    order_timestamp,
    shipped_timestamp,
    fulfillment_duration_hours,
    case
        when fulfillment_duration_hours <= 24 then 'MET_24H_SLA'
        when fulfillment_duration_hours <= 48 then 'MET_48H_SLA'
        when fulfillment_duration_hours > 48 then 'SLA_BREACHED'
        else 'UNFULFILLED'
    end as fulfillment_sla_tier
from joined
