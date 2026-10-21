with inventory as (
    select
        inventory_snapshot_key,
        sku as product_id,
        warehouse_id,
        on_hand_qty,
        reserved_qty,
        available_to_promise_qty,
        snapshot_date,
        updated_timestamp
    from {{ ref('stg_threepl__inventory') }}
),

shipments_summary as (
    select
        sku as product_id,
        warehouse_id,
        cast(shipped_timestamp as date) as shipment_date,
        count(distinct shipment_id) as total_shipments_count
    from {{ ref('stg_threepl__shipments') }}
    where shipment_status in ('in_transit', 'delivered')
    group by 1, 2, 3
)

select
    inv.inventory_snapshot_key,
    inv.product_id,
    inv.warehouse_id,
    inv.snapshot_date,
    inv.on_hand_qty,
    inv.reserved_qty,
    inv.available_to_promise_qty,
    coalesce(shp.total_shipments_count, 0) as units_shipped_on_date,
    case
        when inv.available_to_promise_qty = 0 then 'OUT_OF_STOCK'
        when inv.available_to_promise_qty < 50 then 'LOW_STOCK_RISK'
        else 'HEALTHY'
    end as stock_health_status
from inventory inv
left join shipments_summary shp
    on inv.product_id = shp.product_id
   and inv.warehouse_id = shp.warehouse_id
   and inv.snapshot_date = shp.shipment_date
