with source as (
    select * from {{ source('raw_threepl', 'threepl_inventory_raw') }}
),

ranked as (
    select
        trim(sku) as sku,
        trim(warehouse_id) as warehouse_id,
        cast(on_hand_qty as integer) as on_hand_qty,
        cast(reserved_qty as integer) as reserved_qty,
        cast(snapshot_date as date) as snapshot_date,
        cast(updated_at as timestamp_ntz) as updated_timestamp,
        case
            when on_hand_qty < 0 then true
            else false
        end as is_anomalous_inventory,
        _extracted_at,
        _source_system,
        _batch_id,
        _record_hash,
        row_number() over (
            partition by sku, warehouse_id, snapshot_date
            order by updated_at desc, _extracted_at desc
        ) as rn
    from source
    where nullif(trim(sku), '') is not null
      and warehouse_id != 'WH-UNKNOWN'
)

select
    {{ generate_surrogate_key(['sku', 'warehouse_id', 'snapshot_date']) }} as inventory_snapshot_key,
    sku,
    warehouse_id,
    greatest(on_hand_qty, 0) as on_hand_qty,
    greatest(reserved_qty, 0) as reserved_qty,
    greatest(on_hand_qty - reserved_qty, 0) as available_to_promise_qty,
    snapshot_date,
    updated_timestamp,
    is_anomalous_inventory,
    _extracted_at,
    _source_system,
    _batch_id,
    _record_hash
from ranked
where rn = 1
