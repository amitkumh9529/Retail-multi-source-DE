with source as (
    select * from {{ source('raw_threepl', 'threepl_shipments_raw') }}
),

ranked as (
    select
        trim(shipment_id) as shipment_id,
        trim(sku) as sku,
        trim(warehouse_id) as warehouse_id,
        upper(trim(carrier)) as carrier,
        lower(trim(status)) as shipment_status,
        cast(shipped_at as timestamp_ntz) as shipped_timestamp,
        cast(updated_at as timestamp_ntz) as updated_timestamp,
        _extracted_at,
        _source_system,
        _batch_id,
        _record_hash,
        row_number() over (
            partition by shipment_id
            order by updated_at desc, _extracted_at desc
        ) as rn
    from source
    where shipment_id is not null
)

select
    shipment_id,
    sku,
    warehouse_id,
    carrier,
    shipment_status,
    shipped_timestamp,
    updated_timestamp,
    _extracted_at,
    _source_system,
    _batch_id,
    _record_hash
from ranked
where rn = 1
