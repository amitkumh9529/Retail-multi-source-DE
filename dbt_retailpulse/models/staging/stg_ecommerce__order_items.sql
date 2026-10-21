with source as (
    select * from {{ source('raw_ecommerce', 'ecommerce_order_items_raw') }}
),

ranked as (
    select
        order_id,
        cast(line_item_id as integer) as line_item_id,
        product_id,
        cast(quantity as integer) as quantity,
        cast(unit_price as numeric(12,2)) as unit_price,
        cast(quantity * unit_price as numeric(12,2)) as extended_amount,
        cast(order_ts as timestamp_ntz) as order_timestamp,
        _extracted_at,
        _source_system,
        _batch_id,
        _record_hash,
        row_number() over (
            partition by order_id, line_item_id
            order by _extracted_at desc
        ) as rn
    from source
    where order_id is not null and line_item_id is not null
)

select
    {{ generate_surrogate_key(['order_id', 'line_item_id']) }} as order_item_key,
    order_id,
    line_item_id,
    product_id,
    quantity,
    unit_price,
    extended_amount,
    order_timestamp,
    _extracted_at,
    _source_system,
    _batch_id,
    _record_hash
from ranked
where rn = 1
