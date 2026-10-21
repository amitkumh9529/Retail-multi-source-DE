with source as (
    select * from {{ source('raw_ecommerce', 'ecommerce_orders_raw') }}
),

ranked as (
    select
        order_id,
        customer_id,
        cast(order_ts as timestamp_ntz) as order_timestamp,
        cast(updated_at as timestamp_ntz) as updated_timestamp,
        lower(trim(status)) as order_status,
        upper(trim(currency)) as currency_code,
        lower(trim(payment_method)) as payment_method,
        cast(total_amount as numeric(12,2)) as total_order_amount,
        _extracted_at,
        _source_system,
        _batch_id,
        _record_hash,
        row_number() over (
            partition by order_id
            order by updated_at desc, _extracted_at desc
        ) as rn
    from source
    where order_id is not null
)

select
    order_id,
    customer_id,
    order_timestamp,
    updated_timestamp,
    order_status,
    currency_code,
    payment_method,
    total_order_amount,
    _extracted_at,
    _source_system,
    _batch_id,
    _record_hash
from ranked
where rn = 1
