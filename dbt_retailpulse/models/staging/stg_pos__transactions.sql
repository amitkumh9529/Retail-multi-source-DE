with source as (
    select * from {{ source('raw_pos', 'pos_transactions_raw') }}
),

ranked as (
    select
        cast(transaction_id as bigint) as transaction_id,
        cast(store_id as integer) as store_id,
        trim(product_id) as product_id,
        trim(customer_id) as customer_id,
        cast(quantity as integer) as quantity,
        cast(unit_price as numeric(12,2)) as unit_price,
        cast(quantity * unit_price as numeric(12,2)) as gross_amount,
        cast(txn_ts as timestamp_ntz) as transaction_timestamp,
        cast(updated_at as timestamp_ntz) as updated_timestamp,
        lower(trim(payment_method)) as payment_method,
        case when quantity < 0 then true else false end as is_return_or_refund,
        _extracted_at,
        _source_system,
        _batch_id,
        _record_hash,
        row_number() over (
            partition by transaction_id
            order by updated_at desc, _extracted_at desc
        ) as rn
    from source
    where transaction_id is not null
)

select
    transaction_id,
    store_id,
    product_id,
    customer_id,
    quantity,
    unit_price,
    gross_amount,
    transaction_timestamp,
    updated_timestamp,
    payment_method,
    is_return_or_refund,
    _extracted_at,
    _source_system,
    _batch_id,
    _record_hash
from ranked
where rn = 1
