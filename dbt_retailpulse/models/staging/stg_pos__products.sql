with source as (
    select * from {{ source('raw_pos', 'pos_products_raw') }}
),

ranked as (
    select
        trim(product_id) as product_id,
        trim(product_name) as product_name,
        lower(trim(category)) as category,
        cast(list_price as numeric(12,2)) as list_price,
        _extracted_at,
        _source_system,
        _batch_id,
        _record_hash,
        row_number() over (
            partition by product_id
            order by _extracted_at desc
        ) as rn
    from source
    where product_id is not null
)

select
    product_id,
    product_name,
    category,
    list_price,
    _extracted_at,
    _source_system,
    _batch_id,
    _record_hash
from ranked
where rn = 1
