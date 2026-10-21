with source as (
    select * from {{ source('raw_pos', 'pos_stores_raw') }}
),

ranked as (
    select
        cast(store_id as integer) as store_id,
        trim(store_name) as store_name,
        lower(trim(region)) as region,
        cast(opened_on as date) as opened_date,
        _extracted_at,
        _source_system,
        _batch_id,
        _record_hash,
        row_number() over (
            partition by store_id
            order by _extracted_at desc
        ) as rn
    from source
    where store_id is not null
)

select
    store_id,
    store_name,
    region,
    opened_date,
    _extracted_at,
    _source_system,
    _batch_id,
    _record_hash
from ranked
where rn = 1
