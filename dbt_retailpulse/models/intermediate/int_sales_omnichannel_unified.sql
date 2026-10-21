with ecomm_items as (
    select
        items.order_item_key as transaction_item_id,
        items.order_id as source_transaction_id,
        'ONLINE' as sales_channel,
        9999 as store_id, -- Virtual store ID for e-commerce
        orders.customer_id,
        items.product_id,
        items.quantity,
        items.unit_price,
        items.extended_amount as gross_revenue,
        orders.payment_method,
        orders.order_timestamp as transaction_timestamp,
        orders.updated_timestamp as updated_timestamp,
        false as is_refund,
        items._extracted_at,
        items._batch_id,
        items._record_hash
    from {{ ref('stg_ecommerce__order_items') }} items
    inner join {{ ref('stg_ecommerce__orders') }} orders
        on items.order_id = orders.order_id
    where orders.order_status not in ('cancelled')
),

pos_transactions as (
    select
        cast(transaction_id as varchar) as transaction_item_id,
        cast(transaction_id as varchar) as source_transaction_id,
        'IN_STORE' as sales_channel,
        store_id,
        customer_id,
        product_id,
        quantity,
        unit_price,
        gross_amount as gross_revenue,
        payment_method,
        transaction_timestamp,
        updated_timestamp,
        is_return_or_refund as is_refund,
        _extracted_at,
        _batch_id,
        _record_hash
    from {{ ref('stg_pos__transactions') }}
),

unioned as (
    select * from ecomm_items
    union all
    select * from pos_transactions
)

select
    {{ generate_surrogate_key(['sales_channel', 'transaction_item_id']) }} as omnichannel_transaction_key,
    transaction_item_id,
    source_transaction_id,
    sales_channel,
    store_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    gross_revenue,
    payment_method,
    transaction_timestamp,
    updated_timestamp,
    is_refund,
    _extracted_at,
    _batch_id,
    _record_hash
from unioned
