-- Data Quality Gate:
-- Asserts that staged inventory records do not have negative on_hand_qty.
-- Deliberate anomalies from the 3PL generator must be mitigated or quarantined.

select
    inventory_snapshot_key,
    sku,
    warehouse_id,
    on_hand_qty,
    snapshot_date
from {{ ref('stg_threepl__inventory') }}
where on_hand_qty < 0
