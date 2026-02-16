-- Custom Test: Assert Shipment Positive Quantity
-- Ensures no shipment has zero or negative quantity.

select
    shipment_id,
    quantity
from {{ ref('fct_shipments') }}
where quantity <= 0
