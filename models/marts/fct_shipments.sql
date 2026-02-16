-- Fact: Shipments
-- Central fact table with measures and dimension foreign keys.

with enriched as (
    select * from {{ ref('int_shipment_enriched') }}
),

dim_suppliers as (
    select supplier_id, supplier_key from {{ ref('dim_suppliers') }}
),

dim_products as (
    select product_id, product_key from {{ ref('dim_products') }}
),

dim_warehouses as (
    select warehouse_id, warehouse_key from {{ ref('dim_warehouses') }}
),

final as (
    select
        {{ generate_surrogate_key(['e.shipment_id']) }} as shipment_key,

        -- Dimension keys
        ds.supplier_key,
        dp.product_key,
        dw.warehouse_key,

        -- Degenerate dimensions
        e.shipment_id,
        e.shipment_status,
        e.origin_country,
        e.destination_country,

        -- Date dimensions
        e.order_date,
        e.ship_date,
        e.delivery_date,

        -- Measures
        e.quantity,
        e.unit_price,
        e.shipping_cost,
        e.line_total,
        e.total_cost,
        e.cost_per_unit_shipped,
        e.delivery_days,
        e.is_on_time

    from enriched e
    left join dim_suppliers  ds on e.supplier_id  = ds.supplier_id
    left join dim_products   dp on e.product_id   = dp.product_id
    left join dim_warehouses dw on e.warehouse_id  = dw.warehouse_id
)

select * from final
