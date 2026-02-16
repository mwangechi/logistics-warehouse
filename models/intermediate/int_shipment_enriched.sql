-- Intermediate: Enriched Shipments
-- Join shipments with supplier and product context for the mart layer.

with shipments as (
    select * from {{ ref('stg_shipments') }}
),

suppliers as (
    select * from {{ ref('stg_suppliers') }}
),

inventory as (
    select * from {{ ref('stg_inventory') }}
),

enriched as (
    select
        -- Shipment core
        s.shipment_id,
        s.order_date,
        s.ship_date,
        s.delivery_date,
        s.quantity,
        s.unit_price,
        s.shipping_cost,
        s.line_total,
        s.shipment_status,
        s.origin_country,
        s.destination_country,
        s.delivery_days,
        s.warehouse_id,

        -- Supplier context
        s.supplier_id,
        sup.supplier_name,
        sup.country_code   as supplier_country,
        sup.supplier_tier,
        sup.reliability_score,
        sup.lead_time_days as supplier_lead_time,

        -- Product context
        s.product_id,
        inv.product_name,
        inv.category       as product_category,
        inv.subcategory     as product_subcategory,
        inv.weight_kg       as product_weight_kg,
        inv.is_hazardous,

        -- Metrics
        s.shipping_cost / nullif(s.quantity, 0) as cost_per_unit_shipped,
        s.line_total + s.shipping_cost          as total_cost,

        -- On-time flag (within supplier lead time)
        case
            when s.delivery_days is not null
                and s.delivery_days <= sup.lead_time_days
                then true
            when s.delivery_days is not null
                then false
        end as is_on_time

    from shipments s
    left join suppliers sup on s.supplier_id = sup.supplier_id
    left join inventory inv on s.product_id  = inv.product_id
)

select * from enriched
