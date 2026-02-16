-- Analysis: Shipment Trends
-- Monthly shipment volume and revenue trends by supplier tier.

with shipments as (
    select
        f.order_date,
        f.quantity,
        f.line_total,
        f.shipping_cost,
        f.total_cost,
        f.delivery_days,
        f.is_on_time,
        s.supplier_name,
        s.supplier_tier,
        p.category as product_category
    from {{ ref('fct_shipments') }} f
    left join {{ ref('dim_suppliers') }}  s on f.supplier_key  = s.supplier_key
    left join {{ ref('dim_products') }}   p on f.product_key   = p.product_key
),

monthly_summary as (
    select
        date_trunc('month', order_date) as month,
        supplier_tier,
        count(*)                         as shipment_count,
        sum(quantity)                    as total_units,
        round(sum(line_total), 2)        as total_revenue,
        round(sum(shipping_cost), 2)     as total_shipping_cost,
        round(avg(delivery_days), 1)     as avg_delivery_days,
        round(
            100.0 * sum(case when is_on_time then 1 else 0 end)
            / nullif(count(case when is_on_time is not null then 1 end), 0),
            1
        ) as on_time_pct
    from shipments
    group by 1, 2
    order by 1, 2
)

select * from monthly_summary
