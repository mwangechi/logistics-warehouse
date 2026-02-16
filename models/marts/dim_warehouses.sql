-- Dimension: Warehouses
-- Derived from inventory and shipment data.

with inventory as (
    select * from {{ ref('stg_inventory') }}
),

warehouse_products as (
    select
        warehouse_id,
        count(distinct product_id) as product_count,
        sum(quantity_on_hand) as total_units_on_hand,
        sum(quantity_on_hand * unit_cost) as total_inventory_value,
        sum(case when is_hazardous then 1 else 0 end) as hazardous_product_count
    from inventory
    group by warehouse_id
),

-- Warehouse metadata (would come from a source table in production;
-- here we derive a minimal dimension from the data we have)
warehouse_meta as (
    select 'WH001' as warehouse_id, 'Nairobi Hub'        as warehouse_name, 'KE' as country_code, 'Nairobi'    as city union all
    select 'WH002',                  'Shenzhen Logistics', 'CN',              'Shenzhen' union all
    select 'WH003',                  'Mumbai Central',     'IN',              'Mumbai'   union all
    select 'WH004',                  'Frankfurt Depot',    'DE',              'Frankfurt' union all
    select 'WH005',                  'São Paulo DC',       'BR',              'São Paulo'
),

final as (
    select
        {{ generate_surrogate_key(['wm.warehouse_id']) }} as warehouse_key,
        wm.warehouse_id,
        wm.warehouse_name,
        wm.country_code,
        wm.city,
        coalesce(wp.product_count, 0) as product_count,
        coalesce(wp.total_units_on_hand, 0) as total_units_on_hand,
        coalesce(wp.total_inventory_value, 0) as total_inventory_value,
        coalesce(wp.hazardous_product_count, 0) as hazardous_product_count
    from warehouse_meta wm
    left join warehouse_products wp on wm.warehouse_id = wp.warehouse_id
)

select * from final
