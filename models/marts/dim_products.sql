-- Dimension: Products
-- Product catalog with categories and physical attributes.

with inventory as (
    select * from {{ ref('stg_inventory') }}
),

final as (
    select
        {{ generate_surrogate_key(['product_id']) }} as product_key,
        product_id,
        product_name,
        category,
        subcategory,
        unit_cost,
        weight_kg,
        is_hazardous,
        stock_status
    from inventory
)

select * from final
