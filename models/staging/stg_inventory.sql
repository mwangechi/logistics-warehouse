-- Staging: Inventory
-- Clean and standardize raw inventory records.

with source as (
    select * from {{ source('raw', 'raw_inventory') }}
),

renamed as (
    select
        product_id,
        trim(product_name)  as product_name,
        trim(category)      as category,
        trim(subcategory)   as subcategory,
        warehouse_id,

        cast(quantity_on_hand as integer) as quantity_on_hand,
        cast(reorder_point as integer)    as reorder_point,
        cast(unit_cost as decimal(10,2))  as unit_cost,
        cast(weight_kg as decimal(8,3))   as weight_kg,

        cast(is_hazardous as boolean) as is_hazardous,

        -- Stock status
        case
            when quantity_on_hand <= 0 then 'out_of_stock'
            when quantity_on_hand <= reorder_point then 'low_stock'
            else 'in_stock'
        end as stock_status

    from source
)

select * from renamed
