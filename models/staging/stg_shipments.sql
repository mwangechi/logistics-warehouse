-- Staging: Shipments
-- Clean and standardize raw shipment records.

with source as (
    select * from {{ source('raw', 'raw_shipments') }}
),

renamed as (
    select
        shipment_id,
        supplier_id,
        product_id,
        warehouse_id,

        cast(order_date as date)    as order_date,
        cast(ship_date as date)     as ship_date,
        cast(delivery_date as date) as delivery_date,

        cast(quantity as integer)       as quantity,
        cast(unit_price as decimal(10,2))   as unit_price,
        cast(shipping_cost as decimal(10,2)) as shipping_cost,

        -- Derived
        quantity * unit_price as line_total,

        lower(trim(status)) as shipment_status,
        upper(trim(origin_country)) as origin_country,
        upper(trim(destination_country)) as destination_country,

        -- Delivery lead time in days (NULL if not yet delivered)
        case
            when delivery_date is not null and ship_date is not null
                then delivery_date - ship_date
        end as delivery_days

    from source
)

select * from renamed
