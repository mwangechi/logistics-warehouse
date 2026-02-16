-- Staging: Suppliers
-- Clean and standardize raw supplier data.

with source as (
    select * from {{ source('raw', 'raw_suppliers') }}
),

renamed as (
    select
        supplier_id,
        trim(supplier_name) as supplier_name,
        lower(trim(contact_email)) as contact_email,
        upper(trim(country)) as country_code,
        trim(city) as city,

        cast(lead_time_days as integer) as lead_time_days,
        cast(reliability_score as decimal(3,2)) as reliability_score,
        trim(payment_terms) as payment_terms,
        cast(is_active as boolean) as is_active,

        -- Tier classification based on reliability
        case
            when reliability_score >= 0.95 then 'platinum'
            when reliability_score >= 0.90 then 'gold'
            when reliability_score >= 0.85 then 'silver'
            else 'bronze'
        end as supplier_tier

    from source
)

select * from renamed
