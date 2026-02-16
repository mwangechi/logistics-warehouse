-- Dimension: Suppliers
-- Supplier master data with performance metrics.

with suppliers as (
    select * from {{ ref('stg_suppliers') }}
),

final as (
    select
        {{ generate_surrogate_key(['supplier_id']) }} as supplier_key,
        supplier_id,
        supplier_name,
        contact_email,
        country_code,
        city,
        lead_time_days,
        reliability_score,
        supplier_tier,
        payment_terms,
        is_active
    from suppliers
)

select * from final
