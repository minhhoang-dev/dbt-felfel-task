{{
    config(
        materialized='incremental',
        primary_key='inventory_transition_id'
    )
}}

with inventory_transitions as (
    select
        inventory_transition_id,
        amount,
        product_item_supplier_batch_id,
        from_inventory_stage_id,
        to_inventory_stage_id,
        created_at
    from {{ ref('stg_inventory_transitions') }}
    
),

inventory_stages as (
    select
        inventory_stage_id,
        discriminator,
        location_id
    from {{ ref('dim_inventory_stages') }}
),

product_batches as (
    select
        product_item_supplier_batch_id,
        product_id
    from {{ ref('dim_product_item_supplier_batches') }}
),

products as (
    select
        product_id,
        product_name,
        food_category
    from {{ ref('dim_products') }}
),

locations as (
    select
        location_id,
        location_name
    from {{ ref('dim_locations') }}
),

final as (
    select 
        it.inventory_transition_id,
        it.created_at,
        it.amount,
        it.product_item_supplier_batch_id,
        pb.product_id,
        p.product_name,
        p.food_category,
        it.from_inventory_stage_id,
        fis.discriminator as from_stage,
        it.to_inventory_stage_id,
        tis.discriminator as to_stage,
        fis.location_id,
        l.location_name
    from inventory_transitions it
    left join product_batches pb 
        on it.product_item_supplier_batch_id = pb.product_item_supplier_batch_id
    left join products p 
        on pb.product_id = p.product_id
    left join inventory_stages fis 
        on it.from_inventory_stage_id = fis.inventory_stage_id
    left join inventory_stages tis 
        on it.to_inventory_stage_id = tis.inventory_stage_id
    left join locations l 
        on fis.location_id = l.location_id
)


select * from final

{% if is_incremental() %}
    where created_at > (select max(created_at) from {{ this }})  -- Incremental load
{% endif %}