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
        fis.location_id,
        it.from_inventory_stage_id,
        it.to_inventory_stage_id,
        it.created_at,
        it.amount,
        it.product_item_supplier_batch_id,
        pb.product_id,
        p.product_name,
        p.food_category,
        fis.discriminator as from_stage,
        tis.discriminator as to_stage,
        l1.location_name as from_location_name,
        l2.location_name as to_location_name
    from inventory_transitions it
    left join product_batches pb 
        on it.product_item_supplier_batch_id = pb.product_item_supplier_batch_id
    left join products p 
        on pb.product_id = p.product_id
    left join inventory_stages fis
        on it.from_inventory_stage_id = fis.inventory_stage_id
    left join inventory_stages tis 
        on it.to_inventory_stage_id = tis.inventory_stage_id
    left join locations l1
        on fis.location_id = l1.location_id
    left join locations l2
        on tis.location_id = l2.location_id
)

--- Decision and Reasoning
    --- Tracks inventory movements (Sales, Donation, Waste) using from_stage and to_stage.
    --- Joins with dim_inventory_stages to determine fridge location.
    --- Uses created_at for incremental processing for efficient data loading.

select * from final