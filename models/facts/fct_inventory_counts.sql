{{ 
    config(
        materialized='incremental', 
        unique_key='inventory_count_id'
    ) 
}}

with inventory_counts as (
    select
        inventory_count_id,
        amount,
        created_at,
        product_item_supplier_batch_id,
        inventory_stage_id
    from {{ ref('stg_inventory_counts') }}
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
        ic.inventory_count_id,
        pb.product_id,
        ic.inventory_stage_id,
        ist.location_id,
        ic.created_at,
        ic.amount,
        ic.product_item_supplier_batch_id,
        p.product_name,
        p.food_category,
        ist.discriminator as inventory_stage,
        l.location_name
    from inventory_counts ic
    left join product_batches pb 
        on ic.product_item_supplier_batch_id = pb.product_item_supplier_batch_id
    left join products p 
        on pb.product_id = p.product_id
    left join inventory_stages ist 
        on ic.inventory_stage_id = ist.inventory_stage_id
    left join locations l 
        on ist.location_id = l.location_id
)

--- Decisions and Reasoning
    ---- Tracks inventory corrections manually recorded by drivers.
    ---- Joins with dim_inventory_stages and dim_locations to get stage and location details.
    ---- Uses created_at for incremental processing for better performance.

select * from final 

{% if is_incremental() %}
    where created_at > (select max(created_at) from {{ this }})  -- Incremental load
{% endif %}