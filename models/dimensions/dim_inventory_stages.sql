with 

inventory_stages as (
    select
        inventory_stage_id,
        delivery_batch_id,
        location_id,
        discriminator,
    from {{ ref('stg_inventory_stages') }}
)

select * from inventory_stages