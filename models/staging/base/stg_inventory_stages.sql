with

base as (
    select
        InventoryStageId as inventory_stage_id,
        LocationId as location_id,
        DeliveryBatchId as delivery_batch_id,
        Discriminator as discriminator,
    from {{ source('raw_data', 'src_inventory_stages') }}
)

select * from base