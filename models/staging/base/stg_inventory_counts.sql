{{
    config(
        materialized='incremental',
        primary_key='inventory_count_id',
        on_schema_change='append_new_columns'
    )
}}

with

base as (
    select
        InventoryCountId as inventory_count_id, -- primary_key
        InventoryStageId as inventory_stage_id, -- foreign key
        ProductItemSupplierBatchId as product_item_supplier_batch_id, -- foreign key
        Amount as amount,
        Comment as comment,
        CreatedAt as created_at,
    from {{ source('raw_data', 'src_inventory_counts') }}
    where Amount >= 0  -- filter out negative inventory counts
)

select * from base

{%- if is_incremental() %}
    where created_at > (select max(created_at) from {{ this }})
{%- endif %}