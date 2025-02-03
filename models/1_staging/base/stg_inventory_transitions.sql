
{{
    config(
        materialized='incremental',
        primary_key='inventory_transition_id',
        on_schema_change='append_new_columns'
    )
}}

with

base as (
    select
        InventoryTransitionId as inventory_transition_id,
        FromInventoryStageId as from_inventory_stage_id,
        ToInventoryStageId as to_inventory_stage_id,
        ProductItemSupplierBatchId as product_item_supplier_batch_id,
        Amount as amount,
        Comment as comment,
        CreatedAt as created_at
    from {{ source('raw_data', 'src_inventory_transitions') }}
    where Amount > 0 -- remove negative or zero transactions
)


select * from base

{%- if is_incremental() %}
    where created_at > (select max(created_at) from {{ this }})
{%- endif %}