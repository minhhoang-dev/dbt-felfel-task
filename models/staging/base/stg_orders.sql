{{
    config(
        materialized='incremental',
        primary_key='primary_key',
        on_schema_change='append_new_columns'
    )
}}



with

base as (
    select
        concat(OrderId, coalesce(ProductItemSupplierBatchId, ''), coalesce(LocationId, '')) as primary_key,
        OrderId as order_id,
        ProductItemSupplierBatchId as product_item_supplier_batch_id,
        LocationId as location_id,
        CreatedAt as created_at,
        Amount as amount
    from {{ source('raw_data', 'src_orders') }}
    where createdAt is not null -- only valid timestamps
)


select   
    *
from base

{%- if is_incremental() %}
    where created_at > (select max(created_at) from {{ this }})
{%- endif %}
