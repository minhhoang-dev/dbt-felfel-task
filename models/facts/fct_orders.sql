{{
    config(
        materialized='incremental',
        primary_key='primary_key',
        on_schema_change='append_new_columns'
    )
}}

with orders as (
    select
        primary_key,
        order_id,
        created_at,
        product_item_supplier_batch_id,
        location_id,
        amount
    from {{ ref('stg_orders') }}
),

product_batches as (
    select
        product_item_supplier_batch_id,
        production_date,
        expiry_date,
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
        o.primary_key,
        o.order_id,
        o.product_item_supplier_batch_id,
        o.location_id,
        pb.product_id,
        o.created_at,
        l.location_name,
        p.product_name,
        p.food_category,
        pb.production_date,
        pb.expiry_date,
        o.amount
    from orders o
    left join product_batches pb 
        on o.product_item_supplier_batch_id = pb.product_item_supplier_batch_id
    left join products p 
        on pb.product_id = p.product_id
    left join locations l 
        on o.location_id = l.location_id
)

--- Decision and Reasoning:
-- Joins stg_orders with dimensions for denormalization.
-- Uses created_at for incremental processing to optimize performance.
-- Stores historical production_date and expiry_date for better analysis.

select * from final

{% if is_incremental() %}
    where created_at > (select max(created_at) from {{ this }})  -- Incremental load
{% endif %}