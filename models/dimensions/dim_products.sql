with

products as (
    select 
        product_id,
        product_name,
        shelf_life_days,
        food_category
    from {{ ref('stg_products') }}
)

select * from products