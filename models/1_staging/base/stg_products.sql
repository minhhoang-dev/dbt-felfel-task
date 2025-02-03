with

base as (
    select
        ProductId as product_id,
        ProductName as product_name,
        FoodCategory as food_category,
        ShelfLifeDays as shelf_life_days
    from {{ source('raw_data', 'src_products') }}
)

select * from base