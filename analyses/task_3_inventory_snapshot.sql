with

base as (
    select
        row_number() over (partition by location_name, product_name, product_item_supplier_batch_id order by event_at desc) as rn, -- get the current stock given a certain event_at
        location_name,
        product_item_supplier_batch_id,
        product_name,
        event_at,
        --last_transition_type, -- if this info is relevant
        current_stock,
    from {{ ref('fct_inventory_events') }}
    where event_at <= '2024-10-08' ---- set the date you want to have here
        --- and location_name = '' --- if you want to filter by location_name
        --- and product_name = '' -- if you want to filter by product_name
    --qualify row_number() over (partition by location_name, product_name, product_item_supplier_batch_id order by event_at desc) = 1 -- get the current stock given a certain event_at
    order by location_name, product_name, event_at desc
),

aggregated as (
    select
        *
    from base
    where rn = 1
),

final as (
    select
        location_name,
        product_name,
        sum(current_stock) as current_stock
    from aggregated
    group by 1, 2
    order by location_name, product_name
)

select * from final 