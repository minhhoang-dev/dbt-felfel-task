select
    location_name,
    product_name,
    event_at,
    --last_transition_type, -- if this info is relevant
    current_stock,
from {{ ref('fct_inventory_events') }}
where event_at <= '2024-10-08'
    --- and location_name = '' --- if you want to filter by location_name
    --- and product_name = '' -- if you want to filter by product_name
qualify row_number() over (partition by location_name, product_name order by event_at desc) = 1 -- get the current stock given a certain event_at
order by location_name, product_name, event_at desc