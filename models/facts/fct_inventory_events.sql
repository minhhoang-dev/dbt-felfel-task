with

stock_added as (
    select
        to_location_name as location_name,
        created_at as event_at,
        product_item_supplier_batch_id,
        product_id,
        product_name,
        'stock added' as transition_type,
        sum(amount) as stock_change
    from {{ ref('fct_inventory_transitions') }}
    where to_stage = 'sale'
    group by 1, 2, 3, 4, 5
),

stock_donated_wasted as (
    select
        from_location_name as location_name,
        created_at as event_at,
        product_item_supplier_batch_id,
        product_id,
        product_name,
        to_stage as transition_type,
        sum(-amount) as stock_change
    from {{ ref('fct_inventory_transitions') }}
    where from_stage = 'sale'
    group by 1, 2, 3, 4, 5, 6
),

stock_sold as (
    select
        location_name,
        created_at as event_at,
        product_item_supplier_batch_id,
        product_id,
        product_name,
        'sold' as transition_type,
        sum(-amount) as stock_change 
    from {{ ref('fct_orders') }}
    group by 1, 2, 3, 4, 5, 6
),

manually_adjusted as (
    select
        location_name,
        created_at as event_at,
        product_item_supplier_batch_id,
        product_id,
        product_name,
        'manual adjustment' as transition_type,
        amount,
    from {{ ref('fct_inventory_counts') }}
),

unioned as (
    select * from stock_added
    union all
    select * from stock_donated_wasted
    union all
    select * from stock_sold
    union all 
    select * from manually_adjusted
),

stock_with_reset_flag AS (
    -- Identify manual adjustment events and mark them for resetting
    SELECT 
        location_name,
        product_item_supplier_batch_id,
        product_id,
        product_name,
        event_at,
        transition_type,
        stock_change,
        CASE 
            WHEN transition_type = 'manual adjustment' THEN stock_change
            ELSE NULL
        END AS manual_stock
    FROM unioned
),

grouped_events AS (
    -- Assign a reset group number that increases every time a manual adjustment occurs
    SELECT 
        *,
        COUNT(manual_stock) OVER (
            PARTITION BY location_name, product_item_supplier_batch_id, product_id
            ORDER BY event_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS reset_group
    FROM stock_with_reset_flag
),

running_stock AS (
    -- Compute cumulative stock but restart when a manual adjustment occurs
    SELECT 
        location_name,
        product_item_supplier_batch_id,
        product_id,
        product_name,
        event_at,
        transition_type,
        stock_change,
        SUM(
            CASE 
                WHEN transition_type = 'manual adjustment' THEN 0 
                ELSE stock_change 
            END
        ) OVER (
            PARTITION BY location_name, product_item_supplier_batch_id, product_id, reset_group
            ORDER BY event_at 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) 
        + coalesce(MAX(manual_stock) OVER (
            PARTITION BY location_name, product_item_supplier_batch_id, product_id, reset_group
        ), 0) AS current_stock
    FROM grouped_events
),


aggregated as (
    select
        location_name,
        product_item_supplier_batch_id,
        product_id,
        product_name,
        event_at,
        transition_type as last_transition_type,
        stock_change,
        current_stock
    from running_stock
    where product_name is not null
    order by location_name, product_name, event_at asc
)

select * from aggregated