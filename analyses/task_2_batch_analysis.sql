with 

inventory_received as (
    select
        to_location_name as location_name,
        sum(amount) as received_transit_sale
    from {{ ref('fct_inventory_transitions') }} 
    group by 1
),

stock_sold as (
    select
        location_name,
        sum(amount) as amount_sold
    from {{ ref('fct_orders') }}
    where product_item_supplier_batch_id = 'F1672AB1-F568-42F7-AF05-2FA9117C1966'
    group by 1
),

inventory_counts as (
    select
        location_name,
        string_agg(comment) as comment,
        sum(amount) as manual_inventory_count,
    from {{ ref('fct_inventory_counts') }}
    where product_item_supplier_batch_id = 'F1672AB1-F568-42F7-AF05-2FA9117C1966'
    group by 1
),

inventory_batch_analysis_per_location as (
    select
        location_name,
        received_transit_sale,
        amount_sold,
        received_transit_sale - amount_sold as expected_stock,
        manual_inventory_count,
        comment
    from inventory_received ir
    left join stock_sold ss using (location_name)
    left join inventory_counts ic using (location_name)
)

select * from inventory_batch_analysis_per_location

