--- Final Stock = Stock Added - Stock Removed [i.e. sold, wasted, donated] vs Manual Adjustments

with

stock_added as (
    select
        product_item_supplier_batch_id,
        product_id,
        product_name,
        sum(amount) as total_stock_added
    from {{ ref('fct_inventory_transitions') }}
    where to_location_name = 'FELFEL Office Lausanne'
        and created_at <= '2024-12-01 20:00:00.00'
    group by 1, 2, 3
),

stock_sold as (
    select
        product_item_supplier_batch_id,
        product_id,
        sum(amount) as total_stock_sold
    from {{ ref('fct_orders') }}
    where location_name = 'FELFEL Office Lausanne'
        and created_at <= '2024-12-01 20:00:00.00'
    group by 1, 2
),

stock_wasted_donated as (
    select
        product_item_supplier_batch_id,
        product_id,
        sum(amount) as total_stock_wasted_donated
    from {{ ref('fct_inventory_transitions') }}
    where from_location_name = 'FELFEL Office Lausanne'
        and created_at <= '2024-12-01 20:00:00.00'
    group by 1, 2
),

stock_manually_adjusted as (
    select
        product_item_supplier_batch_id,
        product_id,
        sum(amount) as manually_counted
    from {{ ref('fct_inventory_counts') }}
    where location_name = 'FELFEL Office Lausanne'
        and created_at <= '2024-12-01 20:00:00.00'
    group by 1, 2
),

joined as (
    select
        product_item_supplier_batch_id,
        product_id,
        product_name,
        total_stock_added,
        coalesce(total_stock_sold, 0) as total_stock_sold,
        coalesce(total_stock_wasted_donated, 0) as total_stock_wasted_donated,
        manually_counted
    from stock_added sa
    left join stock_sold ss using (product_item_supplier_batch_id, product_id)
    left join stock_wasted_donated swd using (product_item_supplier_batch_id, product_id)
    left join stock_manually_adjusted sma using (product_item_supplier_batch_id, product_id)
    where product_name is not null
),

calculated_stock_added as (
    select
        *,
        total_stock_added - total_stock_sold - total_stock_wasted_donated as calculated_stock,
        case
            when manually_counted is not null then manually_counted
            else total_stock_added - total_stock_sold - total_stock_wasted_donated
        end as final_stock,
    from joined
),

final as (
    select
        product_name,
        manually_counted,
        sum(total_stock_added) as total_stock_added,
        sum(total_stock_sold) as total_stock_sold,
        sum(total_stock_wasted_donated) as total_stock_wasted_donated,
        sum(calculated_stock) as calculated_stock,
        sum(final_stock) as final_stock
    from calculated_stock_added
    group by 1, 2
)

select 
    *,
    calculated_stock != final_stock as has_been_manually_adjusted
from final
order by final_stock desc
