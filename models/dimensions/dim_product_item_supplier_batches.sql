with product_item_supplier_batches as (
    select 
        product_item_supplier_batch_id,
        product_id,
        production_date,
        expiry_date,
    from {{ ref('stg_product_item_supplier_batches') }}
)

select 
    *
from product_item_supplier_batches