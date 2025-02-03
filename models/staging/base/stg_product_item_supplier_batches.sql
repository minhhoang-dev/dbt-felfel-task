with

base as (
    select
        ProductItemSupplierBatchId as product_item_supplier_batch_id,
        ProductId as product_id,
        date(ProductionDate) as production_date,
        date(ExpiryDate) as expiry_date,
    from {{ source('raw_data', 'src_product_item_supplier_batches') }}
    where ProductionDate <= ExpiryDate -- check for data integrity
)

select * from base