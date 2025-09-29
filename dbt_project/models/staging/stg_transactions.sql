
select cast(receipt_id as bigint) as receipt_id,
       cast(store_id as int) as store_id,
       cast(ts as timestamp) as ts,
       cast(loyalty_id as int) as loyalty_id,
       cast(product_id as int) as product_id,
       cast(qty as int) as qty
from bronze.attach_transactions
