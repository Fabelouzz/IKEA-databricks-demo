
select cast(product_id as int) as product_id,
       family, size_class, sku,
       cast(price as double) as price,
       cast(margin as double) as margin
from bronze.attach_products
