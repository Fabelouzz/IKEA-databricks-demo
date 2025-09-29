
with anchors as (
  select distinct b.receipt_id, d.family as anchor_family, to_date(t.ts) as date
  from silver.baskets b
  join bronze.attach_transactions t on b.receipt_id = t.receipt_id
  join silver.dim_products d on t.product_id = d.product_id
  where d.size_class = 'LARGE'
),
den as (
  select anchor_family, date, count(*) as anchor_receipts
  from anchors
  group by anchor_family, date
),
num as (
  select a.anchor_family, a.date, count(distinct b.receipt_id) as attached_receipts
  from anchors a
  join silver.baskets b on a.receipt_id = b.receipt_id
  where b.attached = 1
  group by a.anchor_family, a.date
)
select d.anchor_family, d.date, d.anchor_receipts,
       coalesce(n.attached_receipts,0) as attached_receipts,
       case when d.anchor_receipts>0 then coalesce(n.attached_receipts,0)/d.anchor_receipts else 0 end as attach_rate
from den d
left join num n on d.anchor_family = n.anchor_family and d.date = n.date
