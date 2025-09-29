
# Databricks notebook source
# MAGIC %md
# MAGIC ## 01_generate_synth_data
# MAGIC Generate synthetic retail datasets for 60 days: POS transactions, products, restaurant orders, loyalty profiles, campaigns, stock.
# MAGIC Save as CSV in the repo `data_seed/` for ingestion.

# COMMAND ----------

import pandas as pd, numpy as np
from datetime import datetime, timedelta
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
out = repo_root / "data_seed"
out.mkdir(exist_ok=True, parents=True)

np.random.seed(42)

days = 60
start_date = datetime.now() - timedelta(days=days)
store_id = 1101

# Product catalog
families = ["SOFAS","BEDS","WARDROBES","TABLES","CHAIRS","LAMPS","TEXTILES","KITCHENWARE","DECOR","STORAGE"]
size_class = {"SOFAS":"LARGE","BEDS":"LARGE","WARDROBES":"LARGE","TABLES":"LARGE",
              "CHAIRS":"SMALL","LAMPS":"SMALL","TEXTILES":"SMALL","KITCHENWARE":"SMALL","DECOR":"SMALL","STORAGE":"SMALL"}

rows = []
pid = 1000
for fam in families:
    for i in range(1,51):
        pid += 1
        price = np.random.randint(49, 9999) if size_class[fam]=="LARGE" else np.random.randint(9, 1499)
        margin = round(price * np.random.uniform(0.2, 0.6), 2)
        rows.append([pid, fam, size_class[fam], fam[:3]+"-"+str(i).zfill(3), price, margin])
products = pd.DataFrame(rows, columns=["product_id","family","size_class","sku","price","margin"])

# Loyalty
loy_rows = []
for lid in range(1, 6001):
    dist = np.random.gamma(2, 5)
    hh = np.random.choice([1,2,3,4,5], p=[0.25,0.35,0.2,0.15,0.05])
    loy_rows.append([lid, round(dist,1), hh])
loyalty = pd.DataFrame(loy_rows, columns=["loyalty_id","home_distance_km","household_size"])

# Transactions per day
tx_rows = []
rest_rows = []
receipt_id = 1
order_id = 1

for d in range(days):
    day = start_date + timedelta(days=d)
    base_visits = np.random.randint(800, 1600)
    # more visits on weekends
    if day.weekday() >= 5:
        base_visits = int(base_visits * 1.3)
    # number of receipts
    receipts = int(base_visits * np.random.uniform(0.4, 0.7))
    for r in range(receipts):
        basket_size = np.random.choice([1,2,3,4,5], p=[0.3,0.3,0.2,0.15,0.05])
        lid = np.random.randint(1, len(loyalty)+1)
        # time of day
        hour = np.random.choice(range(10,20), p=[0.04,0.05,0.06,0.08,0.12,0.15,0.16,0.14,0.1,0.06])
        minute = np.random.randint(0,60)
        ts = datetime(day.year, day.month, day.day, hour, minute, 0)
        # chance of an anchor item
        has_anchor = np.random.rand() < 0.18
        items = []
        if has_anchor:
            fam = np.random.choice(["SOFAS","BEDS","WARDROBES","TABLES"])
            anchor_pid = products[products.family==fam].sample(1).product_id.values[0]
            items.append(anchor_pid)
            # add-on count
            addon_n = np.random.choice([0,1,2,3], p=[0.4,0.35,0.2,0.05])
            small_pool = products[products.size_class=="SMALL"].product_id.values
            for _ in range(addon_n):
                items.append(int(np.random.choice(small_pool)))
        else:
            for _ in range(basket_size):
                fam = np.random.choice(families, p=[0.04,0.05,0.05,0.05,0.15,0.18,0.2,0.12,0.1,0.06])
                pid = products[products.family==fam].sample(1).product_id.values[0]
                items.append(pid)
        # create tx rows
        for pid in items:
            qty = np.random.choice([1,2], p=[0.92,0.08])
            tx_rows.append([receipt_id, store_id, ts.isoformat(), lid, pid, qty])
        # restaurant probability higher for anchor baskets
        rest_prob = 0.28 if has_anchor else 0.14
        if np.random.rand() < rest_prob:
            eat_items = np.random.choice(["MEATBALLS","VEG_BALLS","SALMON","KIDS","COFFEE","CINNAMON_BUN"], 
                                         size=np.random.choice([1,2,3], p=[0.6,0.3,0.1]), replace=True)
            value = np.random.uniform(6, 35)
            rest_rows.append([order_id, store_id, ts.isoformat(), lid, ";".join(eat_items), round(value,2)])
            order_id += 1
        receipt_id += 1

transactions = pd.DataFrame(tx_rows, columns=["receipt_id","store_id","ts","loyalty_id","product_id","qty"])
restaurant = pd.DataFrame(rest_rows, columns=["order_id","store_id","ts","loyalty_id","items","basket_value"])

# Campaigns simple
camp = []
for fam in ["LAMPS","TEXTILES","KITCHENWARE","DECOR"]:
    for k in range(4):
        start = start_date + timedelta(days=np.random.randint(0, days-7))
        end = start + timedelta(days=np.random.randint(3, 10))
        camp.append([fam, start.date().isoformat(), end.date().isoformat(), np.random.choice(["BOGO","%OFF10","%OFF20"])])
campaigns = pd.DataFrame(camp, columns=["family","start","end","type"])

# Write CSVs
products.to_csv(out/"products.csv", index=False)
loyalty.to_csv(out/"loyalty.csv", index=False)
transactions.to_csv(out/"transactions.csv", index=False)
restaurant.to_csv(out/"restaurant.csv", index=False)
campaigns.to_csv(out/"campaigns.csv", index=False)

print("Wrote CSVs to", out)
