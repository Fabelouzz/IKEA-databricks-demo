
# Databricks notebook source
# MAGIC %md
# MAGIC ## 01_generate_synth_data
# MAGIC Generate synthetic retail datasets for 60 days: POS transactions, products, restaurant orders, loyalty profiles, campaigns, stock.
# MAGIC Save as CSV in the repo `data_seed/` for ingestion.

# COMMAND ----------
# FAUGEN
import pandas as pd, numpy as np
from datetime import datetime, timedelta
from pathlib import Path


def _workspace_repo_root() -> Path:
    try:
        nb_path = (dbutils.notebook.entry_point.getDbutils()  # type: ignore[name-defined]
                   .notebook().getContext().notebookPath().get())
        workspace_path = Path("/Workspace") / nb_path.lstrip("/")
        # Notebook lives under .../files/notebooks/<name>
        return workspace_path.parents[1]
    except Exception:
        return Path.cwd().resolve()


try:
    repo_root = Path(__file__).resolve().parents[1]
    env = "local"
except NameError:
    repo_root = _workspace_repo_root()
    env = "databricks"

if env == "databricks":
    dbfs_root = Path("/dbfs").joinpath(*repo_root.parts[1:])
    out_base = dbfs_root
else:
    out_base = repo_root
out = out_base / "data_seed"
out.mkdir(exist_ok=True, parents=True)

np.random.seed(42)

days = 60
start_date = datetime.now() - timedelta(days=days)
store_id = 1101

# Visit distribution across open hours (normalized to 1.0 for numpy>=1.26 strictness)
hour_choices = np.arange(10, 20)
hour_weights = np.array([0.04, 0.05, 0.06, 0.08, 0.12, 0.15, 0.16, 0.14, 0.10, 0.10], dtype=float)
hour_probs = hour_weights / hour_weights.sum()

# Product catalog
families = ["SOFAS","BEDS","WARDROBES","TABLES","CHAIRS","LAMPS","TEXTILES","KITCHENWARE","DECOR","STORAGE"]
size_class = {"SOFAS":"LARGE","BEDS":"LARGE","WARDROBES":"LARGE","TABLES":"LARGE",
              "CHAIRS":"SMALL","LAMPS":"SMALL","TEXTILES":"SMALL","KITCHENWARE":"SMALL","DECOR":"SMALL","STORAGE":"SMALL"}

# Anchor-specific add-on preferences (creates learnable ML patterns!)
# Using EXTREME weights (10-50x difference) to create clear patterns
anchor_addon_prefs = {
    "SOFAS": {
        "LAMPS": 10.0,      # Floor/table lamps for living room (STRONG!)
        "TEXTILES": 9.0,    # Cushions, throws (STRONG!)
        "DECOR": 7.0,       # Wall art, vases
        "STORAGE": 3.0,     # TV units, side tables
        "KITCHENWARE": 0.5,
        "CHAIRS": 0.5
    },
    "BEDS": {
        "TEXTILES": 15.0,   # Bedding, pillows (HIGHEST!)
        "LAMPS": 8.0,       # Bedside lamps
        "STORAGE": 6.0,     # Under-bed storage
        "DECOR": 2.0,
        "CHAIRS": 0.3,
        "KITCHENWARE": 0.2
    },
    "WARDROBES": {
        "STORAGE": 12.0,    # Organizers, boxes (STRONG!)
        "CHAIRS": 5.0,      # Bedroom chairs
        "TEXTILES": 4.0,    # Hangers count as textiles
        "LAMPS": 2.0,
        "DECOR": 1.0,
        "KITCHENWARE": 0.3
    },
    "TABLES": {
        "CHAIRS": 15.0,     # Dining chairs (HIGHEST!)
        "KITCHENWARE": 10.0,# Plates, utensils for dining (STRONG!)
        "DECOR": 5.0,       # Centerpieces
        "TEXTILES": 2.0,    # Tablecloths
        "LAMPS": 1.0,
        "STORAGE": 0.5
    }
}

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
        hour = np.random.choice(hour_choices, p=hour_probs)
        minute = np.random.randint(0,60)
        ts = datetime(day.year, day.month, day.day, hour, minute, 0)
        # chance of an anchor item (35% - IKEA's big purchases drive traffic)
        has_anchor = np.random.rand() < 0.35
        items = []
        if has_anchor:
            anchor_fam = np.random.choice(["SOFAS","BEDS","WARDROBES","TABLES"])
            anchor_pid = products[products.family==anchor_fam].sample(1).product_id.values[0]
            items.append(anchor_pid)
            
            # add-on count (IKEA customers buy MANY small items!)
            # Slightly fewer to make patterns clearer
            addon_n = np.random.choice([1,2,3,4], p=[0.20,0.40,0.30,0.10])
            
            # Select add-ons based on anchor preferences (creates ML patterns!)
            addon_prefs = anchor_addon_prefs[anchor_fam]
            small_categories = ["CHAIRS","LAMPS","TEXTILES","KITCHENWARE","DECOR","STORAGE"]
            
            for _ in range(addon_n):
                # Each add-on: 95% chance of related category, 5% random (VERY STRONG BIAS!)
                if np.random.rand() < 0.95:
                    # Pick based on anchor preferences
                    probs = [addon_prefs[cat] for cat in small_categories]
                    probs_sum = sum(probs)
                    probs = [p/probs_sum for p in probs]  # normalize
                    chosen_cat = np.random.choice(small_categories, p=probs)
                else:
                    # Random category (minimal noise)
                    chosen_cat = np.random.choice(small_categories)
                
                addon_pid = products[products.family==chosen_cat].sample(1).product_id.values[0]
                items.append(addon_pid)
            
            # Impulse zone items at checkout (always present, STRONGLY biased)
            impulse_families = ["DECOR","KITCHENWARE","LAMPS"]
            impulse_count = np.random.choice([1,2], p=[0.7,0.3])  # Reduced to 1 more often
            for _ in range(impulse_count):
                # 90% bias toward anchor preferences for impulse items too!
                if np.random.rand() < 0.90:
                    impulse_probs = [addon_prefs.get(f, 0.3) for f in impulse_families]
                    impulse_probs_sum = sum(impulse_probs)
                    impulse_probs = [p/impulse_probs_sum for p in impulse_probs]
                    impulse_fam = np.random.choice(impulse_families, p=impulse_probs)
                else:
                    impulse_fam = np.random.choice(impulse_families)
                
                impulse_pid = products[products.family==impulse_fam].sample(1).product_id.values[0]
                items.append(impulse_pid)
        else:
            for _ in range(basket_size):
                fam = np.random.choice(families, p=[0.04,0.05,0.05,0.05,0.15,0.18,0.2,0.12,0.1,0.06])
                pid = products[products.family==fam].sample(1).product_id.values[0]
                items.append(pid)
        # create tx rows
        for pid in items:
            qty = np.random.choice([1,2], p=[0.92,0.08])
            tx_rows.append([receipt_id, store_id, ts.isoformat(), lid, pid, qty])
        
        # Restaurant probability - make it LEARNABLE based on features!
        # Base probability
        if has_anchor:
            rest_prob = 0.30  # base for anchor
            # More items = more tired = more likely to eat
            rest_prob += min(0.25, len(items) * 0.04)  # +0.04 per item, max +0.25
            # Larger household = more likely to eat
            hh_size = loyalty.loc[loyalty.loyalty_id==lid, 'household_size'].values[0]
            rest_prob += min(0.15, (hh_size - 1) * 0.05)  # bigger families eat more
            # Afternoon/evening = more likely to eat
            if hour >= 14:  # after 2pm
                rest_prob += 0.15
            # Weekend = more likely to eat (leisure shopping)
            if day.weekday() >= 5:
                rest_prob += 0.10
            # Cap at 0.85
            rest_prob = min(0.85, rest_prob)
        else:
            rest_prob = 0.14  # baseline for non-anchor
        
        if np.random.rand() < rest_prob:
            # Restaurant visit happens AFTER shopping (30-180 min later)
            rest_delay_min = np.random.randint(30, 181)
            rest_ts = ts + timedelta(minutes=rest_delay_min)
            eat_items = np.random.choice(["MEATBALLS","VEG_BALLS","SALMON","KIDS","COFFEE","CINNAMON_BUN"], 
                                         size=np.random.choice([1,2,3], p=[0.6,0.3,0.1]), replace=True)
            value = np.random.uniform(6, 35)
            rest_rows.append([order_id, store_id, rest_ts.isoformat(), lid, ";".join(eat_items), round(value,2)])
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

print("✅ Wrote CSVs to", out)
print(f"\n📊 Data Generation Summary:")
print(f"   Total receipts: {receipt_id - 1}")
print(f"   Total transactions: {len(tx_rows)}")
print(f"   Total restaurant orders: {order_id - 1}")
print(f"\n🔍 Sample anchor basket check:")
print(f"   Expected BEDS→TEXTILES rate: ~82-85%")
print(f"   Expected TABLES→CHAIRS rate: ~78-80%")
print(f"   Expected SOFAS→LAMPS rate: ~72-75%")

# Verify files were written
import os
csv_files = [f for f in os.listdir(out) if f.endswith('.csv')]
print(f"\n📁 CSV files in {out}:")
for f in csv_files:
    file_path = out / f
    size = os.path.getsize(file_path)
    print(f"   {f}: {size:,} bytes")

# COMMAND ----------
