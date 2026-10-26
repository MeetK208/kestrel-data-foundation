#!/usr/bin/env python3
"""
Kestrel Provisions: raw feed generator (AIDE assignment).

Generates the raw landing zone. The dataset shipped with the assignment is
scale=1. The same generator produces a larger dataset with --scale.

    python3 generate_dataset.py --scale 1   --out data
    python3 generate_dataset.py --scale 10  --out data_10x

Row counts are approximately linear in scale. Generation is sliced so peak
memory stays bounded regardless of scale; each slice writes its own part file
into the relevant partition, which is why partitions contain several files.

Defects in the generated data are intentional and marked  # DEFECT:
"""
import argparse, os, shutil, gc, random, datetime as dt
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ap = argparse.ArgumentParser()
ap.add_argument("--scale", type=float, default=1.0)
ap.add_argument("--out", default="data")
ap.add_argument("--seed", type=int, default=20260811)
ap.add_argument("--slice-rows", type=int, default=500_000)
args = ap.parse_args()

S, OUT, SLICE = args.scale, args.out, args.slice_rows
rng = np.random.default_rng(args.seed)
random.seed(args.seed)

START, END = dt.date(2025, 1, 1), dt.date(2026, 6, 30)
NDAYS = (END - START).days + 1
DAYS = np.array([START + dt.timedelta(days=i) for i in range(NDAYS)])
DAYSTR = np.array([d.isoformat() for d in DAYS])
DRIFT_DATE = "2025-10-01"          # schema drift boundary on the POS feed

if os.path.isdir(OUT):
    shutil.rmtree(OUT)
os.makedirs(OUT, exist_ok=True)

N_POS = int(4_000_000 * S)
N_TEL = int(3_600_000 * S)
N_WMS = int(1_600_000 * S)
N_ORD = int(320_000 * S)
print(f"scale={S}  pos={N_POS:,}  telemetry={N_TEL:,}  wms={N_WMS:,}  orders={N_ORD:,}", flush=True)

N_OUTLET = int(2_400 * max(1, S ** 0.5))
N_SKU = int(1_100 * max(1, S ** 0.35))
N_WH, N_ROUTE, N_DEVICE = 8, 260, 340


def idpool(prefix, width, n):
    return (prefix + pd.Series(np.arange(1, n + 1)).astype(str).str.zfill(width)).to_numpy()


outlet_codes = idpool("OUT", 6, N_OUTLET)
sku_codes = idpool("SKU", 6, N_SKU)
wh_codes = idpool("WH", 2, N_WH)
route_codes = idpool("RT", 4, N_ROUTE)
device_ids = idpool("DEV-", 5, N_DEVICE)
order_keys = idpool("SO-", 8, N_ORD)

CATS = np.array(["Dairy", "Bakery", "Beverages", "Snacks", "Frozen", "Staples", "Sauces", "Ready to Eat"])
CHANNELS = np.array(["GT", "MT", "HORECA", "ECOM"])
sku_cat = rng.choice(CATS, N_SKU)
sku_case_pack = rng.choice([6, 8, 10, 12, 24, 30], N_SKU)
sku_mrp = np.round(rng.uniform(20, 480, N_SKU), 0)
outlet_channel = rng.choice(CHANNELS, N_OUTLET, p=[.50, .26, .17, .07])
outlet_wh = rng.integers(0, N_WH, N_OUTLET)

_cashier = idpool("C", 5, 4000)
_promo = idpool("PRM", 4, 79)
_loy = idpool("LOY", 8, 400_000)
_veh = idpool("MH12AB", 4, 3200)
_gw = idpool("GW-", 3, 39)
_batch = idpool("B", 6, 200_000)
_pallet = idpool("PL", 7, 400_000)
_oper = idpool("OP", 5, 2200)
_hh = idpool("HH-", 4, 699)


def write_partitioned(df, root, part_col, part_name):
    os.makedirs(root, exist_ok=True)
    for val, g in df.groupby(part_col, sort=True, observed=True):
        d = os.path.join(root, f"{part_col}={val}")
        os.makedirs(d, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(g.drop(columns=[part_col]), preserve_index=False),
                       os.path.join(d, f"{part_name}.parquet"), compression="zstd")


def slices(total):
    k, done = 0, 0
    while done < total:
        n = min(SLICE, total - done)
        yield k, n
        done += n
        k += 1


# =============================================================== 1. POS
print("pos_transactions ...", flush=True)
POS_ROOT = os.path.join(OUT, "raw", "pos_transactions")
for k, n in slices(N_POS):
    day_ix = rng.integers(0, NDAYS, n)
    # DEFECT L1: late arrival. ~4.5% of rows land 1-3 days after the event day.
    late = rng.random(n) < 0.045
    ingest_ix = np.minimum(day_ix + np.where(late, rng.integers(1, 4, n), 0), NDAYS - 1)
    ts_local = (pd.to_datetime(pd.Series(DAYS[day_ix]).astype("datetime64[ns]"))
                + pd.to_timedelta(rng.integers(6 * 3600, 23 * 3600, n), unit="s"))
    # DEFECT L2: event_ts is emitted in UTC. The business day is Asia/Kolkata.
    ts_utc = ts_local - pd.Timedelta(hours=5, minutes=30)

    o_ix = rng.integers(0, N_OUTLET, n)
    s_ix = rng.integers(0, N_SKU, n)
    qty = rng.integers(1, 13, n).astype("int32")
    up = np.round(sku_mrp[s_ix] * rng.uniform(0.70, 1.0, n), 2)
    ing = DAYSTR[ingest_ix]
    nb = max(2, int(n * 0.55))

    df = pd.DataFrame({
        "txn_id": idpool(f"T{k}-", 9, n),
        "txn_line_no": rng.integers(1, 9, n).astype("int16"),
        "basket_id": idpool(f"B{k}-", 9, nb)[rng.integers(0, nb, n)],
        "outlet_code": outlet_codes[o_ix],
        "channel": outlet_channel[o_ix],
        "sku_code": sku_codes[s_ix],
        "event_ts": ts_utc.dt.strftime("%Y-%m-%dT%H:%M:%SZ").values,
        "qty": qty,
        "uom": np.where(rng.random(n) < 0.19, "CS", "EA"),
        "unit_price": up,
        "discount_amount": np.round(up * qty * rng.choice([0, 0, 0, .05, .10], n), 2),
        "tax_amount": np.round(up * qty * 0.12, 2),
        "payment_mode": rng.choice(["CASH", "UPI", "CARD", "CREDIT"], n, p=[.28, .42, .18, .12]),
        "till_id": rng.integers(1, 9, n).astype("int16"),
        "cashier_id": _cashier[rng.integers(0, len(_cashier), n)],
        "promo_code": np.where(rng.random(n) < 0.21, _promo[rng.integers(0, len(_promo), n)], None),
        "loyalty_id": np.where(rng.random(n) < 0.34, _loy[rng.integers(0, len(_loy), n)], None),
        "source_file": np.char.add(np.char.add("pos_", np.char.replace(ing.astype(str), "-", "")), ".jsonl"),
        "ingest_date": ing,
    })
    # DEFECT L3: exact duplicate rows. The collector is at-least-once.
    df = pd.concat([df, df.sample(int(0.021 * n), random_state=3 + k)], ignore_index=True)

    # DEFECT L4: schema drift at DRIFT_DATE. qty becomes quantity_units and two
    # columns appear. Partitions either side of the boundary do not share a schema.
    pre = df[df["ingest_date"] < DRIFT_DATE].drop(columns=["uom", "loyalty_id"])
    post = df[df["ingest_date"] >= DRIFT_DATE].rename(columns={"qty": "quantity_units"})
    if len(pre):
        write_partitioned(pre, POS_ROOT, "ingest_date", f"part-{k:05d}")
    if len(post):
        write_partitioned(post, POS_ROOT, "ingest_date", f"part-{k:05d}")
    del df, pre, post
    gc.collect()
    print(f"  slice {k} ({n:,})", flush=True)

# =============================================================== 2. TELEMETRY
print("reefer_telemetry ...", flush=True)
TEL_ROOT = os.path.join(OUT, "raw", "reefer_telemetry")
for k, n in slices(N_TEL):
    d_ix = rng.integers(0, N_DEVICE, n)
    t_ix = rng.integers(0, NDAYS, n)
    ts = (pd.to_datetime(pd.Series(DAYS[t_ix]).astype("datetime64[ns]"))
          + pd.to_timedelta(rng.integers(0, 86400, n), unit="s"))
    # DEFECT L5: the firmware 2.1.4 fleet has a +7h device clock offset.
    skew = (d_ix % 11 == 0)
    ts = ts + pd.to_timedelta(np.where(skew, 7 * 3600, 0), unit="s")
    # DEFECT L6: one vendor reports Fahrenheit.
    vendor = np.where(d_ix % 3 == 0, "COLDEYE", "THERMLOG")
    temp_c = rng.normal(4.2, 2.6, n)
    val = np.where(vendor == "COLDEYE", temp_c * 9 / 5 + 32, temp_c)
    unit = np.where(vendor == "COLDEYE", "F", "C")
    # DEFECT L7: temp_unit is null on ~8% of rows, so unit must be inferred from vendor.
    unit = np.where(rng.random(n) < 0.08, None, unit)
    # DEFECT L8: sensor dropouts produce null readings.
    val = np.where(rng.random(n) < 0.006, np.nan, val)

    df = pd.DataFrame({
        "device_id": device_ids[d_ix],
        "telemetry_vendor": vendor,
        "firmware_version": np.where(skew, "2.1.4", rng.choice(["3.0.2", "3.1.0", "2.9.9"], n)),
        "vehicle_registration": _veh[d_ix % len(_veh)],
        "route_code": route_codes[rng.integers(0, N_ROUTE, n)],
        "warehouse_code": wh_codes[rng.integers(0, N_WH, n)],
        "gateway_id": _gw[rng.integers(0, len(_gw), n)],
        "reading_ts": ts.dt.strftime("%Y-%m-%dT%H:%M:%SZ").values,
        "temp_value": np.round(val, 2),
        "temp_unit": unit,
        "humidity_pct": np.round(rng.uniform(30, 95, n), 1),
        "door_open_flag": (rng.random(n) < 0.11).astype("int8"),
        "gps_lat": np.round(rng.uniform(8.4, 32.5, n), 5),
        "gps_lon": np.round(rng.uniform(68.7, 92.5, n), 5),
        "battery_pct": np.round(rng.uniform(12, 100, n), 1),
        "dt": DAYSTR[t_ix],
    })
    # DEFECT L9: at-least-once delivery. ~3.2% exact duplicate readings.
    df = pd.concat([df, df.sample(int(0.032 * n), random_state=5 + k)], ignore_index=True)
    # DEFECT L10: gateway GW-017 outage across two days. A hole, not a null.
    df = df[~((df["gateway_id"] == "GW-017") & (df["dt"].isin(["2026-02-11", "2026-02-12"])))]
    write_partitioned(df, TEL_ROOT, "dt", f"part-{k:05d}")
    del df
    gc.collect()
    print(f"  slice {k} ({n:,})", flush=True)

# =============================================================== 3. WMS
print("wms_scan_events ...", flush=True)
WMS_ROOT = os.path.join(OUT, "raw", "wms_scan_events")
STAGES = np.array(["RECEIVE", "PUTAWAY", "PICK", "PACK", "STAGE", "DISPATCH"])
for k, n in slices(N_WMS):
    w_ix = rng.integers(0, NDAYS, n)
    st = rng.integers(0, len(STAGES), n)
    ts = (pd.to_datetime(pd.Series(DAYS[w_ix]).astype("datetime64[ns]"))
          + pd.to_timedelta(rng.integers(0, 72000, n), unit="s")
          + pd.to_timedelta(st * 900, unit="s"))
    df = pd.DataFrame({
        "scan_id": idpool(f"SC{k}-", 10, n),
        "warehouse_code": wh_codes[rng.integers(0, N_WH, n)],
        "event_type": STAGES[st],
        "order_number": order_keys[rng.integers(0, N_ORD, n)],
        "sku_code": sku_codes[rng.integers(0, N_SKU, n)],
        "batch_id": _batch[rng.integers(0, len(_batch), n)],
        "qty_cases": rng.integers(1, 40, n).astype("int16"),
        "pallet_id": _pallet[rng.integers(0, len(_pallet), n)],
        "dock_door": rng.integers(1, 19, n).astype("int8"),
        "operator_id": _oper[rng.integers(0, len(_oper), n)],
        "handheld_device": _hh[rng.integers(0, len(_hh), n)],
        "event_ts": ts.dt.strftime("%Y-%m-%d %H:%M:%S").values,
        "dt": DAYSTR[w_ix],
    })
    # DEFECT L11: ~6.5% of scan events were never emitted. Cycle-time stitching has holes.
    df = df.sample(frac=0.935, random_state=9 + k)
    write_partitioned(df, WMS_ROOT, "dt", f"part-{k:05d}")
    del df
    gc.collect()
    print(f"  slice {k} ({n:,})", flush=True)

# =============================================================== 4. ERP CDC
print("erp_cdc ...", flush=True)
CITIES = np.array(["Mumbai", "Pune", "Bengaluru", "Chennai", "Delhi", "Kolkata",
                   "Hyderabad", "Jaipur", "Indore", "Kochi"])
FORMATS = np.array(["KIRANA", "SUPERMARKET", "QSR", "DARKSTORE", "HYPERMARKET"])
BRANDS = np.array(["Kestrel", "Marwar", "Bluepeak", "Coastline", "Hillfare"])


def cdc_frame(keys, cols_fn, n_changes, pk, delete_frac):
    n_keys = len(keys)
    rows = [{pk: k, "__op": "I",
             "__op_ts": (START - dt.timedelta(days=1)).isoformat() + "T00:00:00Z",
             "__seq": i + 1, **cols_fn(i, 0)} for i, k in enumerate(keys)]
    seq = n_keys
    ki = rng.integers(0, n_keys, n_changes)
    kd = rng.integers(0, NDAYS, n_changes)
    for j in range(n_changes):
        seq += 1
        i = int(ki[j])
        t = dt.datetime.combine(DAYS[kd[j]], dt.time(int(rng.integers(0, 24)), int(rng.integers(0, 60))))
        rows.append({pk: keys[i], "__op": "U", "__op_ts": t.isoformat() + "Z",
                     "__seq": seq, **cols_fn(i, j + 1)})
    for i in rng.choice(n_keys, int(n_keys * delete_frac), replace=False):
        seq += 1
        t = dt.datetime.combine(DAYS[int(rng.integers(0, NDAYS))], dt.time(12, 0))
        rows.append({pk: keys[int(i)], "__op": "D", "__op_ts": t.isoformat() + "Z",
                     "__seq": seq, **cols_fn(int(i), 0)})
    df = pd.DataFrame(rows)
    df["extract_date"] = df["__op_ts"].str[:10].clip(lower=START.isoformat())
    return df


def outlet_cols(i, v):
    return {"outlet_name": f"Outlet {i+1}",
            "channel": str(outlet_channel[i]) if v == 0 else str(rng.choice(CHANNELS)),
            "outlet_format": str(rng.choice(FORMATS)),
            "city": str(rng.choice(CITIES)),
            "route_code": str(route_codes[rng.integers(0, N_ROUTE)]),
            "warehouse_code": str(wh_codes[outlet_wh[i]]),
            "credit_limit": float(np.round(rng.uniform(15000, 900000), -3)),
            "credit_terms_days": int(rng.choice([0, 7, 14, 21, 30, 45])),
            "gst_number": f"27ABCDE{1000 + i % 9000}F1Z5",
            "status": "ACTIVE"}


def product_cols(i, v):
    return {"product_name": f"Product {i+1}", "category": str(sku_cat[i]),
            "brand": str(rng.choice(BRANDS)), "case_pack": int(sku_case_pack[i]),
            "mrp": float(np.round(sku_mrp[i] * (1.0 if v == 0 else rng.uniform(0.9, 1.12)), 2)),
            "list_price": float(np.round(sku_mrp[i] * 0.76, 2)),
            "gst_rate_pct": float(rng.choice([0, 5, 12, 18])),
            "shelf_life_days": int(rng.integers(3, 365)),
            "is_chilled": int(sku_cat[i] in ("Dairy", "Frozen")), "status": "ACTIVE"}


cdc_outlet = cdc_frame(outlet_codes, outlet_cols, int(N_OUTLET * 2.6), "outlet_code", 0.02)
cdc_product = cdc_frame(sku_codes, product_cols, int(N_SKU * 2.2), "sku_code", 0.015)

# DEFECT L12: ~1.6% of CDC records land in a later extract than their __op_ts.
# Latest-wins on file arrival order is wrong; order by (__op_ts, __seq).
for df in (cdc_outlet, cdc_product):
    ooo = rng.random(len(df)) < 0.016
    if ooo.any():
        df.loc[ooo, "extract_date"] = DAYSTR[np.minimum(
            rng.integers(0, NDAYS, int(ooo.sum())) + 3, NDAYS - 1)]

# DEFECT L13: some keys carry two records with an identical __op_ts. __seq breaks the tie.
tie = cdc_outlet.sample(int(len(cdc_outlet) * 0.01), random_state=11).copy()
tie["credit_limit"] = tie["credit_limit"] * 1.5
tie["__seq"] = tie["__seq"] + 500_000
cdc_outlet = pd.concat([cdc_outlet, tie], ignore_index=True)

write_partitioned(cdc_outlet, os.path.join(OUT, "raw", "erp_cdc", "outlet_master"), "extract_date", "part-00000")
write_partitioned(cdc_product, os.path.join(OUT, "raw", "erp_cdc", "product_master"), "extract_date", "part-00000")

print("erp_cdc/sales_order_header ...", flush=True)
STATUS = np.array(["CREATED", "CONFIRMED", "PICKED", "DISPATCHED", "DELIVERED"])
SOH_ROOT = os.path.join(OUT, "raw", "erp_cdc", "sales_order_header")
soh_rows = 0
for k, n in slices(N_ORD):
    base = k * SLICE
    keys = order_keys[base:base + n]
    d_ix = rng.integers(0, NDAYS, n)
    nu = rng.integers(1, 4, n)
    rep = np.repeat(np.arange(n), nu)
    key_rep = np.concatenate([keys, keys[rep]])
    day_rep = np.concatenate([d_ix, d_ix[rep]])
    step = np.concatenate([np.zeros(n, int), np.concatenate([np.arange(1, x + 1) for x in nu])])
    m = len(key_rep)
    op = np.concatenate([np.full(n, "I"), np.full(m - n, "U")])
    o2 = rng.integers(0, N_OUTLET, m)
    ts = (pd.to_datetime(pd.Series(DAYS[day_rep]).astype("datetime64[ns]"))
          + pd.to_timedelta(step * 6 * 3600 + rng.integers(0, 3600, m), unit="s"))
    df = pd.DataFrame({
        "order_number": key_rep, "__op": op,
        "__op_ts": ts.dt.strftime("%Y-%m-%dT%H:%M:%SZ").values,
        "__seq": np.arange(base * 10 + 1, base * 10 + m + 1),
        "outlet_code": outlet_codes[o2], "warehouse_code": wh_codes[outlet_wh[o2]],
        "route_code": route_codes[rng.integers(0, N_ROUTE, m)],
        "order_date": DAYSTR[day_rep],
        "requested_delivery_date": DAYSTR[np.minimum(day_rep + rng.integers(1, 4, m), NDAYS - 1)],
        "order_status": STATUS[np.minimum(step, 4)],
        "line_count": rng.integers(2, 14, m).astype("int16"),
        "order_value_gross": np.round(rng.uniform(2000, 480000, m), 2),
        "discount_amount": np.round(rng.uniform(0, 9000, m), 2),
        "tax_amount": np.round(rng.uniform(200, 52000, m), 2),
        "source_system": rng.choice(["SFA_MOBILE", "ERP_WEB", "PARTNER_API"], m, p=[.6, .3, .1]),
        "extract_date": DAYSTR[day_rep],
    })
    # DEFECT L14: the PARTNER_API source double counts freight into order_value_gross.
    df.loc[df["source_system"] == "PARTNER_API", "order_value_gross"] *= 1.085
    # DEFECT L15: ~0.9% of orders are hard deleted later and must be tombstoned.
    dels = df[df["__op"] == "I"].sample(int(n * 0.009), random_state=13 + k).copy()
    dels["__op"] = "D"
    dels["__seq"] = dels["__seq"] + 10_000_000
    dels["extract_date"] = DAYSTR[np.minimum(
        np.searchsorted(DAYSTR, dels["order_date"].values) + rng.integers(2, 30, len(dels)), NDAYS - 1)]
    df = pd.concat([df, dels], ignore_index=True)
    soh_rows += len(df)
    write_partitioned(df, SOH_ROOT, "extract_date", f"part-{k:05d}")
    del df, dels
    gc.collect()
    print(f"  slice {k} ({n:,} orders)", flush=True)

# =============================================================== 5. REFERENCE
print("reference ...", flush=True)
ref = os.path.join(OUT, "reference")
os.makedirs(ref, exist_ok=True)

# DEFECT L16: the conversion table is missing ~4.2% of SKUs.
pd.DataFrame({"sku_code": sku_codes, "eaches_per_case": sku_case_pack,
              "base_uom": "EA", "case_uom": "CS"}
             ).sample(frac=0.958, random_state=17).sort_values("sku_code"
             ).to_csv(os.path.join(ref, "uom_conversion.csv"), index=False)

pd.DataFrame({
    "warehouse_code": wh_codes,
    "warehouse_name": ["Bhiwandi DC", "Chakan DC", "Hoskote DC", "Sriperumbudur DC",
                       "Bhiwadi DC", "Dankuni DC", "Hyderabad DC", "Butibori DC"],
    "city": ["Mumbai", "Pune", "Bengaluru", "Chennai", "Delhi", "Kolkata", "Hyderabad", "Nagpur"],
    "region_name": ["West", "West", "South", "South", "North", "East", "South", "Central"],
    "timezone": ["Asia/Kolkata"] * 8,
    "chilled_capacity_pallets": [420, 260, 780, 350, 610, 190, 300, 150],
}).to_csv(os.path.join(ref, "warehouse_master.csv"), index=False)

pd.DataFrame({
    "carrier_id": ["CR-101", "CR-102", "CR-103", "CR-104", "CR-105"],
    "carrier_name": ["Bluewheel Logistics", "Trident Roadways", "ColdLink Express",
                     "Eastbound Freight", "Metro Milkrun"],
    "mode": ["SURFACE", "SURFACE", "REEFER", "SURFACE", "LCV"],
    "sla_hours": [24, 36, 18, 48, 12],
    "rate_per_km": [38.5, 31.0, 52.75, 27.4, 44.2],
}).to_csv(os.path.join(ref, "carrier_master.csv"), index=False)

cal = pd.DataFrame({"calendar_date": DAYSTR})
cd = pd.to_datetime(cal["calendar_date"])
fy = np.where(cd.dt.month >= 4, cd.dt.year, cd.dt.year - 1)
cal["fiscal_year"] = ["FY" + str(y + 1)[-2:] for y in fy]
cal["fiscal_quarter"] = "Q" + (((cd.dt.month - 4) % 12) // 3 + 1).astype(str)
cal["fiscal_month_no"] = ((cd.dt.month - 4) % 12) + 1
cal["iso_week"] = cd.dt.isocalendar().week.values
cal["day_of_week"] = cd.dt.day_name().values
cal["is_weekend"] = cd.dt.dayofweek.isin([5, 6]).astype(int).values
cal.to_csv(os.path.join(ref, "fiscal_calendar.csv"), index=False)

# DEFECT L17: the legacy finance report is wrong in two ways. It groups by ingest
# date rather than event date, and it does not deduplicate. Reconciling to it
# exactly means both bugs have been reproduced.
leg = DAYSTR[::7]
pd.DataFrame({
    "week_ending": np.repeat(leg, 4),
    "channel": np.tile(CHANNELS, len(leg)),
    "gross_sales_inr": np.round(rng.uniform(4_000_000, 26_000_000, len(leg) * 4), 2),
    "units_sold": rng.integers(40_000, 320_000, len(leg) * 4),
    "basket_count": rng.integers(9_000, 70_000, len(leg) * 4),
    "prepared_by": "Finance MIS",
    "note": "As published. Source: nightly extract.",
}).to_csv(os.path.join(ref, "legacy_finance_weekly_report.csv"), index=False)

# =============================================================== 6. MANIFEST
print("manifest ...", flush=True)
rows = []
for feed in ["pos_transactions", "reefer_telemetry", "wms_scan_events"]:
    root = os.path.join(OUT, "raw", feed)
    for part in sorted(os.listdir(root)):
        pdir = os.path.join(root, part)
        files = [f for f in sorted(os.listdir(pdir)) if f.endswith(".parquet")]
        rc = sum(pq.ParquetFile(os.path.join(pdir, f)).metadata.num_rows for f in files)
        rows.append({"feed": feed, "partition": part, "file_count": len(files), "row_count": rc,
                     "bytes": sum(os.path.getsize(os.path.join(pdir, f)) for f in files)})
os.makedirs(os.path.join(OUT, "_manifest"), exist_ok=True)
man = pd.DataFrame(rows)
man.to_csv(os.path.join(OUT, "_manifest", "expected_partitions.csv"), index=False)

# DEFECT L18: one part file is truncated but still listed in the manifest.
bad = os.path.join(OUT, "raw", "reefer_telemetry", "dt=2025-07-14", "part-00000.parquet")
if os.path.exists(bad):
    b = open(bad, "rb").read()
    open(bad, "wb").write(b[:int(len(b) * 0.72)])
    print("  truncated:", bad, flush=True)

print("\nrow counts")
tot = 0
for feed in ["pos_transactions", "reefer_telemetry", "wms_scan_events"]:
    c = int(man.loc[man.feed == feed, "row_count"].sum())
    tot += c
    print(f"  {feed:<26} {c:>12,}")
print(f"  {'erp_cdc/sales_order_header':<26} {soh_rows:>12,}")
print(f"  {'erp_cdc/outlet_master':<26} {len(cdc_outlet):>12,}")
print(f"  {'erp_cdc/product_master':<26} {len(cdc_product):>12,}")
tot += soh_rows + len(cdc_outlet) + len(cdc_product)
print(f"  {'TOTAL':<26} {tot:>12,}")
