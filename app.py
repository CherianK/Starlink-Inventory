# app.py
# Starlink Inventory — Data Check, SL Crosswalk, SS/ROP/OUL, and Bucket-4 Eligibility

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from typing import Optional, Dict

import numpy as np
import pandas as pd
import streamlit as st
from scipy.stats import norm

# -------------------------------------------------------------------
# Bucket display names — MUST match your Excel
# -------------------------------------------------------------------
BUCKET_CORE       = "1 - Core Mobile Devices"
BUCKET_ESSENTIAL  = "2 - Essential Accessories"
BUCKET_COMPUTING  = "3 - Computing & Smart Devices"
BUCKET_SPECIALTY  = "4 - Specialty / Project Services"
BUCKETS_ORDER = [BUCKET_CORE, BUCKET_ESSENTIAL, BUCKET_COMPUTING, BUCKET_SPECIALTY]

st.set_page_config(page_title="Starlink Inventory", layout="wide", initial_sidebar_state="expanded")
st.title("Starlink Inventory — Data Check & Diagnostics")

# ----------------------------
# Sidebar: workbook & settings
# ----------------------------
with st.sidebar:
    st.header("Workbook")
    up = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"])
    pth = st.text_input("…or paste a full path to your workbook", value="")

    st.divider()
    st.header("Aging workbook (for savings)")
    aging_up = st.file_uploader("Upload CONSOLIDATED aging (.xlsx)", type=["xlsx"], key="aging")
    aging_pth = st.text_input("…or paste path to aging workbook", value="", key="aging_pth")

    st.divider()
    st.header("Display filters")
    hide_zero_mu = st.checkbox("Hide SKUs with Avg Daily Demand = 0 / NaN", value=True)
    hide_imputed = st.checkbox("Hide imputed rows (Leadtime IsImputed=True)", value=False)

    st.divider()
    st.header("Stock eligibility (Bucket 4 only)")
    enable_gate = st.checkbox("Enable Bucket-4 stock eligibility gate", value=True)
    thr_active_months = st.number_input(
        "Active months (last 12) ≥",
        value=3,
        min_value=1,
        max_value=12,
        step=1,
    )
    thr_order_lines = st.number_input(
        "Order lines (last 12 mo) ≥",
        value=3,
        min_value=1,
        step=1,
    )
    thr_zdp_max = st.number_input(
        "Zero-day % (last 12 mo) ≤",
        value=0.80,
        min_value=0.0,
        max_value=1.0,
        step=0.01,
        format="%.2f",
    )
    thr_cov_max = st.number_input(
        "CoV (last 12 mo) ≤",
        value=1.50,
        min_value=0.0,
        step=0.05,
        format="%.2f",
    )

    st.divider()
    st.header("Settings (fallback SL table)")
    st.caption(
        "If your workbook includes a 36-row SL table (Bucket×ABCXYZ), it will be used. "
        "Otherwise these knobs generate a default crosswalk."
    )
    base_b1 = st.slider(f"{BUCKET_CORE} base",      0.88, 0.999, 0.985, 0.001)
    base_b2 = st.slider(f"{BUCKET_ESSENTIAL} base", 0.88, 0.999, 0.973, 0.001)
    base_b3 = st.slider(f"{BUCKET_COMPUTING} base", 0.88, 0.999, 0.961, 0.001)
    base_b4 = st.slider(f"{BUCKET_SPECIALTY} base", 0.88, 0.999, 0.905, 0.001)

    st.caption("ABC tilt (importance): A > B > C")
    aA = st.number_input("A adjustment", value=0.010, step=0.001, format="%.3f")
    aB = st.number_input("B adjustment", value=0.004, step=0.001, format="%.3f")
    aC = 0.0

    st.caption("XYZ tilt (predictability): X > Y > Z (Z lightly penalized)")
    xX = st.number_input("X adjustment", value=0.004, step=0.001, format="%.3f")
    xY = st.number_input("Y adjustment", value=0.001, step=0.001, format="%.3f")
    xZ = st.number_input("Z adjustment", value=-0.004, step=0.001, format="%.3f")

    st.caption("Clamp for generated SL values")
    clamp_lo, clamp_hi = st.slider("Clamp range", 0.88, 0.999, (0.88, 0.999), 0.001)

    st.caption("Review settings (only used if SL sheet missing/partial)")
    periodic_buckets = st.multiselect(
        "Buckets reviewed periodically (batching)",
        BUCKETS_ORDER,
        default=[],  # you set Continuous in workbook; default = none periodic here
    )
    review_days_periodic = st.number_input(
        "Periodic review days",
        value=14,
        min_value=1,
        max_value=60,
        step=1,
    )

# ----------------------------
# Helpers
# ----------------------------
def open_xlsx(uploaded, path_text) -> Optional[pd.ExcelFile]:
    if uploaded is not None:
        return pd.ExcelFile(uploaded)
    if path_text.strip():
        p = Path(path_text).expanduser()
        if p.exists():
            return pd.ExcelFile(p)
        else:
            st.error(f"Path not found: {p}")
    return None


def find_sheet(xls: pd.ExcelFile, candidates: list[str]) -> Optional[str]:
    names = {s.lower(): s for s in xls.sheet_names}
    for c in candidates:
        if c.lower() in names:
            return names[c.lower()]
    return None


def load_lead(xls: pd.ExcelFile) -> Optional[pd.DataFrame]:
    nm = find_sheet(xls, ["Leadtime_wSTD", "Leadtime", "leadtime_wstd"])
    if not nm:
        st.error("Leadtime sheet not found (expected e.g., 'Leadtime_wSTD').")
        return None
    df = xls.parse(nm)
    need = [
        "Category", "SubCategory", "Brand", "SKU", "Item Name", "MacroBucket", "ABC-XYZ",
        "LeadTime_Final_days", "Std_LeadTime_Final_days", "IsImputed"
    ]
    missing = [c for c in need if c not in df.columns]
    if missing:
        st.warning(f"Leadtime sheet missing columns: {missing}")
    return df


def load_sales(xls: pd.ExcelFile) -> Optional[pd.DataFrame]:
    nm = find_sheet(xls, ["Sales Optimised & Cleaned", "Sales Optimized & Cleaned", "Sales"])
    if not nm:
        return None
    df = xls.parse(nm)
    need = ["SKU", "Date", "Warehouse", "TransactionType", "QTY"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        st.warning(f"Sales sheet missing columns: {missing}")
    if "Date" in df:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"])
        df["Date"] = df["Date"].dt.normalize()
    return df


def load_purchase(xls: pd.ExcelFile) -> Optional[pd.DataFrame]:
    nm = find_sheet(xls, ["Purchases Optimised & Clean", "Purchases"])
    return xls.parse(nm) if nm else None


def load_sku_demand_stats(xls: pd.ExcelFile) -> Optional[pd.DataFrame]:
    nm = find_sheet(xls, ["SKU_DemandStats", "DemandStats"])
    return xls.parse(nm) if nm else None

def load_aging_workbook(xls: Optional[pd.ExcelFile]) -> pd.DataFrame:
    """
    Read all sheets from CONSOLIDATED aging.xlsx into one table.
    Expects columns:
      Item_number, Product_name, Inventory_quantity, Inventory_value,
      Under_180days, 181-365(Days), Above_365days
    """
    if xls is None:
        return pd.DataFrame()

    frames = []
    required = [
        "Item_number", "Product_name",
        "Inventory_quantity", "Inventory_value",
        "Under_180days", "181-365(Days)", "Above_365days",
    ]

    for sheet in xls.sheet_names:
        df = xls.parse(sheet)
        missing = [c for c in required if c not in df.columns]
        if missing:
            st.warning(f"Aging sheet '{sheet}' missing columns: {missing}")
            continue

        # Parse sheet name like '31Jan2024' to a proper date, fall back to raw name
        dt = pd.to_datetime(sheet, format="%d%b%Y", errors="coerce")
        df["MonthEnd"] = dt
        df["MonthLabel"] = dt.strftime("%Y-%m-%d") if pd.notna(dt) else sheet

        frames.append(df)

    if not frames:
        return pd.DataFrame()

    aging = pd.concat(frames, ignore_index=True)

    # Clean numeric fields and compute unit cost
    aging["Inventory_quantity"] = pd.to_numeric(aging["Inventory_quantity"], errors="coerce")
    aging["Inventory_value"] = pd.to_numeric(aging["Inventory_value"], errors="coerce")
    aging["UnitCost"] = np.where(
        aging["Inventory_quantity"] > 0,
        aging["Inventory_value"] / aging["Inventory_quantity"],
        np.nan,
    )
    return aging


def compute_excess_inventory(
    aging: pd.DataFrame,
    policy_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge aging snapshots with policy outputs and compute, per SKU & month:

      AboveROP       = Inventory_quantity - ROP
      ExcessQty      = max(AboveROP, 0)          (over-stock vs ROP)
      ShortfallQty   = max(-AboveROP, 0)         (under-stock vs ROP)
      ExcessValue    = ExcessQty * UnitCost
      ShortfallValue = ShortfallQty * UnitCost

    Bucket 4 is NOT hard-excluded here. We handle whether to include it
    in the totals inside the Savings tab via filters.
    """
    if aging.empty or policy_df.empty:
        return pd.DataFrame()

    # Pull only the pieces we actually need from the policy output,
    # including Category/SubCategory/Brand for richer savings filters.
    wanted_cols = [
        "SKU",
        "Bucket",
        "ABCXYZ",
        "ROP_or_OrderUpTo",
        "Category",
        "SubCategory",
        "Brand",
        "Item Name",
    ]
    existing_cols = [c for c in wanted_cols if c in policy_df.columns]
    pol = policy_df[existing_cols].copy()

    pol["ROP_or_OrderUpTo"] = pd.to_numeric(pol["ROP_or_OrderUpTo"], errors="coerce")

    merged = aging.merge(
        pol,
        left_on="Item_number",
        right_on="SKU",
        how="left",
    )

    # Drop rows where we have no ROP at all (no policy for that item)
    merged = merged[merged["ROP_or_OrderUpTo"].notna()].copy()

    # Flag Bucket 4 for later filtering in the UI
    merged["Bucket"] = merged["Bucket"].astype(str)
    merged["IsBucket4"] = merged["Bucket"].eq(BUCKET_SPECIALTY)

    # Over/under vs ROP
    merged["AboveROP"] = merged["Inventory_quantity"] - merged["ROP_or_OrderUpTo"]

    merged["ExcessQty"] = np.clip(merged["AboveROP"], a_min=0, a_max=None)
    merged["ShortfallQty"] = np.clip(-merged["AboveROP"], a_min=0, a_max=None)

    merged["ExcessValue"] = merged["ExcessQty"] * merged["UnitCost"]
    merged["ShortfallValue"] = merged["ShortfallQty"] * merged["UnitCost"]

    return merged


def to_demand_columns(sales: pd.DataFrame) -> pd.DataFrame:
    """Two transaction types only: 'Sale' and 'CustomerReturn'. Convert to daily demand with positive sales."""
    s = sales.copy()
    s["tx"] = s["TransactionType"].astype(str).str.strip().str.lower()

    qty_abs = pd.to_numeric(s["QTY"], errors="coerce").abs()

    is_sale = s["tx"].eq("sale")
    is_ret = s["tx"].eq("customerreturn")

    s["qty_exclR"] = np.where(is_sale, qty_abs, 0.0)
    s["qty_net"] = np.where(is_sale, qty_abs,
                     np.where(is_ret, -qty_abs, np.nan))

    return s


def recompute_sku_demand_stats(sales: pd.DataFrame) -> pd.DataFrame:
    s = to_demand_columns(sales).dropna(subset=["SKU", "Date"])
    if s.empty:
        st.error("Sales has no valid dates after filtering.")
        return pd.DataFrame()

    min_d = s["Date"].min().date()
    max_d = s["Date"].max().date()
    if pd.isna(min_d) or pd.isna(max_d):
        st.error("Sales has no valid dates.")
        return pd.DataFrame()

    d_net = s.dropna(subset=["qty_net"]).groupby(["SKU", "Date"], as_index=False)["qty_net"].sum()
    d_excl = s.groupby(["SKU", "Date"], as_index=False)["qty_exclR"].sum()

    skus = pd.Index(sorted(pd.unique(pd.concat([d_excl["SKU"], d_net["SKU"]]))), name="SKU")
    days = pd.Index(pd.date_range(min_d, max_d, freq="D").date, name="Date")
    grid = pd.MultiIndex.from_product([skus, days])

    dn = d_net.set_index(["SKU", "Date"]).reindex(grid, fill_value=0.0)
    de = d_excl.set_index(["SKU", "Date"]).reindex(grid, fill_value=0.0)

    def agg_stats(v: pd.Series) -> pd.Series:
        arr = v.values.astype(float)
        total = len(arr)
        active = (arr > 0).sum()
        avg = arr.mean()
        std = arr.std(ddof=1) if total > 1 else 0.0
        zdp = (total - active) / total if total > 0 else np.nan
        cov = (std / avg) if avg > 0 else np.nan
        return pd.Series({"avg": avg, "std": std, "active": active, "total": total, "zdp": zdp, "cov": cov})

    g_excl = de.groupby(level=0)["qty_exclR"].apply(agg_stats)
    g_net = dn.groupby(level=0)["qty_net"].apply(agg_stats)

    out = pd.DataFrame({"SKU": g_excl.index})
    out["Demand_Avg_per_day_net"] = g_net["avg"].values
    out["Demand_Std_per_day_net"] = g_net["std"].values
    out["Demand_ActiveDays_net"] = g_net["active"].values.astype(int)
    out["Demand_Avg_per_day_exclReturns"] = g_excl["avg"].values
    out["Demand_Std_per_day_exclReturns"] = g_excl["std"].values
    out["Demand_ActiveDays_exclReturns"] = g_excl["active"].values.astype(int)
    out["Demand_TotalDays"] = g_excl["total"].values.astype(int)
    out["Demand_ZeroDayPct_net"] = g_net["zdp"].values
    out["Demand_ZeroDayPct_exclReturns"] = g_excl["zdp"].values
    out["Demand_CoV_net"] = g_net["cov"].values
    out["Demand_CoV_exclReturns"] = g_excl["cov"].values
    return out


# ---------- 12-month recency features from Sales ----------
def last12m_features(sales: Optional[pd.DataFrame]) -> pd.DataFrame:
    if sales is None or sales.empty:
        return pd.DataFrame(columns=[
            "SKU", "ActiveMonths_12m", "OrderLines_12m",
            "ZeroDayPct_12m", "CoV_excl_12m"
        ])
    s = to_demand_columns(sales).dropna(subset=["SKU", "Date"])
    if s.empty:
        return pd.DataFrame(columns=[
            "SKU", "ActiveMonths_12m", "OrderLines_12m",
            "ZeroDayPct_12m", "CoV_excl_12m"
        ])

    end = s["Date"].max().normalize()
    start = (end - pd.Timedelta(days=365)).normalize() + pd.Timedelta(days=1)
    s12 = s[(s["Date"] >= start) & (s["Date"] <= end)].copy()

    # Order lines: number of positive sale lines
    ol = s12[s12["qty_exclR"] > 0.0].groupby("SKU").size().rename("OrderLines_12m")

    # Active months: months with positive sale quantity
    s12["Month"] = s12["Date"].values.astype("datetime64[M]")
    msum = s12.groupby(["SKU", "Month"])["qty_exclR"].sum().reset_index()
    am = msum[msum["qty_exclR"] > 0.0].groupby("SKU").size().rename("ActiveMonths_12m")

    # Daily grid for 12m zero-day% and CoV (excl returns)
    days = pd.date_range(start, end, freq="D")
    d = s12.groupby(["SKU", "Date"], as_index=False)["qty_exclR"].sum()
    skus = pd.Index(d["SKU"].unique(), name="SKU")
    grid = pd.MultiIndex.from_product([skus, days], names=["SKU", "Date"])
    de = d.set_index(["SKU", "Date"]).reindex(grid, fill_value=0.0)["qty_exclR"].reset_index()

    stats = de.groupby("SKU")["qty_exclR"].agg(
        mean="mean",
        std=lambda x: float(np.std(x.values, ddof=1)) if len(x) > 1 else 0.0,
        active=lambda x: int((x > 0).sum()),
        total=lambda x: int(len(x))
    )
    stats["ZeroDayPct_12m"] = (stats["total"] - stats["active"]) / stats["total"]
    stats["CoV_excl_12m"] = stats.apply(
        lambda r: (r["std"] / r["mean"]) if r["mean"] > 0 else np.nan, axis=1
    )

    out = pd.DataFrame(index=skus).join([
        ol, am, stats[["ZeroDayPct_12m", "CoV_excl_12m"]]
    ])
    out = out.fillna({"OrderLines_12m": 0, "ActiveMonths_12m": 0})
    out = out.reset_index().rename(columns={"index": "SKU"})
    return out


def make_default_sl(
    base_b1: float, base_b2: float, base_b3: float, base_b4: float,
    aA: float, aB: float, aC: float,
    xX: float, xY: float, xZ: float,
    clamp_lo: float, clamp_hi: float,
    periodic_buckets: list[str],
    review_days_periodic: int
) -> pd.DataFrame:
    base_map = {
        BUCKET_CORE: base_b1,
        BUCKET_ESSENTIAL: base_b2,
        BUCKET_COMPUTING: base_b3,
        BUCKET_SPECIALTY: base_b4,
    }
    abc = {"A": aA, "B": aB, "C": aC}
    xyz = {"X": xX, "Y": xY, "Z": xZ}

    rows = []
    for b in BUCKETS_ORDER:
        for A in ("A", "B", "C"):
            for X in ("X", "Y", "Z"):
                sl = float(np.clip(round(base_map[b] + abc[A] + xyz[X], 3), clamp_lo, clamp_hi))
                z = float(norm.ppf(sl)) if 0 < sl < 1 else np.nan
                policy = "Periodic" if b in periodic_buckets else "Continuous"
                period = review_days_periodic if policy == "Periodic" else np.nan
                rows.append({
                    "Bucket": b,
                    "ABCXYZ": f"{A}-{X}",
                    "ReviewPolicy": policy,
                    "ReviewPeriod_days": period,
                    "SL": sl,
                    "Z_value": z,
                })
    return pd.DataFrame(rows)


def load_sl_crosswalk(xls: pd.ExcelFile) -> Optional[pd.DataFrame]:
    nm = find_sheet(xls, ["SL_Brandwise", "SL_ByBucket_ABCXYZ", "SL"])
    if not nm:
        return None
    sl = xls.parse(nm)
    keep = [c for c in ["Bucket", "ABCXYZ", "ReviewPolicy", "ReviewPeriod_days", "SL", "Z_value"] if c in sl.columns]
    sl = sl[keep].copy()
    if "Bucket" in sl:
        sl["Bucket"] = sl["Bucket"].astype(str)
    if "SL" in sl:
        sl["SL"] = pd.to_numeric(sl["SL"], errors="coerce")
    if "ReviewPeriod_days" in sl:
        sl["ReviewPeriod_days"] = pd.to_numeric(sl["ReviewPeriod_days"], errors="coerce")
    if "Z_value" not in sl.columns:
        sl["Z_value"] = pd.NA
    sl["Z_value"] = pd.to_numeric(sl["Z_value"], errors="coerce")
    need_z = sl["Z_value"].isna() & sl["SL"].between(0, 1)
    sl.loc[need_z, "Z_value"] = sl.loc[need_z, "SL"].map(lambda p: norm.ppf(p))
    return sl


def attach_sl_to_lead(lead: pd.DataFrame, sl: pd.DataFrame, sl_was_default: bool) -> pd.DataFrame:
    df = lead.copy()
    df["Bucket"] = df["MacroBucket"].astype(str)
    df["ABCXYZ"] = df["ABC-XYZ"].astype(str)
    cols = ["Bucket", "ABCXYZ", "SL", "Z_value", "ReviewPolicy", "ReviewPeriod_days"]
    out = df.merge(sl[cols], on=["Bucket", "ABCXYZ"], how="left")
    out["Lead_Imputed"] = out.get("IsImputed").astype("boolean")
    out["SL_Imputed"] = bool(sl_was_default)
    return out


def compute_policy_outputs(
    lead_sl: pd.DataFrame,
    dem: pd.DataFrame,
    periodic_buckets: list[str],
    review_days_periodic: int,
    sl_from_workbook: bool
) -> pd.DataFrame:
    df = lead_sl.copy()

    dem_use = dem[[
        "SKU",
        "Demand_Avg_per_day_exclReturns", "Demand_Std_per_day_exclReturns",
        "Demand_Avg_per_day_net", "Demand_Std_per_day_net",
        "Demand_ZeroDayPct_exclReturns", "Demand_CoV_exclReturns"
    ]].copy()
    df = df.merge(dem_use, on="SKU", how="left")

    def pick_mu(row):
        a = row.get("Demand_Avg_per_day_exclReturns")
        b = row.get("Demand_Avg_per_day_net")
        return a if pd.notna(a) else (abs(b) if pd.notna(b) else np.nan)

    def pick_sd(row):
        a = row.get("Demand_Std_per_day_exclReturns")
        b = row.get("Demand_Std_per_day_net")
        return a if pd.notna(a) else (abs(b) if pd.notna(b) else np.nan)

    df["Mu_D"] = df.apply(pick_mu, axis=1)
    df["Sigma_D"] = df.apply(pick_sd, axis=1)
    df["ZeroDayPct_all"] = pd.to_numeric(df.get("Demand_ZeroDayPct_exclReturns"), errors="coerce")
    df["CoV_excl_all"] = pd.to_numeric(df.get("Demand_CoV_exclReturns"), errors="coerce")

    df["Mu_L"] = pd.to_numeric(df.get("LeadTime_Final_days"), errors="coerce")
    df["Sigma_L"] = pd.to_numeric(df.get("Std_LeadTime_Final_days"), errors="coerce")

    df["Bucket"] = df["Bucket"].astype(str)
    df["SL"] = pd.to_numeric(df.get("SL"), errors="coerce")
    df["Z_value"] = pd.to_numeric(df.get("Z_value"), errors="coerce")

    if sl_from_workbook and df["SL"].isna().any():
        default_sl = make_default_sl(
            base_b1, base_b2, base_b3, base_b4,
            aA, aB, aC, xX, xY, xZ,
            clamp_lo, clamp_hi,
            periodic_buckets, review_days_periodic
        )
        before = df["SL"].isna()
        df = df.drop(columns=["SL", "Z_value", "ReviewPolicy", "ReviewPeriod_days"], errors="ignore") \
               .merge(default_sl, on=["Bucket", "ABCXYZ"], how="left")
        df["SL_Imputed"] = df.get("SL_Imputed", False) | before

    need_z = df["Z_value"].isna() & df["SL"].between(0, 0.999999)
    df.loc[need_z, "Z_value"] = df.loc[need_z, "SL"].map(lambda p: norm.ppf(p))

    rp_missing = df["ReviewPolicy"].isna()
    if rp_missing.any():
        is_periodic = df.loc[rp_missing, "Bucket"].isin(periodic_buckets)
        df.loc[rp_missing, "ReviewPolicy"] = np.where(is_periodic, "Periodic", "Continuous")

    df["ReviewPeriod_days"] = pd.to_numeric(df.get("ReviewPeriod_days"), errors="coerce")
    is_periodic_all = df["Bucket"].isin(periodic_buckets)
    df.loc[~is_periodic_all, "ReviewPeriod_days"] = np.nan
    df.loc[is_periodic_all & df["ReviewPeriod_days"].isna(), "ReviewPeriod_days"] = review_days_periodic

    df["Coverage_days"] = np.where(
        df["ReviewPolicy"].eq("Periodic"),
        df["Mu_L"] + df["ReviewPeriod_days"],
        df["Mu_L"]
    )

    df["Sigma_DuringLT"] = np.sqrt(
        (df["Mu_D"] ** 2) * (df["Sigma_L"] ** 2) + (df["Sigma_D"] ** 2) * df["Mu_L"]
    )
    df["Sigma_DuringCoverage"] = np.sqrt(
        (df["Mu_D"] ** 2) * (df["Sigma_L"] ** 2) + (df["Sigma_D"] ** 2) * df["Coverage_days"]
    )

    df["SafetyStock"] = np.where(
        df["ReviewPolicy"].eq("Periodic"),
        df["Z_value"] * df["Sigma_DuringCoverage"],
        df["Z_value"] * df["Sigma_DuringLT"]
    )

    df["MeanDemand_During"] = np.where(
        df["ReviewPolicy"].eq("Periodic"),
        df["Mu_D"] * df["Coverage_days"],
        df["Mu_D"] * df["Mu_L"]
    )

    df["ROP_or_OrderUpTo"] = df["MeanDemand_During"] + df["SafetyStock"]

    return df


def df_download_xlsx(sheets: Dict[str, pd.DataFrame], filename="starlink_outputs.xlsx") -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as xw:
        for name, df in sheets.items():
            df.to_excel(xw, sheet_name=name[:31], index=False)
    buf.seek(0)
    return buf.read()

# ----------------------------
# Main flow
# ----------------------------
xls = open_xlsx(up, pth)
aging_xls = open_xlsx(aging_up, aging_pth)
aging_tbl = load_aging_workbook(aging_xls)

if not xls:
    st.info("Upload your Excel workbook or paste a path in the sidebar to begin.")
    st.stop()

lead = load_lead(xls)
sales = load_sales(xls)
purch = load_purchase(xls)
dem_sheet = load_sku_demand_stats(xls)
sl_from_wb = load_sl_crosswalk(xls)

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Leadtime rows", 0 if lead is None else len(lead))
with col2:
    st.metric("Sales rows", 0 if sales is None else len(sales))
with col3:
    st.metric("Purchases rows", 0 if purch is None else len(purch) if purch is not None else 0)

if lead is None:
    st.stop()

# SL crosswalk
if sl_from_wb is None:
    st.warning("No SL crosswalk sheet found — generating default 36-row table from Settings.")
    sl_tbl = make_default_sl(
        base_b1, base_b2, base_b3, base_b4,
        aA, aB, aC, xX, xY, xZ,
        clamp_lo, clamp_hi,
        periodic_buckets, review_days_periodic
    )
    sl_from_workbook = False
    sl_was_default = True
else:
    st.success("SL crosswalk loaded from workbook.")
    sl_tbl = sl_from_wb.copy()
    sl_from_workbook = True
    sl_was_default = False

# Demand stats
if dem_sheet is None:
    if sales is None:
        st.error("Neither SKU_DemandStats nor Sales available. Cannot compute policy outputs.")
        st.stop()
    st.warning("SKU_DemandStats sheet missing — recomputing from Sales (Sale, CustomerReturn).")
    dem_tbl = recompute_sku_demand_stats(sales)
else:
    st.success("SKU_DemandStats read from workbook.")
    dem_tbl = dem_sheet.copy()

# Attach SL & compute outputs
lead_sl = attach_sl_to_lead(lead, sl_tbl, sl_was_default)
out_df = compute_policy_outputs(
    lead_sl, dem_tbl,
    periodic_buckets=periodic_buckets,
    review_days_periodic=review_days_periodic,
    sl_from_workbook=sl_from_workbook
)

# Compute 12m features and merge in
feat12 = last12m_features(sales)
out_df = out_df.merge(feat12, on="SKU", how="left")

# ---------- Bucket-4 eligibility gate ----------
def bucket4_gate_apply(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    is_b4 = df["Bucket"].eq(BUCKET_SPECIALTY)

    # Default for non-Bucket4: Stock Policy / Eligibility Reason = "N/A"
    df["Stock Policy"] = np.where(is_b4, "Stock", "N/A")
    df["Eligibility Reason"] = np.where(is_b4, "", "N/A")
    df["Note"] = ""

    if not enable_gate:
        return df

    # Rule flags
    m_ok = (df["ActiveMonths_12m"].fillna(0) >= thr_active_months)
    l_ok = (df["OrderLines_12m"].fillna(0) >= thr_order_lines)
    zc_ok = (df["ZeroDayPct_12m"].fillna(1.0) <= thr_zdp_max) & \
            (df["CoV_excl_12m"].fillna(np.inf) <= thr_cov_max)

    eligible = m_ok | l_ok | zc_ok
    not_eligible_mask = is_b4 & (~eligible)

    def reason_row(r) -> str:
        if not r.get("Bucket") == BUCKET_SPECIALTY:
            return "N/A"
        reasons = []
        if r.get("ActiveMonths_12m", 0) >= thr_active_months:
            reasons.append("active-months")
        if r.get("OrderLines_12m", 0) >= thr_order_lines:
            reasons.append("order-lines")
        if (pd.notna(r.get("ZeroDayPct_12m")) and pd.notna(r.get("CoV_excl_12m")) and
            (r["ZeroDayPct_12m"] <= thr_zdp_max) and (r["CoV_excl_12m"] <= thr_cov_max)):
            reasons.append("low zero-day% & low variability")
        if reasons:
            return "eligible: " + ", ".join(reasons)
        else:
            return "MTO: no rule met"

    df.loc[is_b4, "Eligibility Reason"] = df[is_b4].apply(reason_row, axis=1)

    df.loc[not_eligible_mask, "Stock Policy"] = "Make-to-Order"
    df.loc[not_eligible_mask, "Note"] = "Do not stock — buy against project"
    for c in ["SafetyStock", "MeanDemand_During", "ROP_or_OrderUpTo"]:
        if c in df:
            df.loc[not_eligible_mask, c] = 0.0

    return df

out_df = bucket4_gate_apply(out_df)

# Compute excess-inventory view (savings) using aging workbook + policy outputs
savings_detail = compute_excess_inventory(aging_tbl, out_df)

# ----------------------------
# Apply display filters
# ----------------------------
dem_view = dem_tbl.copy()
if hide_zero_mu and "Demand_Avg_per_day_exclReturns" in dem_view:
    dem_view = dem_view[dem_view["Demand_Avg_per_day_exclReturns"].fillna(0) > 0]

if hide_imputed and "IsImputed" in lead.columns:
    imp_map = lead[["SKU", "IsImputed"]].drop_duplicates()
    dem_view = dem_view.merge(imp_map, on="SKU", how="left")
    dem_view = dem_view[~dem_view["IsImputed"].fillna(False)]
    dem_view = dem_view.drop(columns=["IsImputed"])

po_view = out_df.copy()
if hide_zero_mu and "Mu_D" in po_view:
    po_view = po_view[po_view["Mu_D"].fillna(0) > 0]
if hide_imputed and "Lead_Imputed" in po_view:
    po_view = po_view[~po_view["Lead_Imputed"].fillna(False)]

# ----------------------------
# Friendly column names
# ----------------------------
rename_map = {
    "Category": "Category",
    "SubCategory": "Subcategory",
    "Brand": "Brand",
    "SKU": "SKU",
    "Item Name": "Item Name",
    "Bucket": "Bucket",
    "ABCXYZ": "ABCXYZ",
    "ReviewPolicy": "Review Policy",
    "ReviewPeriod_days": "Review Period (days)",
    "SL": "Service Level Target",
    "Z_value": "Z (service index)",
    "Mu_D": "Avg Daily Demand",
    "Sigma_D": "Std Dev of Daily Demand",
    "Mu_L": "Avg Lead Time (days)",
    "Sigma_L": "Std Dev Lead Time (days)",
    "Coverage_days": "Coverage Window (days)",
    "SafetyStock": "Safety Stock (units)",
    "MeanDemand_During": "Expected Demand over Window (units)",
    "ROP_or_OrderUpTo": "Reorder Point / Order-Up-To (units)",
    "Lead_Imputed": "Lead Time Imputed?",
    "ActiveMonths_12m": "Active Months (12m)",
    "OrderLines_12m": "Order Lines (12m)",
    "ZeroDayPct_12m": "Zero-day % (excl returns)",
    "CoV_excl_12m": "CoV (excl returns)",
}
po_friendly = po_view.rename(columns=rename_map)

# ----------------------------
# Small helper for "select all / clear" multiselects
# ----------------------------
def multiselect_with_select_all(label, options, session_key_prefix):
    """
    Multiselect that remembers choices in session_state and
    provides 'Select all' / 'Clear' buttons.
    """
    if "options" not in st.session_state.get(session_key_prefix, {}):
        st.session_state[session_key_prefix] = {
            "options": options,
            "selected": list(options),
        }
    else:
        # Drop any values that no longer exist
        current = st.session_state[session_key_prefix]
        current["options"] = options
        current["selected"] = [v for v in current["selected"] if v in options]

    state = st.session_state[session_key_prefix]

    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("Select all", key=f"{session_key_prefix}_all"):
            state["selected"] = list(options)
    with c2:
        if st.button("Clear", key=f"{session_key_prefix}_none"):
            state["selected"] = []

    selected = st.multiselect(
        label,
        options=options,
        default=state["selected"],
        key=f"{session_key_prefix}_ms",
    )
    # Keep state in sync
    state["selected"] = selected
    return selected

# ----------------------------
# Tabs
# ----------------------------
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(
    ["Overview", "SL Table", "Demand Stats", "Policy Outputs", "Savings", "Rules", "Savings Trend"]
)

with tab1:
    st.subheader("Sheet presence")
    present = pd.DataFrame({
        "Sheet": ["Leadtime_wSTD", "Sales", "Purchases", "SKU_DemandStats", "SL Crosswalk"],
        "Found?": [
            "Yes",
            "Yes" if sales is not None else "No",
            "Yes" if purch is not None else "No",
            "Recomputed" if dem_sheet is None and sales is not None else ("Yes" if dem_sheet is not None else "No"),
            "Default (Settings)" if sl_from_wb is None else "Workbook"
        ]
    })
    st.dataframe(present, use_container_width=True)
    st.markdown("**Active display filters & gates**")
    st.write({
        "Hide Avg Daily Demand = 0/NaN": hide_zero_mu,
        "Hide imputed (Leadtime IsImputed)": hide_imputed,
        "Bucket-4 eligibility gate": enable_gate,
    })

with tab2:
    st.subheader("Service Level Crosswalk (Bucket × ABCXYZ)")
    st.dataframe(sl_tbl, use_container_width=True, height=420)

with tab3:
    st.subheader("SKU Demand Stats (daily)")
    st.caption("Averages/Std are daily; ZeroDayPct is fraction of days with zero demand over the full available window.")
    st.dataframe(dem_view, use_container_width=True, height=420)

with tab4:
    st.subheader("Computed Policy Outputs")
    show_cols = [
        "Category", "Subcategory", "Brand", "SKU", "Item Name",
        "Bucket", "ABCXYZ", "Review Policy", "Review Period (days)",
        "Service Level Target", "Z (service index)",
        "Avg Daily Demand", "Std Dev of Daily Demand",
        "Avg Lead Time (days)", "Std Dev Lead Time (days)",
        "Coverage Window (days)", "Safety Stock (units)",
        "Expected Demand over Window (units)", "Reorder Point / Order-Up-To (units)",
        "Stock Policy", "Eligibility Reason", "Note",
        "Active Months (12m)", "Order Lines (12m)",
        "Zero-day % (excl returns)", "CoV (excl returns)",
        "Lead Time Imputed?",
    ]
    existing = [c for c in show_cols if c in po_friendly.columns]
    st.dataframe(po_friendly[existing], use_container_width=True, height=560)

    xlsx_bytes = df_download_xlsx({
        "PolicyOutputs_filtered": po_friendly[existing],
        "SL_Crosswalk": sl_tbl,
        "SKU_DemandStats_filtered": dem_view
    })
    st.download_button(
        "⬇️ Download Outputs (Excel)",
        data=xlsx_bytes,
        file_name="starlink_outputs.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

with tab5:
    st.subheader("Savings from Right-Sizing Inventory")

    if aging_tbl.empty:
        st.info("Upload the CONSOLIDATED aging workbook in the sidebar to see savings.")
    elif savings_detail.empty:
        st.warning("Aging workbook loaded, but no overlapping SKUs with policy outputs (or no ROPs).")
    else:
        # --- Month selection ---
        month_labels = (
            savings_detail["MonthLabel"]
            .dropna()
            .unique()
            .tolist()
        )
        month_labels = sorted(month_labels)
        default_idx = len(month_labels) - 1 if month_labels else 0

        sel_month = st.selectbox(
            "Select snapshot month",
            month_labels,
            index=default_idx,
        )

        # Filter to selected month
        sel = savings_detail[savings_detail["MonthLabel"] == sel_month].copy()

                # --- Filters: Bucket / Category / Subcategory / Brand ---
        with st.expander("Filters (Bucket, Category, Subcategory, Brand)", expanded=True):
            st.caption("Use the checklists below. 'Select all' / 'Clear' are available for each filter.")

            # Ensure columns exist to avoid key errors
            for col_name in ["Bucket", "Category", "SubCategory", "Brand"]:
                if col_name not in sel.columns:
                    sel[col_name] = ""

            # Base options (from the *month* snapshot)
            bucket_options = [b for b in BUCKETS_ORDER if b in sel["Bucket"].unique()]
            cat_options    = sorted(sel["Category"].dropna().unique())
            subcat_options = sorted(sel["SubCategory"].dropna().unique())
            brand_options  = sorted(sel["Brand"].dropna().unique())

            # If no buckets at all, we’re done
            if not bucket_options:
                st.warning("No buckets found in this month’s snapshot.")
                sel = sel.iloc[0:0]
            else:
                # 1) Bucket checklist via multiselect_with_select_all
                bucket_selected = multiselect_with_select_all(
                    "Buckets",
                    options=bucket_options,
                    session_key_prefix="sav_buckets",
                )
                if bucket_selected:
                    sel = sel[sel["Bucket"].isin(bucket_selected)]
                else:
                    sel = sel.iloc[0:0]

            # 2) Category checklist
            if not sel.empty and cat_options:
                cat_selected = multiselect_with_select_all(
                    "Category",
                    options=cat_options,
                    session_key_prefix="sav_category",
                )
                if cat_selected:
                    sel = sel[sel["Category"].isin(cat_selected)]

            # 3) Subcategory checklist
            if not sel.empty and subcat_options:
                subcat_selected = multiselect_with_select_all(
                    "Subcategory",
                    options=subcat_options,
                    session_key_prefix="sav_subcategory",
                )
                if subcat_selected:
                    sel = sel[sel["SubCategory"].isin(subcat_selected)]

            # 4) Brand checklist
            if not sel.empty and brand_options:
                brand_selected = multiselect_with_select_all(
                    "Brand",
                    options=brand_options,
                    session_key_prefix="sav_brand",
                )
                if brand_selected:
                    sel = sel[sel["Brand"].isin(brand_selected)]
            
            # 5) ABCXYZ checklist
            abcxyz_options = sorted(sel["ABCXYZ"].dropna().unique())
            if abcxyz_options:
                abcxyz_selected = multiselect_with_select_all(
                    "ABCXYZ",
                    options=abcxyz_options,
                    session_key_prefix="sav_abcxyz",
                )
                if abcxyz_selected:
                    sel = sel[sel["ABCXYZ"].isin(abcxyz_selected)]

        # If filters eliminate everything, show zero metrics cleanly
        if sel.empty:
            st.info("No rows match the current filters.")
            total_excess_qty = 0
            total_excess_value = 0.0
            total_shortfall_qty = 0
            total_shortfall_value = 0.0
        else:
            # --- Aggregate metrics – over-stock and under-stock ---
            total_excess_qty = sel["ExcessQty"].sum()
            total_excess_value = sel["ExcessValue"].sum()

            total_shortfall_qty = sel["ShortfallQty"].sum()
            total_shortfall_value = sel["ShortfallValue"].sum()

        c1, c2 = st.columns(2)
        with c1:
            st.metric(
                "Excess units above ROP",
                f"{int(round(total_excess_qty)):,}",
            )
            st.metric(
                "Units below ROP (potential shortfall)",
                f"{int(round(total_shortfall_qty)):,}",
            )
        with c2:
            st.metric(
                "Excess inventory value",
                f"{total_excess_value:,.0f}",
            )
            st.metric(
                "Value of units below ROP",
                f"{total_shortfall_value:,.0f}",
            )

        st.caption(
            "Detail by SKU for the selected month, after applying bucket/category/subcategory/brand filters."
        )

        # --- Savings detail table: include Category, SubCategory, Brand ---
        display_cols = [
            "MonthLabel",
            "Item_number",
            "Product_name",
            "Category",
            "SubCategory",
            "Brand",
            "Bucket",
            "ABCXYZ",
            "Inventory_quantity",
            "UnitCost",
            "Inventory_value",
            "ROP_or_OrderUpTo",
            "ExcessQty",
            "ExcessValue",
            "ShortfallQty",
            "ShortfallValue",
            "Under_180days",
            "181-365(Days)",
            "Above_365days",
        ]
        existing_cols = [c for c in display_cols if c in sel.columns]
        savings_view = sel[existing_cols] if not sel.empty else sel

        st.dataframe(savings_view, use_container_width=True, height=520)

        # --- Downloadable Savings report (second Excel output) ---
        savings_xlsx = df_download_xlsx(
            {
                # Filtered month view
                f"Savings_{sel_month}": savings_view,
                # Full multi-month detail (all buckets, unfiltered) for audit
                "Savings_all_months_raw": savings_detail,
            },
            filename="starlink_savings_outputs.xlsx",
        )
        st.download_button(
            "⬇️ Download Savings Report (Excel)",
            data=savings_xlsx,
            file_name="starlink_savings_outputs.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

with tab6:
    st.subheader("Rules, Logic & Column Guide")

    rules_text = """
### 1. Demand & Lead Time

**Avg Daily Demand**  
- Computed per SKU from `SKU_DemandStats`.  
- Prefer *excluding returns* (`Demand_Avg_per_day_exclReturns`).  
- If that is missing, fallback to net (`Demand_Avg_per_day_net`) using absolute value.

**Std Dev of Daily Demand**  
- Same logic as above, but using the corresponding std-dev fields.  
- Captures how “spiky” daily demand is.

**Avg Lead Time (days)**  
- `LeadTime_Final_days` from the Leadtime sheet (after your imputation logic).  

**Std Dev Lead Time (days)**  
- `Std_LeadTime_Final_days` — variability of lead time per SKU.  

If a lead time was imputed in the workbook, **Lead Time Imputed? = TRUE**.

---

### 2. Service Level, Z, and Coverage Window

**Service Level Target (SL)**  
- From your SL crosswalk sheet (`SL` column) if present.  
- If the sheet is missing or has gaps, the fallback SL table is built from:
  - per-bucket base SL sliders, plus
  - ABC and XYZ adjustments, clamped to the chosen range.

**Z (service index)**  
- Z is the standard-normal quantile corresponding to the SL: `Z = norm.ppf(SL)`.  
  - Example: SL = 0.99 → Z ≈ 2.326  
  - Example: SL = 0.95 → Z ≈ 1.645  

**Review Policy**  
- Read from your SL sheet wherever populated.  
- If missing, the app uses the sidebar Review settings:
  - Buckets in *Periodic* list → `Review Policy = Periodic`  
  - Others → `Continuous`.

**Coverage Window (days)**  
- This is the “exposure window” we protect against.  
- Continuous review: `Coverage = Avg Lead Time (μL)`  
- Periodic review: `Coverage = Avg Lead Time (μL) + Review Period (R)`  

Because your SL table currently sets all buckets to **Continuous**, coverage is typically just the lead time.

---

### 3. Variability During the Window

With variable lead time, total uncertainty combines:

1. variability of demand *per day* (σD), and  
2. variability of how long you wait (σL).

The app uses:

- `Sigma_DuringLT       = sqrt( (Mu_D^2 * Sigma_L^2) + (Sigma_D^2 * Mu_L) )`  
- `Sigma_DuringCoverage = sqrt( (Mu_D^2 * Sigma_L^2) + (Sigma_D^2 * Coverage_days) )`

If `Sigma_L = 0`, it reduces to the classic constant-lead-time result:

- `Sigma_DuringLT ≈ Sigma_D * sqrt(Mu_L)`  

---

### 4. Safety Stock and Reorder Point

**Safety Stock (units)**  

- Continuous review: `SS = Z × Sigma_DuringLT`  
- Periodic review:   `SS = Z × Sigma_DuringCoverage`  

This is “extra stock” you hold to buffer randomness while still hitting the chosen SL.

**Expected Demand over Window (units)**  

- Continuous review:  `Expected = Mu_D × Mu_L`  
- Periodic review:    `Expected = Mu_D × Coverage_days`  

This is the baseline demand you expect to sell during the exposure window.

**Reorder Point / Order-Up-To (units)**  

- `ROP / OUL = Expected Demand over Window + Safety Stock`  

Interpretation:
- Reorder Point (continuous): when on-hand + on-order drops below this number, place an order.  
- Order-Up-To (periodic): at each review, raise stock up to this level.

---

### 5. Bucket-4 Stock Eligibility (Specialty / Project Services)

The **stock eligibility gate applies only when `Bucket = "4 - Specialty / Project Services"`**.  
Buckets 1–3 are never filtered by this gate (their Stock Policy is `N/A`).

It uses **last 12 months of Sales** to decide if an item shows repeat demand:

- **Active Months (12m)**  
  Months with any positive sales quantity (excluding returns).

- **Order Lines (12m)**  
  Count of `Sale` lines with positive quantity.

- **Zero-day % (12m)**  
  Over the last 12 months, the fraction of days with zero demand.

- **CoV (12m, excl returns)**  
  Coefficient of variation of daily demand (Std Dev / Mean), ignoring returns.

A Bucket-4 SKU is **eligible to hold stock** if **any** of these rules passes:

1. Active Months (12m) ≥ the threshold from the sidebar.  
2. Order Lines (12m) ≥ the threshold from the sidebar.  
3. Both:
   - Zero-day % (12m) ≤ the threshold, **and**
   - CoV (12m) ≤ the CoV threshold.

When a Bucket-4 SKU is **eligible**:
- **Stock Policy** = `Stock`  
- **Eligibility Reason** = a short explanation such as `"eligible: active-months"` or `"eligible: low zero-day% & low variability"`  

When a Bucket-4 SKU is **not eligible** (and the gate is enabled):
- **Stock Policy** = `Make-to-Order`  
- **Eligibility Reason** = `MTO: no rule met`  
- **Note** = `Do not stock — buy against project`  
- **Safety Stock**, **Expected Demand over Window**, and **ROP/OUL** are set to 0.

This matches your intent: for project-only, erratic items with no repeat pattern, the model **does not recommend stocking**, but still respects the lead time so you can see how long a project buy would take.


"""
    st.markdown(rules_text)

with tab7:
    st.subheader("Monthly Trend of Excess Inventory Value")

    if aging_tbl.empty:
        st.info("Upload the CONSOLIDATED aging workbook in the sidebar to see the trend.")
    elif savings_detail.empty:
        st.warning("Aging workbook loaded, but no overlapping SKUs with policy outputs (or no ROPs).")
    else:
        # --- Filters shared with Savings tab (but applied across ALL months) ---
        trend = savings_detail.copy()

        # Ensure columns exist
        for col_name in ["Bucket", "Category", "SubCategory", "Brand"]:
            if col_name not in trend.columns:
                trend[col_name] = ""

        with st.expander("Filters (Bucket, Category, Subcategory, Brand)", expanded=True):
            st.caption("Uncheck buckets to exclude them. Leave a multiselect empty to show all values.")

            # 1) Bucket checklist
            present_buckets = [b for b in BUCKETS_ORDER if b in trend["Bucket"].unique()]
            if not present_buckets:
                st.warning("No buckets found in savings detail.")
                trend = trend.iloc[0:0]
            else:
                bucket_cols = st.columns(len(present_buckets))
                active_buckets = []
                for col, b in zip(bucket_cols, present_buckets):
                    with col:
                        checked = st.checkbox(
                            b,
                            value=True,
                            key=f"trend_bucket_{b}",
                        )
                    if checked:
                        active_buckets.append(b)

                if active_buckets:
                    trend = trend[trend["Bucket"].isin(active_buckets)]
                else:
                    trend = trend.iloc[0:0]

            # 2) Category multiselect
            cat_options = sorted(trend["Category"].dropna().unique())
            cat_selected = st.multiselect(
                "Category",
                options=cat_options,
                default=[],
                key="trend_cat",
            )
            if cat_selected:
                trend = trend[trend["Category"].isin(cat_selected)]

            # 3) Subcategory multiselect
            subcat_options = sorted(trend["SubCategory"].dropna().unique())
            subcat_selected = st.multiselect(
                "Subcategory",
                options=subcat_options,
                default=[],
                key="trend_subcat",
            )
            if subcat_selected:
                trend = trend[trend["SubCategory"].isin(subcat_selected)]

            # 4) Brand multiselect
            brand_options = sorted(trend["Brand"].dropna().unique())
            brand_selected = st.multiselect(
                "Brand",
                options=brand_options,
                default=[],
                key="trend_brand",
            )
            if brand_selected:
                trend = trend[trend["Brand"].isin(brand_selected)]

        if trend.empty:
            st.info("No rows match the current filters. Trend cannot be plotted.")
        else:
            # Aggregate monthly excess value after filters
            trend_df = (
                trend
                .dropna(subset=["MonthLabel"])
                .groupby("MonthLabel", as_index=False)["ExcessValue"]
                .sum()
            ).sort_values("MonthLabel")

            if trend_df.empty:
                st.info("No excess inventory value could be computed for any month.")
            else:
                st.caption("Total excess inventory value per month after applying filters.")
                st.line_chart(
                    trend_df.set_index("MonthLabel")["ExcessValue"],
                    height=360,
                )

                latest_row = trend_df.iloc[-1]
                earliest_row = trend_df.iloc[0]

                c1, c2 = st.columns(2)
                with c1:
                    st.metric(
                        "Latest month excess value",
                        f"{latest_row['ExcessValue']:,.0f}",
                        help=f"Month: {latest_row['MonthLabel']}",
                    )
                with c2:
                    delta_val = latest_row["ExcessValue"] - earliest_row["ExcessValue"]
                    st.metric(
                        "Change vs first month (filtered set)",
                        f"{delta_val:,.0f}",
                        help=f"First month: {earliest_row['MonthLabel']}",
                    )