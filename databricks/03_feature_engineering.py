"""
03_feature_engineering.py
==========================
Sepsis Risk Dashboard — Feature Engineering
Author: Leelasaikiran

What this script does:
- Reads vitals_filtered.csv and labs_filtered.csv
- For each patient, computes rolling 1-hour feature windows
- Computes the key sepsis signals: lactate trend, MAP trend, SOFA proxy
- Joins vitals features with latest lab values per window
- Saves feature table ready for the LLM agent

Output: data/features/patient_features.csv

Run: python databricks/03_feature_engineering.py
"""

import os
import pandas as pd
import numpy as np

DATA_DIR    = "data/filtered"
OUTPUT_DIR  = "data/features"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── THRESHOLDS (Sepsis-3 / SOFA clinical reference) ──────────────────────────

SEPSIS_THRESHOLDS = {
    "heart_rate":  {"warn": 90,  "critical": 120},
    "map":         {"warn": 70,  "critical": 65},   # below = bad
    "spo2":        {"warn": 94,  "critical": 90},   # below = bad
    "resp_rate":   {"warn": 22,  "critical": 28},
    "lactate":     {"warn": 2.0, "critical": 4.0},
    "wbc":         {"warn": 12,  "critical": 18},
    "creatinine":  {"warn": 1.5, "critical": 2.0},
    "platelets":   {"warn": 100, "critical": 50},   # below = bad
}

VITALS_LABEL_MAP = {
    220045: "heart_rate",
    220052: "map",
    220277: "spo2",
    220210: "resp_rate",
}

LAB_LABEL_MAP = {
    50813: "lactate",
    50912: "creatinine",
    51301: "wbc",
    51265: "platelets",
    50885: "bilirubin",
}

# ── 1. LOAD DATA ───────────────────────────────────────────────────────────────

print("=" * 60)
print("SEPSIS RISK DASHBOARD — FEATURE ENGINEERING")
print("=" * 60)

print("\n[1/5] Loading filtered data...")

vitals   = pd.read_csv(os.path.join(DATA_DIR, "vitals_filtered.csv"))
labs     = pd.read_csv(os.path.join(DATA_DIR, "labs_filtered.csv"))
icustays = pd.read_csv(os.path.join(DATA_DIR, "icustays_filtered.csv"))
patients = pd.read_csv(os.path.join(DATA_DIR, "patients_filtered.csv"))

vitals["charttime"] = pd.to_datetime(vitals["charttime"])
labs["charttime"]   = pd.to_datetime(labs["charttime"])
icustays["intime"]  = pd.to_datetime(icustays["intime"])
icustays["outtime"] = pd.to_datetime(icustays["outtime"])

print(f"  Vitals  : {len(vitals):,} rows")
print(f"  Labs    : {len(labs):,} rows")
print(f"  ICU stays: {len(icustays):,} rows")

# ── 2. PIVOT VITALS — one row per (patient, timestamp) ───────────────────────

print("\n[2/5] Computing vitals features per patient window...")

vitals["signal"] = vitals["itemid"].map(VITALS_LABEL_MAP)

# Pivot: each signal becomes its own column
vitals_pivot = vitals.pivot_table(
    index=["subject_id", "stay_id", "charttime"],
    columns="signal",
    values="valuenum",
    aggfunc="mean"
).reset_index()

vitals_pivot.columns.name = None

# Ensure all 4 vitals columns exist even if sparse
for col in ["heart_rate", "map", "spo2", "resp_rate"]:
    if col not in vitals_pivot.columns:
        vitals_pivot[col] = np.nan

vitals_pivot = vitals_pivot.sort_values(["subject_id", "charttime"])

# ── 3. ROLLING FEATURES — 1-hour windows per patient ─────────────────────────

print("\n[3/5] Computing rolling 1-hour aggregations...")

def rolling_features_for_patient(df):
    """Compute rolling 1-hour stats for a single patient's vitals."""
    df = df.set_index("charttime").sort_index()
    results = []

    for signal in ["heart_rate", "map", "spo2", "resp_rate"]:
        if signal not in df.columns:
            continue
        series = df[signal].dropna()
        if series.empty:
            continue
        rolled = series.rolling("1h", min_periods=1)
        df[f"{signal}_1h_mean"] = rolled.mean()
        df[f"{signal}_1h_min"]  = rolled.min()
        df[f"{signal}_1h_max"]  = rolled.max()

    # Lactate-style trend: compare now vs 3 hours ago (using shift on time-indexed series)
    if "heart_rate" in df.columns:
        df["heart_rate_3h_ago"] = (
            df["heart_rate"]
            .rolling("3h", min_periods=1)
            .apply(lambda x: x.iloc[0] if len(x) > 0 else np.nan, raw=False)
        )
        df["heart_rate_trend_3h"] = df["heart_rate"] - df["heart_rate_3h_ago"]

    if "map" in df.columns:
        df["map_3h_ago"] = (
            df["map"]
            .rolling("3h", min_periods=1)
            .apply(lambda x: x.iloc[0] if len(x) > 0 else np.nan, raw=False)
        )
        df["map_trend_3h"] = df["map"] - df["map_3h_ago"]

    return df.reset_index()

vitals_featured = (
    vitals_pivot
    .groupby("subject_id", group_keys=False)
    .apply(rolling_features_for_patient)
)

print(f"  Vitals feature rows : {len(vitals_featured):,}")
print(f"  Feature columns     : {[c for c in vitals_featured.columns if '_' in c]}")

# ── 4. LATEST LAB VALUES — forward-fill per patient ──────────────────────────

print("\n[4/5] Computing latest lab values per patient per hour...")

labs["signal"] = labs["itemid"].map(LAB_LABEL_MAP)

labs_pivot = labs.pivot_table(
    index=["subject_id", "charttime"],
    columns="signal",
    values="valuenum",
    aggfunc="mean"
).reset_index()

labs_pivot.columns.name = None

# Ensure all lab columns exist
for col in ["lactate", "creatinine", "wbc", "platelets", "bilirubin"]:
    if col not in labs_pivot.columns:
        labs_pivot[col] = np.nan

labs_pivot = labs_pivot.sort_values(["subject_id", "charttime"])

# Lactate trend: current vs 3 hours ago
def lactate_trend(df):
    df = df.set_index("charttime").sort_index()
    if "lactate" in df.columns:
        df["lactate_3h_ago"] = (
            df["lactate"]
            .rolling("3h", min_periods=1)
            .apply(lambda x: x.iloc[0] if len(x) > 0 else np.nan, raw=False)
        )
        df["lactate_trend_3h"] = df["lactate"] - df["lactate_3h_ago"]
        df["lactate_rising"]   = (df["lactate_trend_3h"] > 0.5).astype(int)
    return df.reset_index()

labs_with_trend = (
    labs_pivot
    .groupby("subject_id", group_keys=False)
    .apply(lactate_trend)
)

# ── 5. JOIN VITALS + LABS — merge-asof per patient ───────────────────────────

print("\n[5/5] Joining vitals features with latest lab values...")

vitals_featured = vitals_featured.sort_values(["subject_id", "charttime"])
labs_with_trend = labs_with_trend.sort_values(["subject_id", "charttime"])

# For each vitals row, carry forward the most recent lab values
feature_rows = []
for patient_id, vdf in vitals_featured.groupby("subject_id"):
    ldf = labs_with_trend[labs_with_trend["subject_id"] == patient_id]
    if ldf.empty:
        merged = vdf.copy()
        for col in ["lactate", "creatinine", "wbc", "platelets", "bilirubin",
                    "lactate_trend_3h", "lactate_rising"]:
            merged[col] = np.nan
    else:
        merged = pd.merge_asof(
            vdf.sort_values("charttime"),
            ldf[["charttime", "lactate", "creatinine", "wbc",
                 "platelets", "bilirubin", "lactate_trend_3h", "lactate_rising"]]
            .sort_values("charttime"),
            on="charttime",
            direction="backward",    # use most recent lab, never future
            tolerance=pd.Timedelta("24h")
        )
    feature_rows.append(merged)

features = pd.concat(feature_rows, ignore_index=True)

# Add patient demographics
features = features.merge(
    patients[["subject_id", "gender", "anchor_age"]],
    on="subject_id", how="left"
)

# ── SOFA PROXY SCORE ─────────────────────────────────────────────────────────
# Simplified SOFA using available signals (0-3 per component)

def sofa_map_score(map_val):
    if pd.isna(map_val):          return 0
    if map_val >= 70:             return 0
    if map_val >= 65:             return 1
    return 2

def sofa_creatinine_score(cr):
    if pd.isna(cr):               return 0
    if cr < 1.2:                  return 0
    if cr < 2.0:                  return 1
    if cr < 3.5:                  return 2
    return 3

def sofa_platelet_score(plt):
    if pd.isna(plt):              return 0
    if plt >= 150:                return 0
    if plt >= 100:                return 1
    if plt >= 50:                 return 2
    return 3

features["sofa_map"]         = features["map"].apply(sofa_map_score)
features["sofa_creatinine"]  = features["creatinine"].apply(sofa_creatinine_score)
features["sofa_platelet"]    = features["platelets"].apply(sofa_platelet_score)
features["sofa_proxy"]       = (
    features["sofa_map"] +
    features["sofa_creatinine"] +
    features["sofa_platelet"]
)

# ── SAVE ──────────────────────────────────────────────────────────────────────

output_path = os.path.join(OUTPUT_DIR, "patient_features.csv")
features.to_csv(output_path, index=False)

print(f"\n  Feature table saved: {output_path}")
print(f"  Rows    : {len(features):,}")
print(f"  Columns : {len(features.columns)}")
print(f"  Patients: {features['subject_id'].nunique()}")

print("\n  Key feature columns:")
key_cols = [
    "heart_rate_1h_mean", "map_1h_mean", "spo2_1h_min", "resp_rate_1h_mean",
    "map_trend_3h", "heart_rate_trend_3h",
    "lactate", "lactate_trend_3h", "lactate_rising",
    "creatinine", "wbc", "platelets",
    "sofa_proxy"
]
for col in key_cols:
    if col in features.columns:
        non_null = features[col].notna().sum()
        print(f"    {col:<30} : {non_null:>6,} non-null values")

# Sample — show one patient's feature timeline
sample_id = features["subject_id"].iloc[0]
sample = features[features["subject_id"] == sample_id].sort_values("charttime")
print(f"\n  Sample patient {sample_id} — last 5 rows:")
display_cols = ["charttime", "heart_rate", "map", "spo2",
                "lactate", "sofa_proxy"]
display_cols = [c for c in display_cols if c in sample.columns]
print(sample[display_cols].tail(5).to_string(index=False))

print("\n" + "=" * 60)
print("FEATURE ENGINEERING COMPLETE")
print("=" * 60)
print("""
Next step: 04_llm_agent.py
  - Reads patient_features.csv
  - Builds combined context per patient
  - Calls Claude API for sepsis risk assessment
  - Routes by confidence level
""")
