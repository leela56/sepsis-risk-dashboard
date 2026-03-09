"""
01_data_exploration.py
======================
Sepsis Risk Dashboard — Data Exploration & Filtering
Author: Leelasaikiran

What this script does:
- Loads raw MIMIC-IV Demo CSV files
- Filters down to only the signals needed for sepsis detection
- Validates data quality (coverage, time overlap, frequency)
- Saves filtered datasets ready for the streaming pipeline
- Prints a full data quality summary

Run this locally before uploading anything to Databricks.
"""

import pandas as pd
import os

# ── 0. CONFIG ────────────────────────────────────────────────────────────────

# Paths — adjust if your folder structure is different
DATA_DIR    = "data"
HOSP_DIR    = os.path.join(DATA_DIR, "hosp")
ICU_DIR     = os.path.join(DATA_DIR, "icu")
OUTPUT_DIR  = os.path.join(DATA_DIR, "filtered")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── 1. ITEM IDs ───────────────────────────────────────────────────────────────
# MIMIC uses numeric codes instead of readable names.
# These are the confirmed item IDs for our sepsis signals.

# Vitals (from chartevents.csv)
ITEM_HEART_RATE  = 220045   # bpm          — normal: 60-100
ITEM_MAP         = 220052   # mmHg         — sepsis concern: <65
ITEM_SPO2        = 220277   # %            — sepsis concern: <94
ITEM_RESP_RATE   = 220210   # insp/min     — sepsis concern: >22

# Labs (from labevents.csv)
ITEM_LACTATE     = 50813    # mmol/L       — sepsis concern: >2.0, critical: >4.0
ITEM_CREATININE  = 50912    # mg/dL        — organ function indicator
ITEM_WBC         = 51301    # K/uL         — infection marker
ITEM_PLATELETS   = 51265    # K/uL         — SOFA score component
ITEM_BILIRUBIN   = 50885    # mg/dL        — SOFA score component (liver)

VITALS_IDS = [ITEM_HEART_RATE, ITEM_MAP, ITEM_SPO2, ITEM_RESP_RATE]
LAB_IDS    = [ITEM_LACTATE, ITEM_CREATININE, ITEM_WBC, ITEM_PLATELETS, ITEM_BILIRUBIN]

# Human readable labels for reporting
VITALS_LABELS = {
    ITEM_HEART_RATE : "Heart Rate",
    ITEM_MAP        : "MAP",
    ITEM_SPO2       : "SpO2",
    ITEM_RESP_RATE  : "Resp Rate"
}

LAB_LABELS = {
    ITEM_LACTATE    : "Lactate",
    ITEM_CREATININE : "Creatinine",
    ITEM_WBC        : "WBC",
    ITEM_PLATELETS  : "Platelets",
    ITEM_BILIRUBIN  : "Bilirubin"
}

# ── 2. LOAD RAW FILES ─────────────────────────────────────────────────────────

print("=" * 60)
print("SEPSIS RISK DASHBOARD — DATA EXPLORATION")
print("=" * 60)

print("\n[1/6] Loading raw MIMIC-IV Demo files...")

patients    = pd.read_csv(os.path.join(HOSP_DIR, "patients.csv"))
labevents   = pd.read_csv(os.path.join(HOSP_DIR, "labevents.csv"))
chartevents = pd.read_csv(os.path.join(ICU_DIR,  "chartevents.csv"))
icustays    = pd.read_csv(os.path.join(ICU_DIR,  "icustays.csv"))

print(f"  patients.csv    : {len(patients):>10,} rows")
print(f"  labevents.csv   : {len(labevents):>10,} rows")
print(f"  chartevents.csv : {len(chartevents):>10,} rows")
print(f"  icustays.csv    : {len(icustays):>10,} rows")

# ── 3. FILTER TO SEPSIS SIGNALS ONLY ─────────────────────────────────────────

print("\n[2/6] Filtering to sepsis-relevant signals only...")

vitals = chartevents[chartevents["itemid"].isin(VITALS_IDS)].copy()
labs   = labevents[labevents["itemid"].isin(LAB_IDS)].copy()

vitals["label"] = vitals["itemid"].map(VITALS_LABELS)
labs["label"]   = labs["itemid"].map(LAB_LABELS)

print(f"  Vitals rows kept : {len(vitals):>8,} (from {len(chartevents):,})")
print(f"  Labs rows kept   : {len(labs):>8,} (from {len(labevents):,})")
print(f"  Rows discarded   : {len(chartevents) + len(labevents) - len(vitals) - len(labs):>8,}")

# ── 4. PATIENT COVERAGE CHECK ─────────────────────────────────────────────────

print("\n[3/6] Checking patient coverage...")

patients_with_vitals = set(vitals["subject_id"].unique())
patients_with_labs   = set(labs["subject_id"].unique())
patients_with_both   = patients_with_vitals & patients_with_labs

print(f"  Total patients        : {len(patients)}")
print(f"  With vitals           : {len(patients_with_vitals)} ({len(patients_with_vitals)/len(patients)*100:.0f}%)")
print(f"  With labs             : {len(patients_with_labs)} ({len(patients_with_labs)/len(patients)*100:.0f}%)")
print(f"  With BOTH ✅          : {len(patients_with_both)} ({len(patients_with_both)/len(patients)*100:.0f}%)")

# ── 5. SIGNAL BREAKDOWN ───────────────────────────────────────────────────────

print("\n[4/6] Signal breakdown...")

print("\n  VITALS:")
vitals_breakdown = vitals.groupby("label")["subject_id"].count().sort_values(ascending=False)
for label, count in vitals_breakdown.items():
    print(f"    {label:<15} : {count:>7,} rows")

print("\n  LABS:")
labs_breakdown = labs.groupby("label")["subject_id"].count().sort_values(ascending=False)
for label, count in labs_breakdown.items():
    print(f"    {label:<15} : {count:>7,} rows")

# ── 6. TIMELINE & FREQUENCY CHECK ────────────────────────────────────────────

print("\n[5/6] Checking timeline and reading frequency...")

vitals["charttime"] = pd.to_datetime(vitals["charttime"])
labs["charttime"]   = pd.to_datetime(labs["charttime"])
icustays["intime"]  = pd.to_datetime(icustays["intime"])
icustays["outtime"] = pd.to_datetime(icustays["outtime"])

icustays["los_hours"] = (
    icustays["outtime"] - icustays["intime"]
).dt.total_seconds() / 3600

print(f"\n  ICU Stay Duration:")
print(f"    Average : {icustays['los_hours'].mean():.1f} hours")
print(f"    Shortest: {icustays['los_hours'].min():.1f} hours")
print(f"    Longest : {icustays['los_hours'].max():.1f} hours")

vitals_per_patient = vitals.groupby("subject_id").agg(
    total_readings=("charttime", "count"),
    first_reading =("charttime", "min"),
    last_reading  =("charttime", "max")
).reset_index()

vitals_per_patient["span_hours"] = (
    vitals_per_patient["last_reading"] -
    vitals_per_patient["first_reading"]
).dt.total_seconds() / 3600

vitals_per_patient["readings_per_hour"] = (
    vitals_per_patient["total_readings"] /
    vitals_per_patient["span_hours"].replace(0, float("nan"))
)

print(f"\n  Vitals Frequency:")
print(f"    Avg readings per patient : {vitals_per_patient['total_readings'].mean():.0f}")
print(f"    Avg readings per hour    : {vitals_per_patient['readings_per_hour'].mean():.2f}")

# ── 7. SAVE FILTERED DATASETS ─────────────────────────────────────────────────

print("\n[6/6] Saving filtered datasets...")

# Keep only the columns the pipeline needs
vitals_out = vitals[[
    "subject_id", "hadm_id", "stay_id",
    "charttime", "itemid", "label", "valuenum"
]].dropna(subset=["valuenum"])

labs_out = labs[[
    "subject_id", "hadm_id",
    "charttime", "itemid", "label", "valuenum"
]].dropna(subset=["valuenum"])

icustays_out = icustays[[
    "subject_id", "hadm_id", "stay_id",
    "first_careunit", "intime", "outtime", "los_hours"
]]

patients_out = patients[[
    "subject_id", "gender", "anchor_age"
]]

# Save
vitals_out.to_csv(os.path.join(OUTPUT_DIR, "vitals_filtered.csv"),   index=False)
labs_out.to_csv(os.path.join(OUTPUT_DIR,   "labs_filtered.csv"),     index=False)
icustays_out.to_csv(os.path.join(OUTPUT_DIR, "icustays_filtered.csv"), index=False)
patients_out.to_csv(os.path.join(OUTPUT_DIR, "patients_filtered.csv"), index=False)

print(f"  Saved to: {OUTPUT_DIR}/")
print(f"    vitals_filtered.csv   : {len(vitals_out):,} rows")
print(f"    labs_filtered.csv     : {len(labs_out):,} rows")
print(f"    icustays_filtered.csv : {len(icustays_out):,} rows")
print(f"    patients_filtered.csv : {len(patients_out):,} rows")

# ── 8. FINAL SUMMARY ──────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("DATA EXPLORATION COMPLETE")
print("=" * 60)
print("""
Next step: 02_stream_simulator.py
  - Reads these filtered CSVs
  - Replays events chronologically into Kafka
  - Simulates 3 live streams: vitals, labs, notes
""")
