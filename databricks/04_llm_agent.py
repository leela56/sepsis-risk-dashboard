"""
04_llm_agent.py
================
Sepsis Risk Dashboard — LLM Agent (Claude API)
Author: Leelasaikiran

What this script does:
- Reads patient_features.csv from data/features/
- For each patient, takes their LATEST feature snapshot
- Builds a structured clinical context prompt
- Calls Claude API (claude-3-5-haiku for speed/cost, claude-sonnet for accuracy)
- Routes by confidence: HIGH → immediate_alerts, MEDIUM → review_queue, LOW → audit_log
- Saves all decisions to data/agent_output/

Run: python databricks/04_llm_agent.py
"""

import os
import json
import time
import pandas as pd
import numpy as np
import anthropic
from dotenv import load_dotenv

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────────

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
MODEL = "claude-sonnet-4-5"  # confirmed working; swap to claude-opus-4-5 for higher accuracy

DATA_DIR   = "data/features"
OUTPUT_DIR = "data/agent_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

PROMPT_VERSION = "v1.0"

# Confidence routing thresholds
HIGH_THRESHOLD   = 0.75   # auto-alert
MEDIUM_THRESHOLD = 0.45   # queue for review

# ── LOAD PROMPTS ──────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a critical care AI assistant specializing in early sepsis detection.
You will receive a patient's current clinical snapshot including vitals trends, lab values, and computed scores.

Your task: Assess this patient's sepsis risk.

Key clinical thresholds to apply:
- MAP < 65 mmHg = septic shock range
- Lactate > 2.0 mmol/L = elevated; > 4.0 = critical
- SOFA score increase of 2+ from baseline = sepsis criterion
- Rising lactate over 3 hours is more concerning than a single reading
- Tachycardia (HR > 90) + hypotension (MAP < 70) + RR > 22 = SIRS criteria met

Always return ONLY valid JSON in this exact format:
{
  "risk_level": "HIGH" | "MEDIUM" | "LOW",
  "confidence": <float 0.0 to 1.0>,
  "primary_concern": "<one sentence — the most alarming finding>",
  "reasoning": "<2-3 sentences — clinical reasoning connecting the signals>",
  "recommended_action": "<one concrete clinical action>"
}"""

def build_patient_prompt(row: pd.Series) -> str:
    """Build a structured clinical context from a patient's latest feature row."""

    def fmt(val, decimals=1, suffix=""):
        if pd.isna(val):
            return "not available"
        return f"{val:.{decimals}f}{suffix}"

    def trend_arrow(val):
        if pd.isna(val): return ""
        return " ↑" if val > 0.3 else (" ↓" if val < -0.3 else " →")

    prompt = f"""PATIENT CLINICAL SNAPSHOT
========================
Patient ID : {row['subject_id']}
Age        : {fmt(row.get('anchor_age'), 0)} years
Gender     : {row.get('gender', 'Unknown')}
Time       : {row['charttime']}

CURRENT VITALS (1-hour window stats)
  Heart Rate   : {fmt(row.get('heart_rate_1h_mean'))} bpm (min {fmt(row.get('heart_rate_1h_min'))}, max {fmt(row.get('heart_rate_1h_max'))})
                 3h trend: {fmt(row.get('heart_rate_trend_3h'), 1, ' bpm')}{trend_arrow(row.get('heart_rate_trend_3h'))}
  MAP          : {fmt(row.get('map_1h_mean'))} mmHg
                 3h trend: {fmt(row.get('map_trend_3h'), 1, ' mmHg')}{trend_arrow(row.get('map_trend_3h'))}
  SpO2         : {fmt(row.get('spo2_1h_min'))} % (1h minimum)
  Resp Rate    : {fmt(row.get('resp_rate_1h_mean'))} breaths/min

MOST RECENT LAB VALUES
  Lactate      : {fmt(row.get('lactate'), 2)} mmol/L
                 3h trend: {fmt(row.get('lactate_trend_3h'), 2, ' mmol/L')}{trend_arrow(row.get('lactate_trend_3h'))}
                 Rising?  : {"YES ⚠️" if row.get('lactate_rising') == 1 else "No"}
  Creatinine   : {fmt(row.get('creatinine'), 2)} mg/dL
  WBC          : {fmt(row.get('wbc'), 1)} K/uL
  Platelets    : {fmt(row.get('platelets'), 0)} K/uL
  Bilirubin    : {fmt(row.get('bilirubin'), 2)} mg/dL

COMPUTED SCORES
  SOFA Proxy   : {fmt(row.get('sofa_proxy'), 0)} / 9
    MAP component        : {fmt(row.get('sofa_map'), 0)}
    Creatinine component : {fmt(row.get('sofa_creatinine'), 0)}
    Platelet component   : {fmt(row.get('sofa_platelet'), 0)}

Assess this patient's sepsis risk. Return JSON only."""

    return prompt

# ── AGENT LOOP ────────────────────────────────────────────────────────────────

print("=" * 60)
print("SEPSIS RISK DASHBOARD — LLM AGENT")
print("=" * 60)
print(f"\nModel         : {MODEL}")
print(f"Prompt version: {PROMPT_VERSION}")
print(f"Routing thresholds: HIGH ≥ {HIGH_THRESHOLD} | MEDIUM ≥ {MEDIUM_THRESHOLD}\n")

print("[1/3] Loading feature table...")
features = pd.read_csv(os.path.join(DATA_DIR, "patient_features.csv"))
features["charttime"] = pd.to_datetime(features["charttime"])

# Take the LATEST feature snapshot per patient
# (in real streaming this is handled by watermarking)
latest = (
    features
    .sort_values("charttime")
    .groupby("subject_id")
    .last()
    .reset_index()
)

print(f"  Patients to assess: {len(latest)}")

client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

print(f"\n[2/3] Running agent over {len(latest)} patients...\n")
print("-" * 60)

results = []
high_alerts  = 0
medium_queue = 0
low_logs     = 0

for i, (_, row) in enumerate(latest.iterrows()):
    patient_id = row["subject_id"]
    prompt     = build_patient_prompt(row)

    try:
        message = client.messages.create(
            model=MODEL,
            max_tokens=512,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}]
        )

        raw_response = message.content[0].text.strip()

        # Parse JSON response
        # Claude sometimes wraps in ```json ... ```
        if "```" in raw_response:
            raw_response = raw_response.split("```")[1]
            if raw_response.startswith("json"):
                raw_response = raw_response[4:]

        decision = json.loads(raw_response)
        risk_level      = decision.get("risk_level", "UNKNOWN")
        confidence      = float(decision.get("confidence", 0.0))
        primary_concern = decision.get("primary_concern", "")
        reasoning       = decision.get("reasoning", "")
        recommended     = decision.get("recommended_action", "")

    except json.JSONDecodeError as e:
        risk_level = "UNKNOWN"
        confidence = 0.0
        primary_concern = "JSON parse error"
        reasoning = f"Raw: {raw_response[:200]}"
        recommended = "Manual review required"

    except Exception as e:
        risk_level = "ERROR"
        confidence = 0.0
        primary_concern = str(e)
        reasoning = ""
        recommended = ""

    # Route by confidence
    if confidence >= HIGH_THRESHOLD:
        bucket = "immediate_alert"
        high_alerts += 1
    elif confidence >= MEDIUM_THRESHOLD:
        bucket = "review_queue"
        medium_queue += 1
    else:
        bucket = "audit_log"
        low_logs += 1

    # Progress print
    risk_icon = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(risk_level, "⚪")
    print(f"  [{i+1:>3}/{len(latest)}] Patient {patient_id} "
          f"{risk_icon} {risk_level:<6} conf={confidence:.2f} → {bucket}")
    if primary_concern:
        print(f"           ↳ {primary_concern[:80]}")

    results.append({
        "subject_id":      patient_id,
        "charttime":       row["charttime"],
        "risk_level":      risk_level,
        "confidence":      confidence,
        "routing_bucket":  bucket,
        "primary_concern": primary_concern,
        "reasoning":       reasoning,
        "recommended_action": recommended,
        "prompt_version":  PROMPT_VERSION,
        "model":           MODEL,
        "assessed_at":     pd.Timestamp.now().isoformat(),
        # Key features at time of assessment for audit
        "heart_rate_mean": row.get("heart_rate_1h_mean"),
        "map_mean":        row.get("map_1h_mean"),
        "spo2_min":        row.get("spo2_1h_min"),
        "lactate":         row.get("lactate"),
        "lactate_rising":  row.get("lactate_rising"),
        "sofa_proxy":      row.get("sofa_proxy"),
    })

    # Be polite to the API — small delay between calls
    time.sleep(0.3)

# ── SAVE OUTPUT ───────────────────────────────────────────────────────────────

print("\n" + "-" * 60)
print("\n[3/3] Saving agent decisions...")

decisions = pd.DataFrame(results)
decisions.to_csv(os.path.join(OUTPUT_DIR, "agent_decisions.csv"), index=False)

# Split by routing bucket
decisions[decisions["routing_bucket"] == "immediate_alert"].to_csv(
    os.path.join(OUTPUT_DIR, "immediate_alerts.csv"), index=False
)
decisions[decisions["routing_bucket"] == "review_queue"].to_csv(
    os.path.join(OUTPUT_DIR, "review_queue.csv"), index=False
)

print(f"\n  Saved to: {OUTPUT_DIR}/")
print(f"    agent_decisions.csv  : {len(decisions)} total assessments")
print(f"    immediate_alerts.csv : {high_alerts}  patients")
print(f"    review_queue.csv     : {medium_queue} patients")
print(f"    audit (logged only)  : {low_logs}  patients")

print("\n" + "=" * 60)
print("AGENT ASSESSMENT COMPLETE")
print("=" * 60)

# Summary
print("\n  ROUTING SUMMARY:")
print(f"    🔴 HIGH  — immediate alert : {high_alerts:>3} patients")
print(f"    🟡 MEDIUM — review queue   : {medium_queue:>3} patients")
print(f"    🟢 LOW   — logged only     : {low_logs:>3} patients")

print("\n  TOP HIGH-RISK PATIENTS:")
high_df = decisions[decisions["risk_level"] == "HIGH"].sort_values("confidence", ascending=False)
if not high_df.empty:
    for _, r in high_df.head(5).iterrows():
        print(f"    Patient {r['subject_id']} | conf={r['confidence']:.2f}")
        print(f"      {r['primary_concern'][:80]}")
else:
    print("    None flagged HIGH risk in this batch.")

print("""
Next step: 05_write_supabase.py
  - Pushes agent decisions to Supabase PostgreSQL
  - Creates sepsis_alerts and audit_log tables
  - Enables the FastAPI backend to serve live results
""")
