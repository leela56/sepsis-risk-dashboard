"""
02_stream_simulator.py
======================
Sepsis Risk Dashboard — Kafka Stream Simulator
Author: Leelasaikiran

What this script does:
- Reads the 4 filtered MIMIC-IV CSVs from data/filtered/
- Replays vitals and lab events chronologically into Confluent Kafka
- Simulates 2 live streams: vitals-topic and labs-topic
- Respects real-time ordering: events are sent in charttime order
- REPLAY_SPEED controls how fast events are replayed

Run: python databricks/02_stream_simulator.py

Kafka topics produced:
  vitals-topic  → heart rate, MAP, SpO2, resp rate
  labs-topic    → lactate, creatinine, WBC, platelets, bilirubin
"""

import os
import json
import time
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

# ── CONFIG ───────────────────────────────────────────────────────────────────

BOOTSTRAP_SERVERS = os.getenv("CONFLUENT_BOOTSTRAP_SERVERS")
API_KEY           = os.getenv("CONFLUENT_API_KEY")
API_SECRET        = os.getenv("CONFLUENT_API_SECRET")

VITALS_TOPIC = "vitals-topic"
LABS_TOPIC   = "labs-topic"

# How many times faster than real time to replay
# 600 = 1 real ICU hour plays back in 6 seconds
# 60  = 1 real ICU hour plays back in 60 seconds (slower, easier to observe)
REPLAY_SPEED = 600

DATA_DIR = "data/filtered"

# ── KAFKA PRODUCER ────────────────────────────────────────────────────────────

producer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism":    "PLAIN",
    "sasl.username":     API_KEY,
    "sasl.password":     API_SECRET,
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    """Called once per message to confirm delivery or report failure."""
    if err:
        print(f"  ❌ Delivery failed: {err}")

def send_event(topic, event_dict, key=None):
    """Serialize a dict to JSON and send to a Kafka topic."""
    # Convert any non-serializable types
    for k, v in event_dict.items():
        if pd.isna(v):
            event_dict[k] = None
        elif hasattr(v, "isoformat"):          # datetime → string
            event_dict[k] = v.isoformat()
        elif hasattr(v, "item"):               # numpy types → Python native
            event_dict[k] = v.item()

    producer.produce(
        topic=topic,
        key=str(key) if key else None,
        value=json.dumps(event_dict),
        callback=delivery_report,
    )
    producer.poll(0)  # trigger callbacks without blocking

# ── LOAD FILTERED DATA ────────────────────────────────────────────────────────

print("=" * 60)
print("SEPSIS RISK DASHBOARD — STREAM SIMULATOR")
print("=" * 60)
print(f"\nReplay speed: {REPLAY_SPEED}x real time")
print(f"  → 1 ICU hour plays in {3600 / REPLAY_SPEED:.0f} seconds\n")

print("[1/4] Loading filtered datasets...")

vitals   = pd.read_csv(os.path.join(DATA_DIR, "vitals_filtered.csv"))
labs     = pd.read_csv(os.path.join(DATA_DIR, "labs_filtered.csv"))
icustays = pd.read_csv(os.path.join(DATA_DIR, "icustays_filtered.csv"))
patients = pd.read_csv(os.path.join(DATA_DIR, "patients_filtered.csv"))

vitals["charttime"] = pd.to_datetime(vitals["charttime"])
labs["charttime"]   = pd.to_datetime(labs["charttime"])

print(f"  Vitals loaded : {len(vitals):,} rows")
print(f"  Labs loaded   : {len(labs):,} rows")

# ── MERGE AND SORT ALL EVENTS CHRONOLOGICALLY ─────────────────────────────────

print("\n[2/4] Merging and sorting all events by time...")

# Tag each event with which topic it belongs to
vitals_tagged = vitals.copy()
vitals_tagged["_topic"] = VITALS_TOPIC

labs_tagged = labs.copy()
labs_tagged["_topic"] = LABS_TOPIC

# Align columns — labs don't have stay_id
labs_tagged["stay_id"] = None

# Combine and sort chronologically
all_events = pd.concat([vitals_tagged, labs_tagged], ignore_index=True)
all_events = all_events.sort_values("charttime").reset_index(drop=True)

print(f"  Total events to replay: {len(all_events):,}")
print(f"  Time range: {all_events['charttime'].min()} → {all_events['charttime'].max()}")

# Enrich with patient demographics
all_events = all_events.merge(
    patients[["subject_id", "gender", "anchor_age"]],
    on="subject_id",
    how="left"
)

# ── REPLAY LOOP ───────────────────────────────────────────────────────────────

print("\n[3/4] Starting stream replay into Kafka...\n")
print(f"  vitals → {VITALS_TOPIC}")
print(f"  labs   → {LABS_TOPIC}")
print("\n  Press Ctrl+C to stop.\n")
print("-" * 60)

prev_event_time = None
events_sent     = 0
start_wall_time = time.time()

try:
    for _, row in all_events.iterrows():
        event_time = row["charttime"]

        # Calculate the real-time delay between events and scale by REPLAY_SPEED
        if prev_event_time is not None:
            real_gap_seconds = (event_time - prev_event_time).total_seconds()
            sleep_seconds    = real_gap_seconds / REPLAY_SPEED
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        prev_event_time = event_time
        topic = row["_topic"]

        event = {
            "subject_id":  row["subject_id"],
            "hadm_id":     row.get("hadm_id"),
            "stay_id":     row.get("stay_id"),
            "charttime":   event_time,
            "itemid":      row["itemid"],
            "label":       row["label"],
            "valuenum":    row["valuenum"],
            "gender":      row.get("gender"),
            "anchor_age":  row.get("anchor_age"),
        }

        send_event(topic, event, key=row["subject_id"])
        events_sent += 1

        # Print progress every 500 events
        if events_sent % 500 == 0:
            elapsed   = time.time() - start_wall_time
            sim_hours = (event_time - all_events["charttime"].iloc[0]).total_seconds() / 3600
            print(
                f"  [{events_sent:>7,} events] "
                f"sim time: {sim_hours:>7.1f}h | "
                f"wall time: {elapsed:>6.1f}s | "
                f"patient: {row['subject_id']}"
            )

except KeyboardInterrupt:
    print("\n\n  Simulator stopped by user.")

finally:
    # Flush all pending messages before exit
    print("\n[4/4] Flushing remaining messages to Kafka...")
    producer.flush()
    elapsed = time.time() - start_wall_time
    print(f"\n  Done. {events_sent:,} events sent in {elapsed:.1f} seconds.")
    print(f"  Topics: {VITALS_TOPIC}, {LABS_TOPIC}")
    print(f"\nNext step: 03_feature_engineering.py")
