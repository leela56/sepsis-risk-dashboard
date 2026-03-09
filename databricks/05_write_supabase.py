"""
05_write_supabase.py
====================
Sepsis Risk Dashboard — Supabase Writer
Author: Leelasaikiran

What this script does:
- Connects to Supabase PostgreSQL using psycopg2
- Creates the `sepsis_alerts` table if it doesn't exist
- Reads the LLM agent decisions from data/agent_output/agent_decisions.csv
- Upserts the latest risk assessment for each patient
- This table acts as the live backend for the Next.js frontend dashboard

Run: python databricks/05_write_supabase.py
"""

import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────────

DB_URL = os.getenv("SUPABASE_DB_URL")
AGENT_OUTPUT_FILE = "data/agent_output/agent_decisions.csv"

# ── 1. CREATE TABLE ───────────────────────────────────────────────────────────

def init_db():
    print("[1/3] Initializing Supabase database...")
    if not DB_URL:
        raise ValueError("Missing SUPABASE_DB_URL in .env")

    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cursor = conn.cursor()

    # Create table for live dashboard
    # Using subject_id as PRIMARY KEY so upserts replace older assessments
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sepsis_alerts (
            subject_id VARCHAR(20) PRIMARY KEY,
            charttime TIMESTAMP,
            risk_level VARCHAR(10),
            confidence NUMERIC,
            routing_bucket VARCHAR(20),
            primary_concern TEXT,
            reasoning TEXT,
            recommended_action TEXT,
            model VARCHAR(50),
            assessed_at TIMESTAMP,
            
            -- Context stats for frontend display
            heart_rate_mean NUMERIC,
            map_mean NUMERIC,
            spo2_min NUMERIC,
            lactate NUMERIC,
            sofa_proxy INTEGER
        );
    """)

    print("  Table `sepsis_alerts` is ready.")
    cursor.close()
    conn.close()

# ── 2. LOAD DATA ──────────────────────────────────────────────────────────────

def load_data() -> pd.DataFrame:
    print("\n[2/3] Loading agent decisions...")
    if not os.path.exists(AGENT_OUTPUT_FILE):
        raise FileNotFoundError(f"Agent output not found: {AGENT_OUTPUT_FILE}. Run 04_llm_agent.py first.")
    
    df = pd.read_csv(AGENT_OUTPUT_FILE)
    
    # Fill NaNs with None so psycopg2 can insert them as SQL NULLs
    df = df.replace({pd.NA: None, float("nan"): None})
    
    print(f"  Loaded {len(df)} patient assessments.")
    return df

# ── 3. UPSERT TO SUPABASE ─────────────────────────────────────────────────────

def upsert_decisions(df: pd.DataFrame):
    print("\n[3/3] Upserting to Supabase PostgreSQL...")
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cursor = conn.cursor()

    # Define columns exactly matching the DataFrame and Table
    columns = [
        "subject_id", "charttime", "risk_level", "confidence", "routing_bucket",
        "primary_concern", "reasoning", "recommended_action", "model", "assessed_at",
        "heart_rate_mean", "map_mean", "spo2_min", "lactate", "sofa_proxy"
    ]
    
    # Convert DataFrame to list of tuples for execute_values
    values = [tuple(row[c] for c in columns) for _, row in df.iterrows()]

    query = f"""
        INSERT INTO sepsis_alerts ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (subject_id) DO UPDATE SET
            charttime = EXCLUDED.charttime,
            risk_level = EXCLUDED.risk_level,
            confidence = EXCLUDED.confidence,
            routing_bucket = EXCLUDED.routing_bucket,
            primary_concern = EXCLUDED.primary_concern,
            reasoning = EXCLUDED.reasoning,
            recommended_action = EXCLUDED.recommended_action,
            model = EXCLUDED.model,
            assessed_at = EXCLUDED.assessed_at,
            heart_rate_mean = EXCLUDED.heart_rate_mean,
            map_mean = EXCLUDED.map_mean,
            spo2_min = EXCLUDED.spo2_min,
            lactate = EXCLUDED.lactate,
            sofa_proxy = EXCLUDED.sofa_proxy;
    """

    execute_values(cursor, query, values)

    print(f"  Successfully upserted {len(values)} rows to `sepsis_alerts`.")
    
    # Verify counts
    cursor.execute("SELECT risk_level, COUNT(*) FROM sepsis_alerts GROUP BY risk_level;")
    counts = cursor.fetchall()
    
    print("\n  Live Dashboard DB Status:")
    for risk, count in sorted(counts, key=lambda x: x[1], reverse=True):
        icon = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(risk, "⚪")
        print(f"    {icon} {risk:<6}: {count:>3} patients")

    cursor.close()
    conn.close()

# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("SEPSIS RISK DASHBOARD — SUPABASE WRITER")
    print("=" * 60)
    
    try:
        init_db()
        df = load_data()
        upsert_decisions(df)
        
        print("\n" + "=" * 60)
        print("DATABASE UPDATE COMPLETE")
        print("=" * 60)
        print("""
Next steps:
  1. Write FASTAPI backend (`backend/main.py`) to serve this data
  2. Build NEXT.JS frontend (`frontend/`) to visualize it
""")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
