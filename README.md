# Sepsis Watchtower: Agentic AI Risk Dashboard

Hey! Welcome to my end-to-end data engineering and AI project. 

This is a real-time, event-driven streaming pipeline that uses an LLM Agent (Claude 3.5 Sonnet) to monitor live ICU patient data and detect early warning signs of sepsis. Instead of relying on static, noisy alarms that cause "alarm fatigue," this system acts like an intelligent resident doctor—analyzing rolling trends and structured clinical criteria to provide high-quality alerts.

## The Architecture (How it works under the hood)

1. **The Pulse (Data Ingestion)**: Hospital bedside monitors don't send daily CSVs; they stream live data. I built a Python simulator that takes historical MIMIC-IV ICU data and plays it chronologically, second-by-second, into **Confluent Kafka**.
2. **The Brainstem (Feature Engineering)**: A real-time Python script listens to the Kafka topics. It computes 1-hour rolling averages for vitals and 3-hour moving trends for lab results (like Lactate), structuring chaotic signals into clean medical context.
3. **The Agent (AI Assessment)**: **Anthropic's Claude 3.5 Sonnet** takes these rolling snapshops and applies the Sepsis-3 clinical criteria. It uses chain-of-thought to spit out a highly structured JSON response with a distinct `Risk Level` (HIGH/MEDIUM/LOW) and a human-readable `Primary Concern`.
4. **The Memory (Database)**: The agent's structured decisions are instantly upserted into a **Supabase (PostgreSQL)** database.
5. **The Eyes (Dashboard)**: A high-performance **FastAPI** backend serves the live data to a modern **Next.js** React dashboard, giving doctors an instant, color-coded view of the entire ICU.

## Tech Stack
- **Streaming & Engineering**: Confluent Kafka, Python, Pandas
- **AI Agent**: Anthropic Claude 3.5 Sonnet, JSON Structured Outputs
- **Database**: Supabase (PostgreSQL)
- **Backend API**: Python FastAPI, Uvicorn (Deployed on Railway)
- **Frontend UI**: Next.js, React, TailwindCSS, Framer Motion, Lucide (Deployed on Vercel)

## Want to run it locally?

### 1. The Data Pipeline
You'll need `.env` set up with keys for Confluent Kafka, Anthropic, and Supabase.
```bash
# 1. Start the event stream (Terminal 1)
python databricks/02_stream_simulator.py

# 2. Start the rolling feature engine (Terminal 2)
python databricks/03_feature_engineering.py

# 3. Fire up the LLM Agent (Terminal 3)
python databricks/04_llm_agent.py

# 4. Push the live decisions to the database (Terminal 4)
python databricks/05_write_supabase.py
```

### 2. The Dashboard
```bash
# 1. Start the backend API locally
cd backend
python main.py

# 2. Start the frontend Next.js app
cd frontend
npm run dev
```

---
*Built as a portfolio showcase demonstrating the intersection of real-time streaming data engineering and applied Agentic AI.*
