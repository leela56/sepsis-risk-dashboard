from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Sepsis Risk Dashboard API")

# Allow standard frontend ports for local dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_URL = os.getenv("SUPABASE_DB_URL")

if not DB_URL:
    raise ValueError("Missing SUPABASE_DB_URL in environment variables.")

def get_db_connection():
    conn = psycopg2.connect(DB_URL)
    return conn

@app.get("/")
def read_root():
    return {"status": "ok", "message": "Sepsis Risk Dashboard API is running"}

@app.get("/api/patients")
def get_patients():
    """
    Fetch all patient risk assessments from Postgres.
    Ordered by risk level (HIGH > MEDIUM > LOW) and then confidence.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # SQL sorting case for risk buckets
        query = """
            SELECT * FROM sepsis_alerts
            ORDER BY 
                CASE risk_level
                    WHEN 'HIGH' THEN 1
                    WHEN 'MEDIUM' THEN 2
                    WHEN 'LOW' THEN 3
                    ELSE 4
                END ASC,
                confidence DESC
        """
        cursor.execute(query)
        data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return {
            "count": len(data),
            "data": data
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/patients/{subject_id}")
def get_patient(subject_id: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("SELECT * FROM sepsis_alerts WHERE subject_id = %s", (subject_id,))
        data = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if not data:
            raise HTTPException(status_code=404, detail=f"Patient {subject_id} not found")
            
        return data
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/stats")
def get_stats():
    """
    Return high-level statistics for the dashboard.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute("""
            SELECT risk_level, COUNT(*) as cnt 
            FROM sepsis_alerts 
            GROUP BY risk_level
        """)
        data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        stats = {
            "total_patients": 0,
            "high_risk": 0,
            "medium_risk": 0,
            "low_risk": 0
        }
        
        for row in data:
            risk = row["risk_level"]
            count = row["cnt"]
            stats["total_patients"] += count
            
            if risk == "HIGH": stats["high_risk"] = count
            elif risk == "MEDIUM": stats["medium_risk"] = count
            elif risk == "LOW": stats["low_risk"] = count
            
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    print("Starting Sepsis Dashboard API on http://localhost:8000")
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
