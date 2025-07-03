# backend/workers/kpi_etl.py
from backend.services.warehouse import fetch_df
from backend.core.database import AsyncSessionLocal
from backend.models.company import Company
from backend.models.dto import KPITile
import json, datetime as dt

@celery_app.task
def ingest_kpis(company_id: str):
    db = AsyncSessionLocal()
    company = db.get(Company, company_id)
    df = asyncio.run(fetch_df(company.snowflake_dsn, """
        SELECT label, value, delta_pct, spark_json
        FROM analytics.current_kpi_snapshot   -- your view
    """))

    # push into Postgres
    now = dt.datetime.utcnow()
    for _, row in df.iterrows():
        db.execute(
            "INSERT INTO kpi_snapshot (captured_at,label,value,delta_pct,spark,owner_id)"
            " VALUES (:ts,:label,:val,:pct,:spark,:oid)",
            {
              "ts": now, "label": row.label, "val": row.value,
              "pct": row.delta_pct, "spark": json.dumps(row.spark_json),
              "oid": company_id,
            },
        )
    db.commit()
