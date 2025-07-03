import pandas as pd, sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from functools import cache

@cache   # one engine per DSN
def get_async_engine(dsn: str):
    # SQLAlchemy 2.0 style async; driver must support asyncio
    return create_async_engine(dsn, pool_pre_ping=True)

async def fetch_df(dsn: str, query: str) -> pd.DataFrame:
    eng = get_async_engine(dsn)
    async with eng.connect() as conn:
        df = await conn.run_sync(lambda c: pd.read_sql(query, c))
    return df
