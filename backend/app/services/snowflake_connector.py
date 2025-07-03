import snowflake.connector, pandas as pd, os
def fetch_table(dsn: str, query: str) -> pd.DataFrame:
    ctx = snowflake.connector.connect(**snowflake.connector.parse_account(dsn))
    try:
        cur = ctx.cursor().execute(query)
        return cur.fetch_pandas_all()
    finally:
        ctx.close()
