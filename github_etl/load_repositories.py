import pandas as pd
from psycopg2.extras import execute_values

def load_repositories_to_postgres(df: pd.DataFrame, engine):
    """
    Load GitHub repositories into github_source_data.github_repositories
    Schema-aligned and FK-safe.
    """

    if df.empty:
        print("No repositories to load.")
        return

    # Enforce correct Python types
    df = df.copy()
    df["repo_id"] = df["repo_id"].astype(str)
    df["repo_name"] = df["repo_name"].astype(str)
    df["repo_full_name"] = df["repo_full_name"].astype(str)
    df["owner"] = df["owner"].astype(str)
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)

    cols = [
        "repo_id",
        "repo_name",
        "repo_full_name",
        "created_at",
        "owner",
    ]

    values = [tuple(row) for row in df[cols].itertuples(index=False, name=None)]

    insert_sql = """
        INSERT INTO github_source_data.github_repositories
        (repo_id, repo_name, repo_full_name, created_at, owner)
        VALUES %s
        ON CONFLICT (repo_id) DO NOTHING
    """

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            execute_values(
                cur,
                insert_sql,
                values,
                page_size=1000
            )
        raw_conn.commit()
        print(f"✅ Loaded {len(values)} repositories")
    except Exception as e:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()
