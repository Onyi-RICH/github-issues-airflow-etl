import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from .utils import clean_issue_frame


def normalize_python_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert pandas / NumPy scalar types to native Python types
    so psycopg2 can adapt them safely.
    """
    for col in df.columns:

        # 🚫 detail_id is TEXT — never coerce
        if col == "detail_id":
            df[col] = df[col].apply(
                lambda x: None if pd.isna(x) else str(x)
            )
            continue

        # Integers (real BIGINT columns)
        if pd.api.types.is_integer_dtype(df[col]):
            df[col] = df[col].apply(
                lambda x: int(x) if not pd.isna(x) else None
            )

        # Floats
        elif pd.api.types.is_float_dtype(df[col]):
            df[col] = df[col].apply(
                lambda x: float(x) if not pd.isna(x) else None
            )

        # Timestamps
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].apply(
                lambda x: x.to_pydatetime() if not pd.isna(x) else None
            )

        # Objects
        else:
            df[col] = df[col].apply(
                lambda x: None if pd.isna(x) else x
            )

    return df




def load_issues_to_postgres(df: pd.DataFrame, engine):
    df = clean_issue_frame(df)
    df = df.drop_duplicates(subset=["detail_node_id"], keep="first")

    if df.empty:
        print("No records to load.")
        return

    # 🔥 CRITICAL FIX
    df = normalize_python_types(df)

    cols = list(df.columns)
    values = [tuple(row) for row in df.itertuples(index=False, name=None)]

    insert_sql = f"""
        INSERT INTO github_source_data.real_github_issues ({",".join(cols)})
        VALUES %s
        ON CONFLICT (detail_node_id) DO NOTHING
    """

    raw_conn = engine.raw_connection()

    try:
        with raw_conn.cursor() as cur:
            execute_values(
                cur,
                insert_sql,
                values,
                page_size=2000
            )
        raw_conn.commit()
        print(f"✅ SUCCESS — Loaded {len(values)} rows safely.")
    except Exception as e:
        raw_conn.rollback()
        print(f"❌ LOAD FAILED: {e}")
        raise
    finally:
        raw_conn.close()
