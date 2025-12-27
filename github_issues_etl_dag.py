from unittest import result
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from airflow.datasets import Dataset
from datetime import timedelta

from github import Github, Auth
from sqlalchemy import create_engine, text
import pandas as pd

from github_etl.extract_all_issues import extract_all_issue_events_for_user
from github_etl.load_to_postgres import load_issues_to_postgres

ISSUES_DS = Dataset("postgres://github/real_github_issues")

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "sla": timedelta(minutes=20),
}


def notify_failure(context):
    print(f"❌ DAG FAILED: {context['dag_run']}")


with DAG(
    dag_id="github_issues_etl",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    on_failure_callback=notify_failure,
    tags=["github", "etl"],
) as dag:

    @task                       # Get watermark – Reads last successful timestamp
    def get_watermark():
        db = BaseHook.get_connection("neon_db")
        engine = create_engine(
            f"postgresql+psycopg2://{db.login}:{db.password}@{db.host}:{db.port}/{db.schema}"
        )
        
        with engine.begin() as conn:
            result = conn.execute(text("""
                SELECT last_success_ts
                FROM etl_metadata.github_issue_watermark
                WHERE pipeline_name = 'github_issues'
            """)).scalar()
    # FIRST RUN SAFETY
        if result is None:
            print("ℹ️ No watermark found — full extraction will run")
        else:
            print(f"ℹ️ Incremental extraction since {result}")

        return result


    @task                       # Extract Github incrementally
    def extract(watermark):
        gh = BaseHook.get_connection("github_conn")
        client = Github(auth=Auth.Token(gh.password))
        df = extract_all_issue_events_for_user(client, watermark)
        return df.to_dict(orient="list")

    @task(outlets=[ISSUES_DS])    # Load Idempotent database insert (ON CONFLICT)
    def load(df_dict):
        df = pd.DataFrame(df_dict)
        db = BaseHook.get_connection("neon_db")
        engine = create_engine(
            f"postgresql+psycopg2://{db.login}:{db.password}@{db.host}:{db.port}/{db.schema}"
        )
        load_issues_to_postgres(df, engine)

    @task       # Update watermark only on success
    def update_watermark():
        db = BaseHook.get_connection("neon_db")
        engine = create_engine(
            f"postgresql+psycopg2://{db.login}:{db.password}@{db.host}:{db.port}/{db.schema}"
        )
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE etl_metadata.github_issue_watermark
                SET last_success_ts = NOW()
                WHERE pipeline_name = 'github_issues'
            """))

    wm = get_watermark()
    extracted = extract(wm)
    loaded = load(extracted)
    update_watermark() << loaded










