import os

def get_github_token():
    """
    Returns GitHub PAT from env var.
    Used locally AND inside Airflow if Airflow Connections are not used.
    """
    token = os.getenv("GITHUB_PAT")
    if not token:
        raise ValueError("GITHUB_PAT environment variable not set.")
    return token

def get_database_url():
    """
    Returns DATABASE_URL from env var, used for SQLAlchemy engine.
    """
    url = os.getenv("DATABASE_URL")
    if not url:
        raise ValueError("DATABASE_URL environment variable not set.")
    return url
