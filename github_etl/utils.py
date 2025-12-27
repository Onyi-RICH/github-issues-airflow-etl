import pandas as pd
import numpy as np

# Simple in-memory cache
CACHE = {
    "timeline": {},
    "comments": {},
}

def get_cached_timeline(issue):
    key = issue.id
    if key not in CACHE["timeline"]:
        CACHE["timeline"][key] = list(issue.get_timeline())
    return CACHE["timeline"][key]

def get_cached_comments(issue):
    key = issue.id
    if key not in CACHE["comments"]:
        CACHE["comments"][key] = list(issue.get_comments())
    return CACHE["comments"][key]


def clean_issue_frame(df: pd.DataFrame) -> pd.DataFrame:
    """
    Defensive cleaning aligned with github_source_data.real_github_issues
    """

    df = df.copy()

    # ------------------------------------------------------------------
    # 0. Ensure ALL expected columns exist (CRITICAL for Airflow safety)
    # ------------------------------------------------------------------
    expected_columns = [
        "detail_node_id",
        "repo_id",
        "issue_id",
        "issue_title",
        "source",
        "timestamp",
        "actor",
        "action",
        "detail_id",
        "assignee",
        "current_assignee",
        "repo_name",
        "owner",
    ]

    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    # ------------------------------------------------------------------
    # 1. Fill NULL assignees
    # ------------------------------------------------------------------
    df["assignee"] = df["assignee"].fillna("N/A")
    df["current_assignee"] = df["current_assignee"].fillna("N/A")

    # ------------------------------------------------------------------
    # 2. Null invalid detail_id values (strings containing '/')
    # ------------------------------------------------------------------
    df.loc[
        df["detail_id"].astype(str).str.contains("/", na=False),
        "detail_id",
    ] = None

    # ------------------------------------------------------------------
    # 3. Enforce correct data types (match DB schema)
    # ------------------------------------------------------------------
    df["detail_id"] = df["detail_id"].apply(
        lambda x: None if pd.isna(x) else str(x)
    )

    df["detail_node_id"] = df["detail_node_id"].astype(str)
    df["repo_id"] = df["repo_id"].astype(str)

    # issue_id should remain INT-compatible
    df["issue_id"] = df["issue_id"].apply(
        lambda x: None if pd.isna(x) else int(x)
    )

    # ------------------------------------------------------------------
    # 4. Timestamp safety
    # ------------------------------------------------------------------
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    return df
