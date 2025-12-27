import json
import pandas as pd
from datetime import datetime
from typing import Optional
from tqdm import tqdm
from github import Github
from .utils import get_cached_comments, get_cached_timeline


def flatten_issue_clean(issue):
    """
    Produce row-per-action event history for GitHub issues (non-PR).
    """
    # Skip PRs directly
    if issue.pull_request:
        return pd.DataFrame()

    rows = []
    seen = set()

    repo_id = issue.repository.id
    issue_id = issue.number
    issue_title = issue.title
    current_assignee = issue.assignee.login if issue.assignee else None

    def add_row(source, timestamp, actor, action, details):
        key = (source, timestamp, actor, action, json.dumps(details, sort_keys=True))
        if key in seen:
            return
        seen.add(key)

        detail_id = details.get("id") if isinstance(details, dict) else None
        detail_node_id = details.get("node_id") if isinstance(details, dict) else None

        assignee = None
        if isinstance(details, dict) and isinstance(details.get("assignee"), dict):
            assignee = details["assignee"].get("login")

        rows.append({
            "repo_id": str(repo_id),
            "issue_id": issue_id,
            "issue_title": issue_title,
            "source": source,
            "timestamp": timestamp,
            "actor": actor,
            "action": action,
            "detail_id": detail_id,
            "detail_node_id": detail_node_id,
            "assignee": assignee,
            "current_assignee": current_assignee,
        })

    raw = issue.raw_data
    add_row(
        "issue",
        pd.to_datetime(raw["created_at"]),
        raw.get("user", {}).get("login"),
        "issue_opened",
        {"id": raw.get("id"), "node_id": raw.get("node_id")},
    )

    for c in get_cached_comments(issue):
        r = c.raw_data
        add_row(
            "comments",
            pd.to_datetime(r["created_at"]),
            r["user"]["login"],
            "comment_added",
            {"id": r.get("id"), "node_id": r.get("node_id")},
        )

    for t in get_cached_timeline(issue):
        r = t.raw_data
        add_row(
            "timeline",
            pd.to_datetime(r.get("created_at")),
            r.get("actor", {}).get("login") if r.get("actor") else None,
            r.get("event"),
            r,
        )

    return pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True)


def extract_all_issue_events_for_user(github_client,  since_ts: Optional[datetime] = None):  # DO NOT use datetime | None (Airflow uses Python 3.8)
    """
    Incremental extraction based on GitHub 'updated since' watermark.
    """
    user = github_client.get_user()
    repos = list(user.get_repos())

    all_rows = []

    for repo in tqdm(repos, desc="Processing repositories"):
        try:
            issues = repo.get_issues(
                state="all",
                since=since_ts  # 🔥 incremental
            )
        except Exception as e:
            print(f"ERROR retrieving {repo.full_name}: {e}")
            continue

        for issue in issues:
            df_issue = flatten_issue_clean(issue)
            if df_issue.empty:
                continue

            df_issue["repo_name"] = repo.name
            df_issue["owner"] = user.login
            all_rows.append(df_issue)

    return pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()




