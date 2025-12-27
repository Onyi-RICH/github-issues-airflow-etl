import os
from sqlalchemy import create_engine
from github import Github, Auth
from github_etl.extract_all_issues import extract_all_issue_events_for_user
from github_etl.load_to_postgres import load_issues_to_postgres


# ----------------------------------------
# 1. Environment Variables (REQUIRED)
# ----------------------------------------

GITHUB_PAT = os.environ.get("GITHUB_PAT")
DATABASE_URL = os.environ.get("DATABASE_URL")

if not GITHUB_PAT:
    raise RuntimeError("Missing GITHUB_PAT env variable")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL env variable")


# ----------------------------------------
# 2. Clients
# ----------------------------------------

github = Github(auth=Auth.Token(GITHUB_PAT))
engine = create_engine(DATABASE_URL)


# ----------------------------------------
# 3. Run ETL Twice (Idempotency Test)
# ----------------------------------------

print("Extracting GitHub issues...")
df = extract_all_issue_events_for_user(github)
print(f"Extracted rows: {len(df)}")

print("\nLoading data — Run 1")
load_issues_to_postgres(df, engine)

print("\nLoading data — Run 2 (idempotency check)")
load_issues_to_postgres(df, engine)

print("\n✅ TEST COMPLETE — No duplicates should exist.")
