import os
from sqlalchemy import create_engine
from github import Github, Auth
from github_etl.extract_repositories import extract_repositories_for_user
from github_etl.load_repositories import load_repositories_to_postgres

github = Github(auth=Auth.Token(os.environ["GITHUB_PAT"]))
engine = create_engine(os.environ["DATABASE_URL"])

df_repos = extract_repositories_for_user(github)
print(f"Extracted {len(df_repos)} repositories")

load_repositories_to_postgres(df_repos, engine)
