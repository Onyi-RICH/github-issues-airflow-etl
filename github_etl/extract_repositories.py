import pandas as pd
from github import Github

def extract_repositories_for_user(github_client: Github) -> pd.DataFrame:
    """
    Extract 1 row per repository for FK integrity.
    """
    user = github_client.get_user()
    repos = list(user.get_repos())

    rows = []

    for repo in repos:
        rows.append({
            "repo_id": str(repo.id),              # MUST be string
            "repo_name": repo.name,
            "repo_full_name": repo.full_name,
            "created_at": repo.created_at,
            "owner": repo.owner.login,
        })

    return pd.DataFrame(rows)
