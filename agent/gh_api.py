"""
Shared GitHub REST API helpers for the Databricks PR review agent and its
human-in-the-loop approval step.
"""

import os
import requests

GITHUB_API = "https://api.github.com"
CHECK_NAME = "Databricks AI Review"  # must match the name used in branch protection


def get_pr_info(repo: str, pr_number: str, token: str) -> dict:
    url = f"{GITHUB_API}/repos/{repo}/pulls/{pr_number}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_pr_diff(repo: str, pr_number: str, token: str) -> str:
    url = f"{GITHUB_API}/repos/{repo}/pulls/{pr_number}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3.diff",
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.text


def get_pr_files(repo: str, pr_number: str, token: str) -> list[dict]:
    url = f"{GITHUB_API}/repos/{repo}/pulls/{pr_number}/files"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def post_pr_comment(repo: str, pr_number: str, token: str, body: str) -> None:
    url = f"{GITHUB_API}/repos/{repo}/issues/{pr_number}/comments"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    resp = requests.post(url, headers=headers, json={"body": body}, timeout=30)
    resp.raise_for_status()


def create_check_run(repo: str, head_sha: str, token: str, summary: str) -> int:
    """Creates the check run in a pending (in_progress) state. Leaving it
    without a conclusion is what blocks merging once it's added as a
    required status check in branch protection."""
    url = f"{GITHUB_API}/repos/{repo}/check-runs"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    payload = {
        "name": CHECK_NAME,
        "head_sha": head_sha,
        "status": "in_progress",
        "output": {
            "title": "Awaiting human approval",
            "summary": summary,
        },
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()["id"]


def complete_check_run(
    repo: str, check_run_id: str, token: str, conclusion: str, summary: str
) -> None:
    """conclusion must be 'success' or 'failure'."""
    url = f"{GITHUB_API}/repos/{repo}/check-runs/{check_run_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    payload = {
        "status": "completed",
        "conclusion": conclusion,
        "output": {
            "title": "Approved by reviewer"
            if conclusion == "success"
            else "Changes requested by reviewer",
            "summary": summary,
        },
    }
    resp = requests.patch(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()


def set_output(name: str, value: str) -> None:
    """Writes a step output for downstream jobs to read via needs.<job>.outputs."""
    output_file = os.environ.get("GITHUB_OUTPUT")
    if output_file:
        with open(output_file, "a", encoding="utf-8") as f:
            f.write(f"{name}={value}\n")
