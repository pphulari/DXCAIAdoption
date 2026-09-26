"""
MVP Databricks PR review agent.

Fetches a pull request's diff via the GitHub REST API, sends it to Claude
with a Databricks-focused system prompt, and posts the result back as a
single PR comment.

This talks to the GitHub REST API directly rather than through an MCP
server, to keep the MVP simple and dependency-light. Once this loop is
proven reliable, swap get_pr_diff / get_pr_files / post_pr_comment for
calls through your GitHub MCP client without changing anything else.
"""

import os
import sys

import requests
from anthropic import Anthropic

GITHUB_API = "https://api.github.com"
MODEL = "claude-sonnet-4-6"
MAX_DIFF_CHARS = 60_000  # keep the prompt bounded on very large PRs


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


def load_system_prompt() -> str:
    path = os.path.join(os.path.dirname(__file__), "prompts", "system_prompt.md")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def review_with_claude(diff: str, changed_files: list[dict], api_key: str) -> str:
    client = Anthropic(api_key=api_key)

    file_list = "\n".join(
        f"- {f['filename']} (+{f['additions']}/-{f['deletions']})"
        for f in changed_files
    )
    truncated = diff[:MAX_DIFF_CHARS]
    truncation_note = (
        "\n\n[diff truncated for length]" if len(diff) > MAX_DIFF_CHARS else ""
    )

    user_prompt = (
        f"Changed files in this PR:\n{file_list}\n\n"
        f"Unified diff:\n```diff\n{truncated}{truncation_note}\n```\n\n"
        "Review this diff per your instructions and return your findings."
    )

    response = client.messages.create(
        model=MODEL,
        max_tokens=2000,
        system=load_system_prompt(),
        messages=[{"role": "user", "content": user_prompt}],
    )
    return "".join(block.text for block in response.content if block.type == "text")


def post_pr_comment(repo: str, pr_number: str, token: str, body: str) -> None:
    url = f"{GITHUB_API}/repos/{repo}/issues/{pr_number}/comments"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    resp = requests.post(url, headers=headers, json={"body": body}, timeout=30)
    resp.raise_for_status()


def main() -> None:
    repo = os.environ["GITHUB_REPOSITORY"]
    pr_number = os.environ["PR_NUMBER"]
    github_token = os.environ["GITHUB_TOKEN"]
    anthropic_key = os.environ["ANTHROPIC_API_KEY"]

    diff = get_pr_diff(repo, pr_number, github_token)
    if not diff.strip():
        print("Empty diff, nothing to review.")
        return

    changed_files = get_pr_files(repo, pr_number, github_token)
    review = review_with_claude(diff, changed_files, anthropic_key)

    comment_body = (
        "## DXC Databricks Code Review Agent (automated)\n\n"
        f"{review}\n\n"
        "---\n"
        "_Automated first pass — reply here or tag a teammate for anything "
        "unclear. Add the `skip-ai-review` label to opt a PR out._"
    )
    post_pr_comment(repo, pr_number, github_token, comment_body)
    print("Posted review comment.")


if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print(f"GitHub API error: {e}", file=sys.stderr)
        sys.exit(1)
