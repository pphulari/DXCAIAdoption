"""
Databricks PR review agent — now with a human-in-the-loop gate.

Fetches a pull request's diff, sends it to Claude with a Databricks-focused
system prompt, posts the findings as a PR comment, and opens a pending
GitHub Check Run. The check run is left pending (no conclusion) — a
downstream job in the workflow completes it only after a human approves
or rejects via the protected environment. As long as this check is
registered as a required status check in branch protection, the PR
cannot merge until that happens.
"""

import os
import sys

from anthropic import Anthropic

from gh_api import (
    get_pr_info,
    get_pr_diff,
    get_pr_files,
    post_pr_comment,
    create_check_run,
    complete_check_run,
    set_output,
)

MODEL = "claude-sonnet-4-6"
MAX_DIFF_CHARS = 60_000  # keep the prompt bounded on very large PRs


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


def main() -> None:
    repo = os.environ["GITHUB_REPOSITORY"]
    pr_number = os.environ["PR_NUMBER"]
    github_token = os.environ["GITHUB_TOKEN"]
    anthropic_key = os.environ["ANTHROPIC_API_KEY"]

    pr_info = get_pr_info(repo, pr_number, github_token)
    head_sha = pr_info["head"]["sha"]

    diff = get_pr_diff(repo, pr_number, github_token)

    if not diff.strip():
        print("Empty diff, nothing to review — auto-clearing the check.")
        check_run_id = create_check_run(
            repo, head_sha, github_token, "No reviewable changes found."
        )
        complete_check_run(
            repo, check_run_id, github_token, "success", "No reviewable changes found."
        )
        set_output("check_run_id", str(check_run_id))
        return

    changed_files = get_pr_files(repo, pr_number, github_token)
    review = review_with_claude(diff, changed_files, anthropic_key)

    comment_body = (
        "## Databricks code review (automated)\n\n"
        f"{review}\n\n"
        "---\n"
        "_This PR is blocked pending human approval of the AI review "
        "(see the **Databricks AI Review** check below). "
        "Add the `skip-ai-review` label to opt out._"
    )
    post_pr_comment(repo, pr_number, github_token, comment_body)

    check_run_id = create_check_run(
        repo,
        head_sha,
        github_token,
        "AI review posted as a PR comment above. Waiting for an authorized "
        "reviewer to approve or reject before this PR can merge.",
    )
    set_output("check_run_id", str(check_run_id))
    print(f"Posted review comment and opened pending check run {check_run_id}.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001 — surface any failure clearly in Actions logs
        print(f"Review agent failed: {e}", file=sys.stderr)
        sys.exit(1)
