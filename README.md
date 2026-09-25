# Databricks PR review agent — MVP
----
Automated first-pass code review for Databricks/PySpark pull requests.
On every PR open or update, a GitHub Actions workflow sends the diff to
Claude with a Databricks-focused review prompt and posts the findings as
a PR comment.

## Setup

1. Copy this folder's contents into the root of your repo (keep the
   `.github/workflows` and `agent/` paths as-is).
2. Add one repo secret: **Settings > Secrets and variables > Actions >
   New repository secret**
   - `ANTHROPIC_API_KEY` — your Claude API key
   (`GITHUB_TOKEN` is provided automatically by Actions — no setup
   needed for a single-repo MVP.)
3. Open a test PR with a deliberate issue (e.g. a `.collect()` call or a
   hardcoded token) and confirm a review comment appears within a
   minute or two of the workflow completing.

## Opting a PR out

Add the `skip-ai-review` label to any PR to skip the automated review.

## Next steps beyond this MVP

- Swap the direct GitHub REST calls in `review_agent.py` for your
  GitHub MCP client once it's wired up — the function signatures
  (`get_pr_diff`, `get_pr_files`, `post_pr_comment`) are written so the
  rest of the script doesn't need to change.
- Move from a single summary comment to inline line-level comments via
  the GitHub "create a review" API, mapping findings to diff hunks.
- Add deterministic static checks (ruff, sqlfluff, a secrets scanner)
  that run before the LLM call and get passed in as extra context.
- Move from the default `GITHUB_TOKEN` to a GitHub App installation
  token once you roll this out across more than one repo.
- Log every review (findings, engineer feedback, token cost) to a
  table for quality and cost tracking.
