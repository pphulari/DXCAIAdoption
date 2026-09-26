# DXC Databricks PR Review Agent

An AI code review agent that automatically reviews pull requests
touching Databricks/PySpark code, posts findings as a PR comment, and
requires a human to approve or reject the review before the PR can
merge.

## What it does

On every pull request, the agent:

1. Fetches the PR's diff and changed files from GitHub.
2. Sends the diff to Claude with a system prompt scoped to Databricks
   concerns: performance anti-patterns, Delta Lake / Unity Catalog
   correctness, security, and cluster cost.
3. Posts the findings as a single PR comment.
4. Opens a **pending** GitHub check run named `Databricks AI Review`.
5. Pauses and waits for an authorized human reviewer to click
   **Approve** or **Reject** in the GitHub Actions UI.
6. Resolves the check run based on that decision. Because the check is
   registered as a required status check, **the PR cannot merge until
   this resolves** — the AI's findings are advisory, the human
   decision is what actually gates the merge.

## Repo structure

```
.github/workflows/
  databricks-review.yml   # triggers the agent on PR open/update
agent/
  review_agent.py          # fetches diff, calls Claude, posts comment, opens check run
  complete_review.py       # resolves the check run after a human decision
  gh_api.py                 # shared GitHub REST API helpers
  requirements.txt
  prompts/
    system_prompt.md        # defines what the agent looks for and how it reports findings
README.md
```

## How the workflow is structured

The workflow (`.github/workflows/databricks-review.yml`) has four jobs:

| Job | Runs when | Does |
|---|---|---|
| `post-review` | PR opened/updated, no `skip-ai-review` label | Runs the agent, posts the comment, opens the pending check |
| `await-approval` | `post-review` succeeded | Pauses on the `pr-review-approval` environment until a reviewer decides |
| `approve-check` | `await-approval` was approved | Sets the check run to `success` |
| `reject-check` | `await-approval` was rejected | Sets the check run to `failure` |

The agent talks to the GitHub REST API directly (not through the
GitHub MCP server) to keep the MVP dependency-light. The three
functions in `gh_api.py` that make the network calls
(`get_pr_diff`, `get_pr_files`, `post_pr_comment`,
`create_check_run`, `complete_check_run`) are isolated so they can be
swapped for MCP tool calls later without touching the review logic
itself.

## One-time setup

### 1. Add the API key secret

Repo **Settings → Secrets and variables → Actions → New repository
secret**, name it `ANTHROPIC_API_KEY`. (`GITHUB_TOKEN` is provided
automatically by Actions — no setup needed.)

### 2. Create the approval environment

1. **Settings → Environments → New environment**, name it exactly
   `pr-review-approval` (must match the workflow YAML), then
   **Configure environment**.
2. Check **Required reviewers**, then search for and add the
   people/teams allowed to approve or reject reviews.
   - Pick reviewers who typically *aren't* the ones opening these
     PRs — this setup can't stop a PR author from approving their own
     PR in code, only by policy (see Known limitations).
3. Click **Save protection rules**.

### 3. Run the workflow once

Open a test PR so the `post-review` job runs and the
`Databricks AI Review` check reports at least once — GitHub only lets
you search for a check by name in branch protection after it has run.

### 4. Make the check required

1. **Settings → Branches**, edit (or add) the protection rule for your
   target branch.
2. Enable **Require status checks to pass before merging**, then
   search for and add `Databricks AI Review`.
3. Recommended: enable **Require approval of the most recent
   reviewable push**, so an approval on an old commit doesn't cover
   new, unreviewed changes.
4. Recommended: enable **Do not allow bypassing the above settings**,
   otherwise repo admins can merge without waiting for the check.
5. Save.

### 5. Confirm it works end to end

Open (or reuse) a test PR with a deliberate issue (e.g. a stray
`.collect()` call or a hardcoded token). Confirm:
- a review comment appears within a minute or two,
- a pending `Databricks AI Review` check appears and the merge button
  is disabled,
- going to **Actions → the workflow run → Review deployments** and
  approving or rejecting resolves the check accordingly.

## Opting a PR out

Add the `skip-ai-review` label to any PR. No check run is created, so
nothing blocks that PR's merge.

## Known limitations

- **Self-approval isn't enforced in code.** An environment-gated job
  doesn't expose "who clicked approve" in a way the workflow can check
  against the PR author at run time — this has to be enforced by who
  you choose as required reviewers, not by logic in the scripts.
- **`github.actor` in the approve/reject jobs is the actor who
  triggered the workflow run (usually the PR's pusher), not
  necessarily the person who clicked Approve/Reject.** For a precise
  audit trail of who approved, query the run's deployment review
  history via the GitHub API rather than relying on the `APPROVER`
  value logged by `complete_review.py`.
- **Stuck reviews**: if nobody responds, the check stays pending
  indefinitely and the PR stays blocked. Consider adding
  `timeout-minutes` to the `await-approval` job, or a scheduled
  workflow that fails any check pending longer than a set window.
- **New commits orphan the check**: a new push changes the head SHA,
  so the old check run no longer applies. This is expected — the
  workflow re-runs on `synchronize` and opens a fresh check for the
  new commit.
- **GITHUB_TOKEN and PR reviews**: this design intentionally drives
  the merge gate through a check run, not a native GitHub PR review
  (Approve/Request changes) — reviews submitted via `GITHUB_TOKEN`
  don't count toward branch protection's required-approval count, but
  check runs aren't subject to that restriction.

## Roadmap / natural next steps

- Swap the direct GitHub REST calls in `gh_api.py` for real GitHub MCP
  tool calls once an MCP client is wired into the workflow.
- Move from one summary comment to inline, line-level comments via the
  GitHub "create a review" API, mapping findings to diff hunks.
- Add deterministic static checks (`ruff`, `sqlfluff`, a secrets
  scanner) that run before the LLM call and get passed in as extra
  context, so the LLM reasons about *why* something matters rather
  than just pattern-matching.
- Add an explicit self-approval check if you move to a slash-command
  based decision channel (`/approve`, `/reject` as PR comments)
  instead of, or alongside, the environment gate.
- Log every review (findings, human decision, token cost) to a table
  for quality and cost tracking over time.
- Move from the default `GITHUB_TOKEN` to a GitHub App installation
  token once this is rolled out across more than one repo.
