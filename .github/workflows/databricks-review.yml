name: Databricks PR Review

on:
  pull_request:
    types: [opened, synchronize, reopened]

permissions:
  contents: read
  pull-requests: write
  checks: write

jobs:
  # Runs the AI review, posts the PR comment, and opens a pending check run.
  post-review:
    if: ${{ !contains(github.event.pull_request.labels.*.name, 'skip-ai-review') }}
    runs-on: ubuntu-latest
    outputs:
      check_run_id: ${{ steps.run_agent.outputs.check_run_id }}
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install -r agent/requirements.txt

      - name: Run review agent
        id: run_agent
        working-directory: agent
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
          GITHUB_REPOSITORY: ${{ github.repository }}
          PR_NUMBER: ${{ github.event.pull_request.number }}
        run: python review_agent.py

  # Pauses here until a reviewer configured on the "pr-review-approval"
  # environment clicks Approve or Reject in the Actions UI.
  await-approval:
    needs: post-review
    if: needs.post-review.outputs.check_run_id != ''
    runs-on: ubuntu-latest
    environment: pr-review-approval
    steps:
      - name: Decision recorded
        run: echo "Environment reviewer approved this run."

  # Only runs if the environment gate was approved.
  approve-check:
    needs: [post-review, await-approval]
    if: needs.await-approval.result == 'success'
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install -r agent/requirements.txt

      - name: Mark check run as approved
        working-directory: agent
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          GITHUB_REPOSITORY: ${{ github.repository }}
          CHECK_RUN_ID: ${{ needs.post-review.outputs.check_run_id }}
          CONCLUSION: success
          APPROVER: ${{ github.actor }}
        run: python complete_review.py

  # Only runs if the environment gate was rejected.
  reject-check:
    needs: [post-review, await-approval]
    if: needs.await-approval.result == 'failure'
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install -r agent/requirements.txt

      - name: Mark check run as rejected
        working-directory: agent
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          GITHUB_REPOSITORY: ${{ github.repository }}
          CHECK_RUN_ID: ${{ needs.post-review.outputs.check_run_id }}
          CONCLUSION: failure
          APPROVER: ${{ github.actor }}
        run: python complete_review.py
