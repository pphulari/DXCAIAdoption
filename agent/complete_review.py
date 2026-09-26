"""
Completes the pending "Databricks AI Review" check run once a human has
approved or rejected it via the protected environment gate. This is what
actually unblocks (or keeps blocking) the merge.
"""

import os
import sys

from gh_api import complete_check_run


def main() -> None:
    repo = os.environ["GITHUB_REPOSITORY"]
    check_run_id = os.environ["CHECK_RUN_ID"]
    token = os.environ["GITHUB_TOKEN"]
    conclusion = os.environ["CONCLUSION"]  # "success" or "failure"
    approver = os.environ.get("APPROVER", "a reviewer")

    if conclusion not in ("success", "failure"):
        print(f"Invalid conclusion: {conclusion}", file=sys.stderr)
        sys.exit(1)

    if conclusion == "success":
        summary = f"Manually approved via the pr-review-approval environment (actor: @{approver})."
    else:
        summary = (
            f"Rejected via the pr-review-approval environment (actor: @{approver}). "
            "Push a fix or re-run the workflow to request review again."
        )

    complete_check_run(repo, check_run_id, token, conclusion, summary)
    print(f"Check run {check_run_id} set to {conclusion}.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001
        print(f"Failed to complete check run: {e}", file=sys.stderr)
        sys.exit(1)
