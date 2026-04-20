"""Output persistence helpers for local and GitHub saves."""

import subprocess


def write_sql_output_files(folder_path, staging_sql, transform_sql, business_sql):
    """Write generated SQL output files to target folder."""
    folder_path.mkdir(parents=True, exist_ok=False)
    (folder_path / "staging.sql").write_text(staging_sql, encoding="utf-8")
    (folder_path / "transform.sql").write_text(transform_sql, encoding="utf-8")
    (folder_path / "business.sql").write_text(business_sql, encoding="utf-8")


def save_outputs_in_github(base_path, output_folder_path):
    """Commit and push output folder to GitHub main branch."""
    branch_result = subprocess.run(
        ["git", "-C", str(base_path), "branch", "--show-current"],
        capture_output=True,
        text=True,
        check=True,
    )
    current_branch = (branch_result.stdout or "").strip()
    if current_branch != "main":
        raise ValueError(
            f"Current git branch is '{current_branch}'. Please switch to 'main' before saving outputs in GitHub."
        )

    relative_output_path = str(output_folder_path.relative_to(base_path))
    subprocess.run(
        ["git", "-C", str(base_path), "add", relative_output_path],
        capture_output=True,
        text=True,
        check=True,
    )

    commit_message = f"Add ETL outputs {output_folder_path.name}"
    commit_result = subprocess.run(
        ["git", "-C", str(base_path), "commit", "-m", commit_message],
        capture_output=True,
        text=True,
        check=False,
    )
    if commit_result.returncode != 0:
        details = (commit_result.stderr or commit_result.stdout or "").strip()
        raise ValueError(f"Git commit failed: {details}")

    push_result = subprocess.run(
        ["git", "-C", str(base_path), "push", "origin", "main"],
        capture_output=True,
        text=True,
        check=False,
    )
    if push_result.returncode != 0:
        details = (push_result.stderr or push_result.stdout or "").strip()
        raise ValueError(f"Git push failed: {details}")
