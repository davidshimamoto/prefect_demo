"""
Demo script for copying and running the Prefect dbt Demo connector and writing to a DuckDB database.

Requirements:
- DuckDB library installed

Usage:
    python tuva_demo.py

"""

import io
import shutil
import urllib.request
import zipfile
from pathlib import Path
import os

from prefect import flow, task
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

DEFAULT_REPO_ZIP = "https://github.com/PrefectHQ/examples/archive/refs/heads/examples-markdown.zip"


@task(retries=2, retry_delay_seconds=5, log_prints=True)
def build_dbt_demo(repo_zip_url: str = DEFAULT_REPO_ZIP) -> Path:
    """Download and extract dbt project (cache if exists)."""
    # Write to root/dbt folder
    dbt_dir = Path.cwd() / "dbt"
    dbt_dir.mkdir(exist_ok=True)
    project_dir = dbt_dir / "dbt_demo"
    if project_dir.exists():
        print(f"Using cached project at {project_dir}")
        return project_dir
    tmp_extract_base = project_dir.parent / "_tmp_extract"
    if tmp_extract_base.exists():
        shutil.rmtree(tmp_extract_base)
    print(f"Downloading: {repo_zip_url}")
    with urllib.request.urlopen(repo_zip_url) as resp:
        data = resp.read()
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        zf.extractall(tmp_extract_base)
    candidates = list(tmp_extract_base.rglob("**/dbt_project.yml"))
    if not candidates:
        raise ValueError("dbt_project.yml not found")
    project_root = candidates[0].parent
    shutil.move(str(project_root), str(project_dir))
    shutil.rmtree(tmp_extract_base)
    print(f"Extracted to {project_dir}")
    return project_dir


@task(retries=2, retry_delay_seconds=5, log_prints=True)
def create_dbt_profiles(project_dir: Path) -> None:
    """Generate profiles.yml for your warehouse (e.g., DuckDB)."""
    # Use the same data directory as tuva_demo
    data_dir = Path.cwd() / "data"
    data_dir.mkdir(exist_ok=True)
    db_path = data_dir / "demo.duckdb"

    profiles_content = """demo:
  outputs:
    dev:
      type: duckdb
      path: {}
      threads: 1
  target: dev""".format(db_path.as_posix())
    profiles_path = project_dir / "profiles.yml"
    with open(profiles_path, "w") as f:
        f.write(profiles_content)
    print(f"Created profiles at {profiles_path}")


@task(retries=2, retry_delay_seconds=5, log_prints=True)
def run_dbt_commands(commands: list[str], project_dir: Path) -> None:
    """Run dbt commands with Prefect integration."""
    print(f"Running: {commands}")
    settings = PrefectDbtSettings(
        project_dir=str(project_dir),
        profiles_dir=str(project_dir),
    )
    runner = PrefectDbtRunner(settings=settings, raise_on_failure=False)
    for command in commands:
        print(f"dbt {command}")
        runner.invoke(command.split())
        print(f"Completed: dbt {command}")


@flow(name="dbt Demo", log_prints=True)
def dbt_flow(repo_zip_url: str = DEFAULT_REPO_ZIP) -> None:
    """Orchestrate full dbt lifecycle daily."""
    project_dir = build_dbt_demo(repo_zip_url)
    create_dbt_profiles(project_dir)
    run_dbt_commands(["deps"], project_dir)
    run_dbt_commands(["seed"], project_dir)
    run_dbt_commands(["run"], project_dir)
    run_dbt_commands(["test"], project_dir)
    data_dir = Path.cwd() / "data"
    duckdb_path = data_dir / "demo.duckdb"
    print(f"Done! Output at: {duckdb_path.resolve()}")


if __name__ == "__main__":
    dbt_flow()
