"""
Demo script for cloning and running the Tuva Project Demo connector and writing to a DuckDB database.
This will only download the repo once. If you wish to clone it again, delete the existing folder.

Requirements:
- DuckDB library installed

Usage:
    python tuva_demo.py

"""

import subprocess
from pathlib import Path
from typing import Optional

import duckdb
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

@task(retries=2, retry_delay_seconds=5, log_prints=True)
def clone_repository(repo_url: str, target_dir: Optional[Path] = None) -> Path:
    """Clone the Tuva Health demo repository."""
    logger = get_run_logger()

    if target_dir is None:
        # Write to root/dbt folder
        dbt_dir = Path.cwd() / "dbt"
        dbt_dir.mkdir(exist_ok=True)
        target_dir = dbt_dir / "tuva_demo"
    else:
        target_dir = Path(target_dir)


    if not target_dir.exists():
        logger.info(f"Cloning repository from {repo_url} to {target_dir}")

        # shutil.rmtree(target_dir)

        result = subprocess.run(
            ["git", "clone", repo_url, str(target_dir)],
            capture_output=True,
            text=True,
            check=True
        )

        logger.info(f"Repository cloned successfully to {target_dir}")
    else:
        logger.info(f"Repository exists at {target_dir}")

    return target_dir


@task(retries=2, retry_delay_seconds=5, log_prints=True)
def setup_duckdb_profile(project_dir: Path, db_path: Optional[Path] = None) -> tuple[Path, Path]:
    """Set up DuckDB profile and database for dbt using local directories."""
    logger = get_run_logger()

    # Use local data directory for database
    data_dir = Path.cwd() / "data"
    data_dir.mkdir(exist_ok=True)

    if db_path is None:
        db_path = data_dir / "tuva_demo.duckdb"

    logger.info(f"Setting up DuckDB database at {db_path}")

    # Create the DuckDB database file
    conn = duckdb.connect(str(db_path))
    conn.close()

    # Use local profiles directory for profiles
    profiles_dir = Path.cwd() / "config"
    profiles_dir.mkdir(exist_ok=True)

    # Create profiles.yml for DuckDB in local config directory
    profiles_yml_content = f"""tuva_demo:
  outputs:
    dev:
      type: duckdb
      path: {db_path.absolute()}
      threads: 4
      extensions:
        - httpfs
  target: dev
"""

    profiles_yml_path = profiles_dir / "profiles.yml"

    # Write the profiles.yml (overwrite if exists for clean setup)
    with open(profiles_yml_path, 'w') as f:
        f.write(profiles_yml_content)

    logger.info(f"DuckDB profile configured at {profiles_yml_path}")
    logger.info(f"Database file created at {db_path}")

    return db_path, profiles_dir


@task(retries=2, retry_delay_seconds=5, log_prints=True)
def update_dbt_project_config(project_dir: Path) -> None:
    """Update dbt_project.yml to use the DuckDB profile."""
    logger = get_run_logger()

    dbt_project_path = project_dir / "dbt_project.yml"

    if not dbt_project_path.exists():
        raise FileNotFoundError(f"dbt_project.yml not found at {dbt_project_path}")

    # Read the existing dbt_project.yml
    with open(dbt_project_path, 'r') as f:
        content = f.read()

    # Update the profile line
    lines = content.split('\n')
    for i, line in enumerate(lines):
        if line.strip().startswith('profile:'):
            lines[i] = 'profile: "tuva_demo"'
            break
    else:
        # If profile line doesn't exist, add it after the name line
        for i, line in enumerate(lines):
            if line.strip().startswith('name:'):
                lines.insert(i + 1, 'profile: "tuva_demo"')
                break

    # Write the updated content back
    with open(dbt_project_path, 'w') as f:
        f.write('\n'.join(lines))

    logger.info(f"Updated dbt_project.yml to use tuva_demo profile")


@task(retries=2, retry_delay_seconds=5, log_prints=True)
def run_dbt_commands(commands: list[str], project_dir: Path, profiles_dir: Path) -> None:
    """Run dbt commands with Prefect integration."""
    print(f"Running: {commands}")
    settings = PrefectDbtSettings(
        project_dir=str(project_dir),
        profiles_dir=str(profiles_dir),
    )
    runner = PrefectDbtRunner(settings=settings, raise_on_failure=False)
    for command in commands:
        print(f"dbt {command}")
        runner.invoke(command.split())
        print(f"Completed: dbt {command}")


@task(retries=2, retry_delay_seconds=5, log_prints=True)
def verify_results(db_path: Path) -> dict:
    """Verify the results by querying the DuckDB database."""
    logger = get_run_logger()

    conn = duckdb.connect(str(db_path))

    # Get list of tables
    tables_result = conn.execute("SHOW TABLES").fetchall()
    tables = [table[0] for table in tables_result]

    # Get row counts for key tables
    table_counts = {}
    for table in tables:
        try:
            count_result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            table_counts[table] = count_result[0]
        except Exception as e:
            table_counts[table] = f"Error: {str(e)}"

    conn.close()

    results = {
        "tables_created": len(tables),
        "table_list": tables,
        "table_counts": table_counts
    }

    logger.info(f"Verification results: {results}")
    return results


@flow(name="Tuva Health Demo", log_prints=True)
def tuva_demo_flow(
    repo_url: str = "https://github.com/tuva-health/demo",
    target_dir: Optional[str] = None,
    db_path: Optional[str] = None
) -> dict:
    """
    Main Prefect flow to clone and run the Tuva Health demo with DuckDB.

    Args:
        repo_url: URL of the Tuva Health demo repository
        target_dir: Directory to clone the repository to (optional)
        db_path: Path for the DuckDB database file (optional)

    Returns:
        Dictionary with verification results
    """
    logger = get_run_logger()
    logger.info("Starting Tuva Health Demo Flow")

    # Convert string paths to Path objects if provided
    target_path = Path(target_dir) if target_dir else None
    db_path_obj = Path(db_path) if db_path else None

    # Clone the repository
    project_dir = clone_repository(repo_url, target_path)

    # Set up DuckDB profile and database
    database_path, profiles_dir = setup_duckdb_profile(project_dir, db_path_obj)

    # Update dbt project configuration
    update_dbt_project_config(project_dir)

    # Install dbt dependencies
    # install_dbt_dependencies(project_dir, profiles_dir)
    run_dbt_commands(["deps"], project_dir, profiles_dir)

    # Run dbt build
    # run_dbt_build(project_dir, profiles_dir)
    run_dbt_commands(["seed"], project_dir, profiles_dir)
    run_dbt_commands(["run"], project_dir, profiles_dir)
    run_dbt_commands(["test"], project_dir, profiles_dir)

    # Verify results
    results = verify_results(database_path)

    logger.info("Tuva Health Demo Flow completed successfully")
    logger.info(f"Project directory: {project_dir}")
    logger.info(f"Database path: {database_path}")

    return {
        "project_directory": str(project_dir),
        "database_path": str(database_path),
        "profiles_directory": str(profiles_dir),
        "verification_results": results
    }

if __name__ == "__main__":
    result = tuva_demo_flow()
    print(f"Flow completed: {result}")