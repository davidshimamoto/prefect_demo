#!/usr/bin/env python3
"""
Demo script for cloning and running the Tuva Project Demo with Snowflake database.
This combines the Tuva Health repository with Snowflake connectivity using Prefect connector blocks.
This will only download the repo once. If you wish to clone it again, delete the existing folder.

Requirements:
- prefect-snowflake library installed
- prefect-dbt library installed
- Snowflake connector block named "tuva-snowflake-conn" configured in Prefect

Usage:
    python tuva_snowflake_demo.py

Note: The connector block should already include database, warehouse, and schema configuration.
"""

import subprocess
from pathlib import Path
from typing import Optional, Literal

from prefect import flow, task
from prefect.logging import get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from prefect_snowflake import SnowflakeConnector, SnowflakeCredentials


@task(retries=2, retry_delay_seconds=5, log_prints=True)
def clone_repository(repo_url: str, target_dir: Optional[Path] = None) -> Path:
    """Clone the Tuva Health repository."""
    logger = get_run_logger()

    if target_dir is None:
        # Write to root/dbt folder
        dbt_dir = Path.cwd() / "dbt"
        dbt_dir.mkdir(exist_ok=True)
        target_dir = dbt_dir / "tuva_snowflake_demo"
    else:
        target_dir = Path(target_dir)

    if not target_dir.exists():
        logger.info(f"Cloning repository from {repo_url} to {target_dir}")

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
def test_snowflake_connection(connector_block_name: str) -> dict:
    """Test the Snowflake connection using the connector block."""
    logger = get_run_logger()
    logger.info(f"Loading Snowflake connector from block: {connector_block_name}")

    # Load the connector block
    connector = SnowflakeConnector.load(connector_block_name)

    with connector as conn:
        logger.info("Successfully connected to Snowflake!")

        # Test query - get current timestamp, user, database, warehouse, and schema
        result = conn.fetch_one("""
            SELECT
                CURRENT_TIMESTAMP() as current_time,
                CURRENT_USER() as current_user,
                CURRENT_DATABASE() as current_database,
                CURRENT_WAREHOUSE() as current_warehouse,
                CURRENT_SCHEMA() as current_schema
        """)

        connection_info = {
            "timestamp": str(result[0]),
            "user": result[1],
            "database": result[2],
            "warehouse": result[3],
            "schema": result[4]
        }

        logger.info(f"Connection info: {connection_info}")
        return connection_info


# @task
# def load_environment_blocks(environment: Literal["staging", "production"]):
#     """Load the appropriate Prefect blocks based on environment"""
#     # This function is commented out as SnowflakeTargetConfigs is not available
#     # in the current prefect-snowflake version
#     pass


@task(log_prints=True)
def setup_snowflake_profile(project_dir: Path, connector_block_name: str) -> Path:
    """Set up Snowflake profile for dbt using the Prefect connector block."""
    logger = get_run_logger()

    # Load connector to get connection details
    connector = SnowflakeConnector.load(connector_block_name)

    # Use local profiles directory for profiles
    profiles_dir = Path.cwd() / "config"
    profiles_dir.mkdir(exist_ok=True)

    # Create profiles.yml for Snowflake in local config directory
    # Handle optional private key authentication
    private_key_section = ""
    if hasattr(connector.credentials, 'private_key') and connector.credentials.private_key:
        # Write private key to file
        private_key_path = profiles_dir / "snowflake_private_key.p8"
        private_key_value = connector.credentials.private_key.get_secret_value()

        # Write as binary if bytes, otherwise as text
        if isinstance(private_key_value, bytes):
            with open(private_key_path, 'wb') as f:
                f.write(private_key_value)
        else:
            with open(private_key_path, 'w') as f:
                f.write(private_key_value)

        logger.info(f"Private key written to {private_key_path}")

        private_key_section = f"""
      private_key_path: {private_key_path}"""
        if hasattr(connector.credentials, 'private_key_passphrase') and connector.credentials.private_key_passphrase:
            private_key_section += f"""
      private_key_passphrase: {connector.credentials.private_key_passphrase.get_secret_value()}"""

    profiles_yml_content = f"""tuva_snowflake:
  outputs:
    dev:
      type: snowflake
      account: {connector.credentials.account}
      user: {connector.credentials.user}
      password: {connector.credentials.password.get_secret_value() if connector.credentials.password else ''}
      role: {connector.credentials.role or 'PUBLIC'}
      database: {connector.database}
      warehouse: {connector.warehouse}
      schema: {connector.schema_}
      authenticator: {connector.credentials.authenticator}{private_key_section}
      threads: 4
      client_session_keep_alive: false
      query_tag: tuva
  target: dev
"""

    profiles_yml_path = profiles_dir / "profiles.yml"

    # Write the profiles.yml (overwrite if exists for clean setup)
    with open(profiles_yml_path, 'w') as f:
        f.write(profiles_yml_content)

    logger.info(f"Snowflake profile configured at {profiles_yml_path}")

    return profiles_dir


@task(log_prints=True)
def update_dbt_project_config(project_dir: Path) -> None:
    """Update dbt_project.yml to use the Snowflake profile."""
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
            lines[i] = 'profile: "tuva_snowflake"'
            break
    else:
        # If profile line doesn't exist, add it after the name line
        for i, line in enumerate(lines):
            if line.strip().startswith('name:'):
                lines.insert(i + 1, 'profile: "tuva_snowflake"')
                break

    # Write the updated content back
    with open(dbt_project_path, 'w') as f:
        f.write('\n'.join(lines))

    logger.info(f"Updated dbt_project.yml to use tuva_snowflake profile")


@task(log_prints=True)
def run_dbt_commands(commands: list[str], project_dir: Path, profiles_dir: Path) -> None:
    """Run dbt commands with Prefect integration."""
    logger = get_run_logger()
    logger.info(f"Running dbt commands: {commands}")

    settings = PrefectDbtSettings(
        project_dir=str(project_dir),
        profiles_dir=str(profiles_dir),
    )
    runner = PrefectDbtRunner(settings=settings, raise_on_failure=False)

    for command in commands:
        logger.info(f"Executing: dbt {command}")
        runner.invoke(command.split())
        logger.info(f"Completed: dbt {command}")


@flow(name="Tuva Health with Snowflake", log_prints=True)
def tuva_snowflake_flow(
    repo_url: str,
    connector_block_name: str,
    target_dir: Optional[str] = None
) -> dict:
    """
    Main Prefect flow to clone and run the Tuva Health with Snowflake.

    Args:
        repo_url: URL of the Tuva Health repository
        target_dir: Directory to clone the repository to (optional)
        connector_block_name: Name of the Snowflake connector block

    Returns:
        Dictionary with verification results and connection info
    """
    logger = get_run_logger()
    logger.info("Starting Tuva Health Flow with Snowflake")

    try:
        # Test Snowflake connection first
        connection_info = test_snowflake_connection(connector_block_name)

        # Convert string path to Path object if provided
        target_path = Path(target_dir) if target_dir and target_dir != "None" else None

        # Clone the repository
        project_dir = clone_repository(repo_url, target_path)

        # Set up Snowflake profile
        profiles_dir = setup_snowflake_profile(project_dir, connector_block_name)

        # Update dbt project configuration
        update_dbt_project_config(project_dir)

        # Run dbt commands
        # run_dbt_commands(["deps"], project_dir, profiles_dir)
        # run_dbt_commands(["debug"], project_dir, profiles_dir)
        # run_dbt_commands(["seed"], project_dir, profiles_dir)
        run_dbt_commands(["run"], project_dir, profiles_dir)
        run_dbt_commands(["test"], project_dir, profiles_dir)


        logger.info("Tuva Health Flow with Snowflake completed successfully")
        logger.info(f"Project directory: {project_dir}")

        return {
            "connection_info": connection_info,
            "project_directory": str(project_dir),
            "profiles_directory": str(profiles_dir)
        }

    except Exception as e:
        logger.error(f"Error during Tuva Snowflake run: {str(e)}")
        logger.info("\nTroubleshooting tips:")
        logger.info("1. Ensure 'prefect-snowflake' and 'prefect-dbt' are installed")
        logger.info(f"2. Verify connector block '{connector_block_name}' exists and is configured")
        logger.info("3. Check network connectivity to Snowflake")
        logger.info("4. Verify your Snowflake account permissions and schema access")
        logger.info("5. Ensure the database and schema specified in the connector exist")
        raise


if __name__ == "__main__":
    print("Running Tuva Health with Snowflake")
    print("=" * 60)

    # Run the flow
    result = tuva_snowflake_flow(
        repo_url="https://github.com/tuva-health/demo",
        connector_block_name="tuva-snowflake-conn"
    )

    print("\n" + "=" * 60)
    print("Demo completed!")
    if result:
        print(f"Summary: {result}")