# Prefect Demo Project

This project contains Prefect flows that demonstrate data platform integrations with both process-based and Docker deployment options:

1. **Tuva Health Demo**: Clones and runs the [Tuva Health demo repository](https://github.com/tuva-health/demo) using DuckDB
2. **DuckDB Demo**: Downloads and runs a basic dbt example project with DuckDB
3. **Snowflake Demo**: Demonstrates connecting to Snowflake using Prefect credentials blocks

## Prerequisites

- Python 3.11+ (recommended)
- Git
- Docker (for Docker deployments)
- dbt CLI (installed via requirements.txt)

## Installation

### Option 1: Local Python Environment

1. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Make sure you have `git` available in your PATH.

### Option 2: Docker Environment

1. Build the Docker image:
   ```bash
   docker build -t prefect-tuva-demo:latest .
   ```

2. Or let Prefect build it automatically during deployment.

## Usage

### Running the Flows

You can run the flows in several ways:

#### 1. Run directly as Python scripts:

**Tuva Health Demo:**
```bash
python src/flows/tuva_demo.py
```

**DuckDB Demo:**
```bash
python src/flows/duckdb_demo.py
```

**Snowflake Demo:**
```bash
python snowflake_demo.py
```

#### 2. Run with custom parameters:

**Tuva Health Demo:**
```python
from src.flows.tuva_demo import tuva_demo_flow

result = tuva_demo_flow(
    repo_url="https://github.com/tuva-health/demo",
    target_dir="./my-tuva-demo",  # Optional: specify where to clone
    db_path="./my-database.duckdb"  # Optional: specify database location
)
```

**Snowflake Demo:**
```python
from snowflake_demo import snowflake_demo_flow

# Uses your configured "tuva-snowflake-conn" connector block
result = snowflake_demo_flow()
```

#### 3. Deploy and run with Prefect server:

##### Process-based Deployments:
```bash
# Start Prefect server
prefect server start

# In another terminal, deploy process-based flows
prefect deploy duckdb_demo_process
prefect deploy tuva_demo_process

# Create and start a process work pool
prefect work-pool create my-process-work-pool --type process
prefect worker start --pool my-process-work-pool

# Run deployments
prefect deployment run "DuckDB Demo/duckdb_demo_process"
prefect deployment run "Tuva Health Demo/tuva_demo_process"
```

##### Docker-based Deployments:
```bash
# Start Prefect server
prefect server start

# In another terminal, deploy Docker-based flows (builds image automatically)
prefect deploy duckdb_demo_docker
prefect deploy tuva_demo_docker

# Create and start a Docker work pool
prefect work-pool create my-docker-work-pool --type docker
prefect worker start --pool my-docker-work-pool

# Run deployments
prefect deployment run "DuckDB Demo/duckdb_demo_docker"
prefect deployment run "Tuva Health Demo/tuva_demo_docker"
```

##### Deploy all at once:
```bash
prefect deploy --all
```

## What the Flows Do

### Tuva Health Demo Flow
1. **Clone Repository**: Downloads the Tuva Health demo repository from GitHub to `./dbt/tuva_demo/`
2. **Setup DuckDB**: Creates a local DuckDB database at `./data/tuva_demo.duckdb` and configures dbt profiles in `./config/`
3. **Configure dbt**: Updates the dbt project configuration to use the DuckDB profile
4. **Install Dependencies**: Runs `dbt deps` to install the Tuva Project package
5. **Run Transformations**: Executes `dbt run` to create all models
6. **Verify Results**: Checks the created tables and provides a summary

### DuckDB Demo Flow
1. **Download Project**: Downloads a sample dbt project from the Prefect examples repository
2. **Extract Project**: Extracts the dbt project to `./dbt/duckdb_demo/`
3. **Setup DuckDB**: Creates a local DuckDB database at `./data/demo.duckdb`
4. **Install Dependencies**: Runs `dbt deps` to install required packages
5. **Seed Data**: Runs `dbt seed` to load sample data
6. **Run Models**: Executes `dbt run` to create all models
7. **Test Models**: Runs `dbt test` to validate data quality

### Snowflake Demo Flow
1. **Load Connector**: Loads the configured Snowflake connector block "tuva-snowflake-conn"
2. **Test Connection**: Verifies connectivity and displays current user/timestamp
3. **Run Sample Queries**: Executes sample SQL queries to demonstrate data retrieval
4. **DataFrame Integration**: Shows how to query Snowflake data into pandas DataFrames
5. **Connection Management**: Demonstrates proper connection handling and cleanup

## Output

### Tuva Health Demo Flow
The flow returns a dictionary containing:
- `project_directory`: Path where the repository was cloned
- `database_path`: Path to the DuckDB database file
- `profiles_directory`: Path to the dbt profiles directory
- `verification_results`: Summary of tables created and row counts

### DuckDB Demo Flow
The flow outputs the path to the created DuckDB database with sample dbt transformations.

### Snowflake Demo Flow
The flow returns a dictionary containing:
- `connection_test`: Current timestamp and user information from Snowflake
- `query_results`: Results from sample queries
- `dataframe_shape`: Dimensions of the created pandas DataFrame

## Files Created

- **Repository Clones**:
  - `./dbt/tuva_demo/` - Tuva Health demo project files
  - `./dbt/duckdb_demo/` - DuckDB demo project files
- **DuckDB Databases**:
  - `./data/tuva_demo.duckdb` - Healthcare data transformations
  - `./data/demo.duckdb` - Basic demo transformations
- **dbt Profiles**: Local configuration files in `./config/` directory
- **Demo Scripts**:
  - `snowflake_demo.py` - Standalone Snowflake connection demo

## Deployment Types

This project supports two deployment types:

### Process-based Deployments
- **Names**: `duckdb_demo_process`, `tuva_demo_process`
- **Work Pool**: `my-process-work-pool`
- **Best for**: Development, local testing, direct Python execution
- **Requirements**: Local Python environment with all dependencies

### Docker-based Deployments
- **Names**: `duckdb_demo_docker`, `tuva_demo_docker`
- **Work Pool**: `my-docker-work-pool`
- **Best for**: Production, isolated environments, consistent execution
- **Requirements**: Docker installed and running
- **Features**:
  - Automatic image building via `prefect deploy`
  - Isolated execution environment
  - Consistent Python/system dependencies

### Standalone Demos
- **Snowflake Demo**: Direct Python execution without Prefect deployments
- **Requirements**: Configured Snowflake connector block named "tuva-snowflake-conn"

## Project Structure

```
prefect_test/
├── src/flows/              # Prefect flow definitions
│   ├── __init__.py        # Package initialization
│   ├── tuva_demo.py       # Tuva Health demo flow
│   ├── duckdb_demo.py     # Basic dbt/DuckDB demo flow
│   └── snowflake_demo.py  # Snowflake integration demo flow
├── config/                 # dbt profiles configuration
├── data/                  # DuckDB database files
├── dbt/                   # dbt project directories
│   ├── tuva_demo/         # Cloned Tuva Health demo
│   └── duckdb_demo/       # Downloaded DuckDB demo project
├── snowflake_demo.py      # Standalone Snowflake demo script
├── Dockerfile             # Docker image definition
├── .dockerignore          # Docker build exclusions
├── prefect.yaml           # Prefect deployment configuration
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

## Troubleshooting

### General Issues
- Ensure you have sufficient disk space for repositories and databases
- Make sure `git` is installed and available in your PATH
- Verify that `prefect`, `dbt-core`, `dbt-duckdb`, `prefect-dbt`, and `prefect-snowflake` are properly installed
- Check that you have write permissions in the project directories
- If deployment fails, ensure Prefect server is running: `prefect server start`
- For Windows users, ensure proper path handling in command line

### Snowflake-specific Issues
- **Connector block not found**: Create the Snowflake connector block "tuva-snowflake-conn" in Prefect UI or via code
- **Connection timeouts**: Check network connectivity to Snowflake and firewall settings
- **Authentication errors**: Verify credentials in the connector block are correct and have necessary permissions
- **Missing dependencies**: Ensure `prefect-snowflake` is installed: `pip install prefect-snowflake`

### Docker-specific Issues
- **Docker not found**: Ensure Docker is installed and running
- **Permission denied**: Make sure Docker daemon is running and accessible
- **Image build failures**: Check that all files in the project are accessible (not locked)
- **Worker connection issues**: Ensure `PREFECT_API_URL` environment variable points to the correct server
- **Volume mounting**: Docker deployments may have different file system access patterns

### Work Pool Issues
- **Work pool doesn't exist**: Create work pools before starting workers:
  ```bash
  prefect work-pool create my-process-work-pool --type process
  prefect work-pool create my-docker-work-pool --type docker
  ```
- **No workers available**: Ensure at least one worker is running for the deployment's work pool
- **Wrong work pool type**: Match deployment work pool with worker work pool type

## Quick Start Commands

```bash
# Complete setup for process deployments
prefect server start &
prefect work-pool create my-process-work-pool --type process
prefect deploy duckdb_demo_process tuva_demo_process
prefect worker start --pool my-process-work-pool &
prefect deployment run "DuckDB Demo/duckdb_demo_process"

# Complete setup for Docker deployments
prefect server start &
prefect work-pool create my-docker-work-pool --type docker
prefect deploy duckdb_demo_docker tuva_demo_docker
prefect worker start --pool my-docker-work-pool &
prefect deployment run "DuckDB Demo/duckdb_demo_docker"
```