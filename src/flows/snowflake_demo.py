#!/usr/bin/env python3
"""
Demo script for connecting to Snowflake using Prefect connector block.

Requirements:
- prefect-snowflake library installed
- Snowflake connector block named "tuva-snowflake-conn" configured in Prefect

Usage:
    python snowflake_demo.py

Note: The connector block should already include database, warehouse, and schema configuration.
"""

from prefect import flow, task
from prefect_snowflake import SnowflakeConnector
import pandas as pd


@task
def test_snowflake_connection(connector_block_name: str):
    """Test the Snowflake connection using the connector block."""
    print(f"Loading Snowflake connector from block: {connector_block_name}")

    # Load the connector block
    connector = SnowflakeConnector.load(connector_block_name)

    with connector as conn:
        print("Successfully connected to Snowflake!")

        # Test query - get current timestamp and user
        result = conn.fetch_one("SELECT CURRENT_TIMESTAMP(), CURRENT_USER()")
        print(f"Current timestamp: {result[0]}")
        print(f"Current user: {result[1]}")

        return result


@task
def run_sample_query(connector_block_name: str):
    """Run a sample query to demonstrate data retrieval."""
    print("Running sample query...")

    connector = SnowflakeConnector.load(connector_block_name)

    with connector as conn:
        # Sample query - adjust table/schema names as needed for your environment
        query = """
        SELECT
            'Sample Data' as source,
            CURRENT_TIMESTAMP() as query_time,
            1 as test_value
        UNION ALL
        SELECT
            'More Sample Data' as source,
            CURRENT_TIMESTAMP() as query_time,
            2 as test_value
        """

        # Fetch all results
        results = conn.fetch_all(query)

        print(f"Query returned {len(results)} rows:")
        for row in results:
            print(f"  {row}")

        return results


@task
def query_to_dataframe(connector_block_name: str):
    """Demonstrate querying Snowflake data into a pandas DataFrame."""
    print("Querying data into pandas DataFrame...")

    connector = SnowflakeConnector.load(connector_block_name)

    with connector as conn:
        # Get connection for pandas
        connection = conn.get_connection()

        # Use pandas to read directly from Snowflake
        query = """
        SELECT
            'DataFrame Test' as description,
            CURRENT_DATE() as date_col,
            RANDOM() as random_value,
            ROW_NUMBER() OVER (ORDER BY RANDOM()) as row_num
        FROM TABLE(GENERATOR(ROWCOUNT=>5))
        """

        df = pd.read_sql(query, connection)
        print("DataFrame contents:")
        print(df)
        print(f"\nDataFrame shape: {df.shape}")

        return df


@flow
def snowflake_demo_flow():
    """Main flow to demonstrate Snowflake connectivity."""
    connector_block_name = "tuva-snowflake-conn"

    print(f"Starting Snowflake demo using connector block: {connector_block_name}")

    try:
        # Test basic connection
        connection_result = test_snowflake_connection(connector_block_name)

        # Run sample query
        query_results = run_sample_query(connector_block_name)

        # Query to DataFrame
        df_result = query_to_dataframe(connector_block_name)

        print("All Snowflake operations completed successfully!")
        return {
            "connection_test": connection_result,
            "query_results": query_results,
            "dataframe_shape": df_result.shape
        }

    except Exception as e:
        print(f"Error during Snowflake demo: {str(e)}")
        print("\nTroubleshooting tips:")
        print("1. Ensure 'prefect-snowflake' is installed: pip install prefect-snowflake")
        print(f"2. Verify connector block '{connector_block_name}' exists and is configured")
        print("3. Check network connectivity to Snowflake")
        print("4. Verify your Snowflake account permissions")
        raise


if __name__ == "__main__":
    print("Running Snowflake Demo")
    print("=" * 50)

    # Run the flow
    result = snowflake_demo_flow()

    print("\n" + "=" * 50)
    print("Demo completed!")
    if result:
        print(f"Summary: {result}")