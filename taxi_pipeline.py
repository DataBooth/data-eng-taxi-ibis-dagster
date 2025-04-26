import time
from pathlib import Path

import duckdb
import ibis
from dagster import Definitions, asset
from dagster_duckdb import DuckDBResource
from loguru import logger

# Paths
DB_PATH = Path("data/nyc_taxi.duckdb")
PARQUET_DIR = Path("../data-eng-taxi/seeds")
RAW_PARQUET_PATTERN = str(PARQUET_DIR / "*.parquet")
EXPORT_PARQUET_PATH = Path("data/nyc_taxi_export.parquet")

# The following line creates a Dagster resource for managing DuckDB database connections.
duckdb_resource = DuckDBResource(database=str(DB_PATH))

# Using DuckDBResource ensures that:
# - Each Dagster asset gets a properly managed, isolated connection to the DuckDB database file.
# - Connections are automatically closed after use, preventing file lock conflicts.
# - You avoid concurrency issues that arise when multiple assets/processes access the same DuckDB file.
# - Dagster can inject this resource into any asset that declares a 'duckdb' argument.


@asset
def ingest_taxi_parquet_to_duckdb(duckdb: DuckDBResource):
    """
    Ingests all NYC taxi Parquet files into a persistent DuckDB database table.

    This asset reads all Parquet files matching the specified pattern from the source directory,
    and loads them into a DuckDB table named 'nyc_taxi'. If the table already exists, it is dropped
    and recreated to ensure a clean, reproducible ingest. The total number of rows ingested is logged.

    Args:
        duckdb (DuckDBResource): A Dagster resource that provides managed DuckDB connections.

    Returns:
        None
    """
    with duckdb.get_connection() as con:
        con.execute("DROP TABLE IF EXISTS nyc_taxi;")
        con.execute(f"""
            CREATE TABLE nyc_taxi AS
            SELECT * FROM read_parquet('{RAW_PARQUET_PATTERN}');
        """)
        row_count = con.execute("SELECT COUNT(*) FROM nyc_taxi;").fetchone()[0]
        logger.info(f"Total rows loaded into DuckDB: {row_count}")


@asset(deps=["ingest_taxi_parquet_to_duckdb"])
def export_nyc_taxi_to_parquet(duckdb: DuckDBResource):
    """
    Exports the DuckDB 'nyc_taxi' table to a Parquet file and logs its file size.

    This asset connects to the persistent DuckDB database, exports the entire 'nyc_taxi' table
    to a single Parquet file at the specified location, and logs the resulting file size in megabytes.
    This step ensures that downstream consumers (e.g., Spark) operate on a clean, unified dataset.

    Args:
        duckdb (DuckDBResource): A Dagster resource that provides managed DuckDB connections.

    Returns:
        None
    """
    with duckdb.get_connection() as con:
        con.execute(f"""
            COPY nyc_taxi TO '{EXPORT_PARQUET_PATH}' (FORMAT PARQUET, OVERWRITE TRUE);
        """)
    file_size = EXPORT_PARQUET_PATH.stat().st_size
    size_mb = file_size / (1024 * 1024)
    logger.info(f"Exported DuckDB table to {EXPORT_PARQUET_PATH} ({size_mb:.2f} MB)")


def get_ibis_table_from_table(con, table_name):
    """
    Returns an Ibis expression that filters and aggregates NYC taxi data.

    This function connects to a table (by name) using the provided Ibis connection,
    filters for trips with fare amounts greater than $50, and computes the average fare
    grouped by passenger count. The returned expression can be executed on any supported
    Ibis backend (e.g., DuckDB, Spark).

    Args:
        con: An Ibis backend connection object.
        table_name (str): The name of the table to query.

    Returns:
        ibis.expr.types.Table: An Ibis table expression for the specified aggregation.
    """
    t = con.table(table_name)
    return (
        t.filter(t.fare_amount > 50)
        .group_by(t.passenger_count)
        .aggregate(avg_fare=t.fare_amount.mean())
    )


@asset(deps=["ingest_taxi_parquet_to_duckdb"])
def duckdb_asset(duckdb: DuckDBResource):
    """
    Executes an Ibis query on the persistent DuckDB table and returns the result.

    This asset connects to the DuckDB database using Ibis, applies a transformation that filters
    for trips with fare amounts greater than $50, groups by passenger count, and computes the average fare.
    The result is returned as a pandas DataFrame. The asset also logs the execution time for performance monitoring.

    Args:
        duckdb (DuckDBResource): Dagster resource for managed DuckDB connections.

    Returns:
        pandas.DataFrame: The aggregated result of the Ibis query.
    """
    start = time.perf_counter()
    with duckdb.get_connection() as con:
        ibis_con = ibis.duckdb.connect(con=con)
        expr = get_ibis_table_from_table(ibis_con, "nyc_taxi")
        result = expr.execute()
    logger.info(f"DuckDB asset took {time.perf_counter() - start:.2f} seconds")
    return result


@asset(deps=["export_nyc_taxi_to_parquet"])
def spark_asset():
    """
    Executes an Ibis query on the exported Parquet file using the PySpark backend.

    This asset connects to the exported Parquet file using Ibis with the PySpark backend,
    applies the same transformation as the DuckDB asset (filtering for fare amounts greater than $50,
    grouping by passenger count, and computing the average fare), and returns the result as a pandas DataFrame.
    Execution time is logged for observability.

    Returns:
        pandas.DataFrame: The aggregated result of the Ibis query on Spark.
    """
    start = time.perf_counter()
    con = ibis.pyspark.connect()
    t = con.read_parquet(str(EXPORT_PARQUET_PATH))
    expr = (
        t.filter(t.fare_amount > 50)
        .group_by(t.passenger_count)
        .aggregate(avg_fare=t.fare_amount.mean())
    )
    result = expr.execute()
    logger.info(f"Spark asset took {time.perf_counter() - start:.2f} seconds")
    return result


defs = Definitions(
    assets=[
        ingest_taxi_parquet_to_duckdb,
        export_nyc_taxi_to_parquet,
        duckdb_asset,
        spark_asset,
    ],
    resources={
        "duckdb": duckdb_resource,
    },
)
