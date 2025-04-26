import time
from pathlib import Path
from loguru import logger

import duckdb
from dagster import Definitions, asset
from dagster_duckdb import DuckDBResource
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

# Paths
DB_PATH = Path("data/nyc_taxi.duckdb")
PARQUET_DIR = Path("../data-eng-taxi/seeds")
RAW_PARQUET_PATTERN = str(PARQUET_DIR / "*.parquet")
EXPORT_PARQUET_PATH = Path("data/nyc_taxi_export.parquet")

# This line creates a Dagster resource for managing DuckDB database connections.
# Using DuckDBResource ensures that:
# - Each Dagster asset gets a properly managed, isolated connection to the DuckDB database file.
# - Connections are automatically closed after use, preventing file lock conflicts.
# - You avoid concurrency issues that arise when multiple assets/processes access the same DuckDB file.
# - Dagster can inject this resource into any asset that declares a 'duckdb' argument.
duckdb_resource = DuckDBResource(database=str(DB_PATH))


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
    with duckdb_resource.get_connection() as con:
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
    with duckdb_resource.get_connection() as con:
        con.execute(f"""
            COPY nyc_taxi TO '{EXPORT_PARQUET_PATH}' (FORMAT PARQUET, OVERWRITE TRUE);
        """)
    file_size = EXPORT_PARQUET_PATH.stat().st_size
    size_mb = file_size / (1024 * 1024)
    logger.info(f"Exported DuckDB table to {EXPORT_PARQUET_PATH} ({size_mb:.2f} MB)")


@asset(compute_kind="python", deps=["ingest_taxi_parquet_to_duckdb"])
def duckdb_asset(duckdb: DuckDBResource):
    """
    Queries the DuckDB table directly using DuckDB's Python API.

    This asset connects to the DuckDB database, executes a SQL query to calculate the average
    fare amount grouped by passenger count for trips with a fare amount greater than $50,
    and returns the result as a pandas DataFrame. Execution time is logged for observability.

    Args:
        duckdb (DuckDBResource): A Dagster resource that provides managed DuckDB connections.

    Returns:
        pandas.DataFrame: The aggregated result of the DuckDB query.
    """
    start = time.perf_counter()
    with duckdb_resource.get_connection() as con:
        result = con.execute("""
            SELECT passenger_count, AVG(fare_amount) AS avg_fare
            FROM nyc_taxi
            WHERE fare_amount > 50
            GROUP BY passenger_count
        """).df()
    logger.info(f"DuckDB asset took {time.perf_counter() - start:.2f} seconds")
    return result


@asset(compute_kind="pyspark", deps=["export_nyc_taxi_to_parquet"])
def spark_asset():
    """
    Queries the exported Parquet file using PySpark.

    This asset reads the Parquet file using PySpark, applies a transformation that filters
    for trips with fare amounts greater than $50, groups by passenger count, and computes the average fare.
    The result is returned as a pandas DataFrame. Execution time is logged for observability.

    Returns:
        pandas.DataFrame: The aggregated result of the Spark query.
    """
    start = time.perf_counter()
    try:
        spark = (
            SparkSession.builder.appName("TaxiAnalysis")
            .config(
                "spark.driver.host", "192.168.0.12"
            )  # Replace with your driver host if needed
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
            .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
            .getOrCreate()
        )
        df = spark.read.parquet(f"file://{EXPORT_PARQUET_PATH.resolve()}")
        result_df = (
            df.filter(col("fare_amount") > 50)
            .groupBy("passenger_count")
            .agg(avg("fare_amount").alias("avg_fare"))
            .toPandas()
        )
        spark.stop()
        logger.info(f"Spark asset took {time.perf_counter() - start:.2f} seconds")
        return result_df
    except Exception as e:
        logger.error(f"Spark asset failed: {e}")
        raise


@asset(compute_kind="pyspark", deps=["export_nyc_taxi_to_parquet"])
def spark_sql_asset():
    """
    Queries the exported Parquet file using Spark SQL.

    This asset reads the Parquet file using PySpark, registers it as a temporary view,
    and then executes a SQL query to calculate the average fare amount grouped by passenger count
    for trips with a fare amount greater than $50. The result is returned as a pandas DataFrame.
    Execution time is logged for observability.

    Returns:
        pandas.DataFrame: The aggregated result of the Spark SQL query.
    """
    start = time.perf_counter()
    try:
        spark = (
            SparkSession.builder.appName("TaxiAnalysis")
            .config(
                "spark.driver.host", "192.168.0.12"
            )  # Replace with your driver host if needed
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
            .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
            .getOrCreate()
        )
        df = spark.read.parquet(str(EXPORT_PARQUET_PATH.resolve()))
        df.createOrReplaceTempView("nyc_taxi")
        result_df = spark.sql("""
            SELECT passenger_count, AVG(fare_amount) AS avg_fare
            FROM nyc_taxi
            WHERE fare_amount > 50
            GROUP BY passenger_count
        """).toPandas()
        spark.stop()
        logger.info(f"Spark SQL asset took {time.perf_counter() - start:.2f} seconds")
        return result_df
    except Exception as e:
        logger.error(f"Spark SQL asset failed: {e}")
        raise


defs = Definitions(
    assets=[
        ingest_taxi_parquet_to_duckdb,
        export_nyc_taxi_to_parquet,
        duckdb_asset,
        spark_asset,
        spark_sql_asset,
    ],
    resources={
        "duckdb": duckdb_resource,
    },
)
