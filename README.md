# data-eng-taxi-ibis-dagster

# NYC Taxi Data Pipeline 

**with Dagster, DuckDB, Spark, and Ibis (Data Engineering example)**

## 🚀 Overview

This project demonstrates a modern, backend-agnostic data pipeline using [Dagster](https://dagster.io/) for orchestration, [DuckDB](https://duckdb.org/) for fast local analytics, [Apache Spark](https://spark.apache.org/) for scalable distributed processing, and [Ibis](https://ibis-project.org/) for portable data transformations.
It ingests raw [NYC taxi trip data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) in Parquet format, persists it in DuckDB, exports a clean Parquet dataset, and runs identical analytics on both DuckDB and Spark backends.

______________________________________________________________________

## 🧭 Philosophy: Why, What, and How

### **Why**

- **Reproducibility**: Ensuring all compute engines operate on the same, well-defined data.
- **Portability**: Using Ibis for backend-agnostic transformation logic, so you can switch engines with minimal code changes.
- **Observability**: Leveraging Dagster for orchestration, lineage, and monitoring.
- **Performance**: Combining the speed of DuckDB for local analytics with the scalability of Spark for big data.

### **What**

- **Ingest**: Loads all raw Parquet files into a persistent DuckDB database.
- **Export**: Writes a clean, unified Parquet file from DuckDB.
- **Analyse**: Runs the same Ibis aggregation logic on both DuckDB and Spark.
- **Log**: Uses Loguru to report on the size of the exported Parquet file(s).

### **How**

- **Dagster assets** define each pipeline step and manage dependencies.
- **Ibis** expresses SQL-like logic in Python, portable across engines.
- **DuckDB** serves as a fast, local OLAP engine.
- **Spark** enables distributed analytics on the exported Parquet.
- **Loguru** provides rich, structured logging for pipeline observability.

______________________________________________________________________

## 🏗️ Pipeline Steps

1. **Ingest Parquet to DuckDB**
   Loads all NYC taxi Parquet files into a persistent DuckDB table.
1. **Export DuckDB Table to Parquet**
   Exports the unified DuckDB table to a single Parquet file, logging the output size.
1. **Analyse with DuckDB**
   Runs an Ibis query on the DuckDB table to answer this question:
   _"For trips with a fare over $50, what is the average fare by passenger count?"_
1. **Analyse with Spark**
   Runs the same Ibis query on the exported Parquet file using Spark.

______________________________________________________________________

## 📦 Setup & Usage

### **1. Install Dependencies**

```sh
uv add dagster dagster-webserver duckdb ibis-framework ibis-duckdb ibis-spark loguru pyarrow pandas
```

### **2. Prepare Data Directory**

Place your NYC taxi Parquet files in `../data-eng-taxi/seeds/`.

### **3. Run the Pipeline**

You can run assets individually in a Python session or orchestrate everything via Dagster:

```sh
dagster dev
```

Then visit [http://localhost:3000](http://localhost:3000) to materialise assets and view logs.

______________________________________________________________________

## 📝 Example Pipeline Code

See [taxi_pipeline_native.py](taxi_pipeline_native.py)

______________________________________________________________________

## 📊 Observability & Logging

- **Loguru** provides rich, timestamped logs for each pipeline stage.
- The size of the exported Parquet file is logged for traceability and optimisation.
- Dagster UI offers run history, asset lineage, and step timing.

______________________________________________________________________

## 🧠 Extending This Pipeline

- Partition data by month or region for scalable analytics.
- Add data quality checks or profiling as new assets.
- Integrate with cloud storage (S3, GCS) for distributed workflows.
- Parameterise thresholds, file paths, or aggregation logic for greater flexibility.

______________________________________________________________________

## 💡 Why This Pattern?

- **Unified Logic:** Write your data transformation once with Ibis, run it anywhere.
- **Reproducibility:** Every step, from ingestion to export to analytics, is tracked and repeatable.
- **Scalability:** Start local with DuckDB, scale out with Spark-no code rewrite needed.
- **Transparency:** Logging and orchestration provide full visibility into your data flow.

______________________________________________________________________

## 📚 References

- [Dagster Documentation](https://docs.dagster.io/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Ibis Documentation](https://ibis-project.org/docs/)
- [Loguru Documentation](https://loguru.readthedocs.io/)
- [NYC Taxi Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

______________________________________________________________________

**Happy data engineering!** 🚕✨
