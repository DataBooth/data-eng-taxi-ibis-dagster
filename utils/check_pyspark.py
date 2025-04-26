# Check PySpark installation and basic functionality
from pyspark.sql import SparkSession


def main():
    try:
        spark = (
            SparkSession.builder.appName("Installation Test")
            .config("spark.driver.host", "192.168.0.12")
            .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
            .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
            .getOrCreate()
        )

        data = [("Test", 1)]
        df = spark.createDataFrame(data, ["Label", "Value"])

        print("\nPySpark operational!")
        print(
            f"Java version: {spark._jvm.java.lang.System.getProperty('java.version')}"
        )
        print(f"Spark version: {spark.version}")
        df.show()
        spark.stop()
    except Exception as e:
        print(f"Critical failure: {str(e)}")
        print("Check Java version (recommend Java 17) and Spark configuration")


if __name__ == "__main__":
    main()
