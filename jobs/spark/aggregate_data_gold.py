from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, max as spark_max,
    current_date, round as spark_round,
)
import os
from dotenv import load_dotenv

load_dotenv()

MINIO_CONF = {
    "endpoint": os.getenv("MINIO_ENDPOINT"), 
    "access_key": os.getenv("MINIO_ACCESS_KEY"),
    "secret_key": os.getenv("MINIO_SECRET_KEY")
}

SOURCE_TABLE = "demo.silver.jobs"

def create_spark_session() -> SparkSession:
    packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]
    return (
        SparkSession.builder
        .appName("JobHunter_Gold_Aggregation")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.demo.type", "hive")
        .config("spark.sql.catalog.demo.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.demo.warehouse", "s3a://warehouse/")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"])
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"])
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

def save_to_iceberg(spark, df, table_name: str, partition_col: str, merge_keys: list):
    print(f"📊 Upserting into {table_name} ...")

    namespace = ".".join(table_name.split(".")[:2])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    temp_view = f"temp_{table_name.replace('.', '_')}"
    df.createOrReplaceTempView(temp_view)

    schema_ddl = ", ".join([
        f"{field.name} {field.dataType.simpleString().upper()}"
        for field in df.schema.fields
    ])

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema_ddl}
        )
        USING iceberg
        PARTITIONED BY ({partition_col})
    """)

    merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_keys])
    update_cols = [c for c in df.columns if c not in merge_keys]

    if not update_cols:
        merge_sql = f"""
            MERGE INTO {table_name} t
            USING {temp_view} s
            ON {merge_condition}
            WHEN NOT MATCHED THEN INSERT *
        """
    else:
        update_set = ", ".join([f"t.{c} = s.{c}" for c in update_cols])
        merge_sql = f"""
            MERGE INTO {table_name} t
            USING {temp_view} s
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT *
        """

    spark.sql(merge_sql)
    result_count = spark.table(table_name).count()
    print(f"   ✅ {table_name} now contains {result_count} row(s).")


def create_gold_aggregate():
    spark = create_spark_session()

    print(f"📥 Loading silver data: {SOURCE_TABLE}")
    try:
        df_silver = spark.table(SOURCE_TABLE)
    except Exception as e:
        print(f"❌ Error loading {SOURCE_TABLE}: {e}")
        return

    row_count = df_silver.count()
    print(f"   Loaded {row_count} job(s) from silver layer.")

    if row_count == 0:
        print("⚠️  No data in silver — skipping gold aggregation.")
        return

    df_silver = df_silver.withColumn("report_date", current_date())

    # ── ITviec jobs (cho Discord Bot !job_de) ────────────────────────────────
    print("\n🔍 Aggregating ITviec jobs...")
    df_itviec = (
        df_silver
        .filter(col("source") == "itviec")
        .select(
            col("keyword"),
            col("title"),
            col("url"),
            col("company"),
            col("location_std").alias("location"),
            col("work_type"),
            col("salary_raw").alias("salary"),
            col("tags"),
            col("posted"),
            col("report_date"),
        )
    )

    itviec_count = df_itviec.count()
    print(f"   Found {itviec_count} ITviec job(s).")

    if itviec_count > 0:
        save_to_iceberg(
            spark, df_itviec,
            "demo.gold.itviec_jobs",
            "report_date",
            ["url", "report_date"],
        )
        df_itviec.show(5, truncate=False)

    # ── Market summary ───────────────────────────────────────────────────────
    print("\n📈 Aggregating market summary ...")
    df_market = (
        df_silver
        .filter(col("min_salary").isNotNull())
        .groupBy("location_std", "currency", "report_date")
        .agg(
            count("url").alias("total_jobs"),
            spark_round(avg("min_salary"), 2).alias("avg_min_salary"),
            spark_round(avg("max_salary"), 2).alias("avg_max_salary"),
            spark_max("max_salary").alias("highest_salary"),
        )
    )
    save_to_iceberg(spark, df_market, "demo.gold.market_summary", "report_date",
                    ["location_std", "currency", "report_date"])
    df_market.orderBy(col("total_jobs").desc()).show(10, truncate=False)

    # ── Source stats ─────────────────────────────────────────────────────────
    print("\n📊 Aggregating source stats ...")
    df_source = (
        df_silver
        .groupBy("source", "report_date")
        .agg(count("url").alias("jobs_count"))
    )
    save_to_iceberg(spark, df_source, "demo.gold.source_stats", "report_date",
                    ["source", "report_date"])
    df_source.orderBy(col("jobs_count").desc()).show(10, truncate=False)

    # ── Daily alerts ─────────────────────────────────────────────────────────
    print("\n🚨 Filtering high-value job alerts ...")
    df_alerts = (
        df_silver
        .filter(
            ((col("currency") == "USD") & (col("min_salary") >= 1000)) |
            ((col("currency") == "VND") & (col("min_salary") >= 20_000_000))
        )
        .select("title", "url", "min_salary", "max_salary",
                "currency", "source", "location_std", "report_date")
    )

    alert_count = df_alerts.count()
    print(f"   Found {alert_count} high-value job(s) today.")

    if alert_count > 0:
        save_to_iceberg(spark, df_alerts, "demo.gold.daily_alerts", "report_date",
                        ["url", "report_date"])

    print("\n✅ GOLD AGGREGATION COMPLETE!")


if __name__ == "__main__":
    create_gold_aggregate()