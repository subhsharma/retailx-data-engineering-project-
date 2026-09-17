from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

datasets = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "stores"
]

spark.sql("CREATE NAMESPACE IF NOT EXISTS retailx.bronze")

for dataset in datasets:
    print(f"\n=== PROCESSING {dataset.upper()} ===")

    source = f"s3a://retailx/processed/{dataset}.csv"
    table = f"retailx.bronze.{dataset}"

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(source)
    )

    print(f"Source rows: {df.count()}")

    df.writeTo(table).using("iceberg").createOrReplace()

    count = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]

    print(f"Bronze table: {table}")
    print(f"Bronze rows: {count}")

print("\n=== ALL BRONZE TABLES CREATED ===")

spark.sql("SHOW TABLES IN retailx.bronze").show(truncate=False)

spark.stop()
