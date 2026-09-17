from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

source = "retailx.bronze.orders"
target = "retailx.silver.orders"

df = spark.table(source)

silver_df = (
    df
    .dropDuplicates(["order_id"])
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
)

silver_df.writeTo(target).using("iceberg").createOrReplace()

print("=== SILVER ORDERS CREATED ===")

spark.sql(f"SELECT COUNT(*) AS row_count FROM {target}").show()

spark.sql(f"SELECT * FROM {target} LIMIT 10").show(truncate=False)

spark.stop()
