from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower

spark = SparkSession.builder.getOrCreate()

source = "retailx.bronze.payments"
target = "retailx.silver.payments"

df = spark.table(source)

silver_df = (
    df
    .dropDuplicates(["payment_id"])
    .filter(col("payment_id").isNotNull())
    .filter(col("order_id").isNotNull())
    .withColumn("payment_method", lower(trim("payment_method")))
    .withColumn("payment_status", lower(trim("payment_status")))
)

silver_df.writeTo(target).using("iceberg").createOrReplace()

print("=== SILVER PAYMENTS CREATED ===")

spark.sql(f"SELECT COUNT(*) AS row_count FROM {target}").show()

spark.sql(f"SELECT * FROM {target} LIMIT 10").show(truncate=False)

spark.stop()
