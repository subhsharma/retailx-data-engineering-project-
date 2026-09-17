from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

source = "retailx.bronze.order_items"
target = "retailx.silver.order_items"

df = spark.table(source)

silver_df = (
    df
    .dropDuplicates(["order_item_id"])
    .filter(col("order_item_id").isNotNull())
    .filter(col("order_id").isNotNull())
    .filter(col("product_id").isNotNull())
)

silver_df.writeTo(target).using("iceberg").createOrReplace()

print("=== SILVER ORDER ITEMS CREATED ===")

spark.sql(f"SELECT COUNT(*) AS row_count FROM {target}").show()

spark.sql(f"SELECT * FROM {target} LIMIT 10").show(truncate=False)

spark.stop()
