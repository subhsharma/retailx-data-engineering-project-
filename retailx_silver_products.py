from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, lower, col

spark = SparkSession.builder.getOrCreate()

source = "retailx.bronze.products"
target = "retailx.silver.products"

df = spark.table(source)

silver_df = (
    df
    .dropDuplicates(["product_id"])
    .withColumn("product_name", trim("product_name"))
    .withColumn("category", lower(trim("category")))
)

silver_df.writeTo(target).using("iceberg").createOrReplace()

print("=== SILVER PRODUCTS CREATED ===")

spark.sql(f"SELECT COUNT(*) AS row_count FROM {target}").show()

spark.sql(f"SELECT * FROM {target} LIMIT 10").show(truncate=False)

spark.stop()
