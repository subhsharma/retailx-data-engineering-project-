from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, lower

spark = SparkSession.builder.getOrCreate()

source = "retailx.bronze.customers"
target = "retailx.silver.customers"

df = spark.table(source)

silver_df = (
    df
    .dropDuplicates(["customer_id"])
    .withColumn("first_name", trim("first_name"))
    .withColumn("last_name", trim("last_name"))
    .withColumn("email", lower(trim("email")))
    .withColumn("city", trim("city"))
)

silver_df.writeTo(target).using("iceberg").createOrReplace()

print("=== SILVER CUSTOMERS CREATED ===")
spark.sql(f"SELECT COUNT(*) AS row_count FROM {target}").show()
spark.sql(f"SELECT * FROM {target} LIMIT 10").show(truncate=False)

spark.stop()
