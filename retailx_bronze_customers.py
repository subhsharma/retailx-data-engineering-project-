from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print("=== RETAILX BRONZE INGESTION ===")

# Read processed customers CSV from MinIO
df = spark.read.option("header", "true").option("inferSchema", "true").csv(
    "s3a://retailx/processed/customers.csv"
)

print("Source rows:", df.count())
print("Source columns:", df.columns)

# Create Bronze namespace
spark.sql("CREATE NAMESPACE IF NOT EXISTS retailx.bronze")

# Write as Iceberg table
df.writeTo("retailx.bronze.customers").using("iceberg").createOrReplace()

print("=== BRONZE TABLE CREATED ===")

spark.sql("SELECT * FROM retailx.bronze.customers LIMIT 10").show()

spark.stop()
