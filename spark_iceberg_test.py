from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print("=== SPARK TEST ===")
print("Spark version:", spark.version)
print("Iceberg catalog:", spark.conf.get("spark.sql.catalog.retailx"))
print("Warehouse:", spark.conf.get("spark.sql.catalog.retailx.warehouse"))

spark.sql("CREATE NAMESPACE IF NOT EXISTS retailx.test")

spark.sql("""
CREATE TABLE IF NOT EXISTS retailx.test.spark_iceberg_test (
    id INT,
    name STRING
) USING iceberg
""")

spark.sql("""
INSERT INTO retailx.test.spark_iceberg_test VALUES
(1, 'Spark'),
(2, 'Iceberg'),
(3, 'MinIO')
""")

print("=== TABLE DATA ===")
spark.sql("SELECT * FROM retailx.test.spark_iceberg_test").show()

spark.stop()
