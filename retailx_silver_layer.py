from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower

spark = SparkSession.builder.getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS retailx.silver")

print("\n" + "=" * 60)
print("             RETAILX SILVER LAYER")
print("=" * 60)


# ============================================================
# 1. CUSTOMERS
# ============================================================
print("\n[1/6] Processing CUSTOMERS...")

customers = spark.table("retailx.bronze.customers")

silver_customers = (
    customers
    .dropDuplicates(["customer_id"])
    .filter(col("customer_id").isNotNull())
    .withColumn("first_name", trim("first_name"))
    .withColumn("last_name", trim("last_name"))
    .withColumn("email", lower(trim("email")))
    .withColumn("city", trim("city"))
)

silver_customers.writeTo(
    "retailx.silver.customers"
).using("iceberg").createOrReplace()

print(
    "CUSTOMERS:",
    spark.sql(
        "SELECT COUNT(*) FROM retailx.silver.customers"
    ).collect()[0][0],
    "rows"
)


# ============================================================
# 2. PRODUCTS
# ============================================================
print("\n[2/6] Processing PRODUCTS...")

products = spark.table("retailx.bronze.products")

silver_products = (
    products
    .dropDuplicates(["product_id"])
    .filter(col("product_id").isNotNull())
    .withColumn("product_name", trim("product_name"))
    .withColumn("category", lower(trim("category")))
)

silver_products.writeTo(
    "retailx.silver.products"
).using("iceberg").createOrReplace()

print(
    "PRODUCTS:",
    spark.sql(
        "SELECT COUNT(*) FROM retailx.silver.products"
    ).collect()[0][0],
    "rows"
)


# ============================================================
# 3. ORDERS
# ============================================================
print("\n[3/6] Processing ORDERS...")

orders = spark.table("retailx.bronze.orders")

silver_orders = (
    orders
    .dropDuplicates(["order_id"])
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
)

silver_orders.writeTo(
    "retailx.silver.orders"
).using("iceberg").createOrReplace()

print(
    "ORDERS:",
    spark.sql(
        "SELECT COUNT(*) FROM retailx.silver.orders"
    ).collect()[0][0],
    "rows"
)


# ============================================================
# 4. ORDER ITEMS
# ============================================================
print("\n[4/6] Processing ORDER ITEMS...")

order_items = spark.table("retailx.bronze.order_items")

silver_order_items = (
    order_items
    .dropDuplicates(["order_item_id"])
    .filter(col("order_item_id").isNotNull())
    .filter(col("order_id").isNotNull())
    .filter(col("product_id").isNotNull())
)

silver_order_items.writeTo(
    "retailx.silver.order_items"
).using("iceberg").createOrReplace()

print(
    "ORDER ITEMS:",
    spark.sql(
        "SELECT COUNT(*) FROM retailx.silver.order_items"
    ).collect()[0][0],
    "rows"
)


# ============================================================
# 5. PAYMENTS
# ============================================================
print("\n[5/6] Processing PAYMENTS...")

payments = spark.table("retailx.bronze.payments")

silver_payments = (
    payments
    .dropDuplicates(["payment_id"])
    .filter(col("payment_id").isNotNull())
    .filter(col("order_id").isNotNull())
    .withColumn("payment_method", lower(trim("payment_method")))
    .withColumn("payment_status", lower(trim("payment_status")))
)

silver_payments.writeTo(
    "retailx.silver.payments"
).using("iceberg").createOrReplace()

print(
    "PAYMENTS:",
    spark.sql(
        "SELECT COUNT(*) FROM retailx.silver.payments"
    ).collect()[0][0],
    "rows"
)


# ============================================================
# 6. STORES
# ============================================================
print("\n[6/6] Processing STORES...")

stores = spark.table("retailx.bronze.stores")

silver_stores = (
    stores
    .dropDuplicates(["store_id"])
    .filter(col("store_id").isNotNull())
    .withColumn("store_name", trim("store_name"))
    .withColumn("city", trim("city"))
)

silver_stores.writeTo(
    "retailx.silver.stores"
).using("iceberg").createOrReplace()

print(
    "STORES:",
    spark.sql(
        "SELECT COUNT(*) FROM retailx.silver.stores"
    ).collect()[0][0],
    "rows"
)


# ============================================================
# FINAL VERIFICATION
# ============================================================
print("\n" + "=" * 60)
print("             SILVER LAYER COMPLETE")
print("=" * 60)

spark.sql("SHOW TABLES IN retailx.silver").show(truncate=False)

print("\nSilver row counts:")

tables = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "stores"
]

for table in tables:
    count = spark.sql(
        f"SELECT COUNT(*) FROM retailx.silver.{table}"
    ).collect()[0][0]

    print(f"  {table:<15} : {count} rows")

print("\n" + "=" * 60)

spark.stop()
