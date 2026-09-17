from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    count,
    countDistinct,
    avg,
    max,
    min,
    year,
    month,
    date_format,
    coalesce,
    lit
)

spark = SparkSession.builder.getOrCreate()

spark.sql("CREATE NAMESPACE IF NOT EXISTS retailx.gold")

print("\n" + "=" * 70)
print("                    RETAILX GOLD LAYER")
print("=" * 70)


# ============================================================
# LOAD SILVER TABLES
# ============================================================

customers = spark.table("retailx.silver.customers")
products = spark.table("retailx.silver.products")
orders = spark.table("retailx.silver.orders")
order_items = spark.table("retailx.silver.order_items")
payments = spark.table("retailx.silver.payments")
stores = spark.table("retailx.silver.stores")


# ============================================================
# 1. DIM_CUSTOMER
# ============================================================

print("\n[1/10] Creating DIM_CUSTOMER...")

dim_customer = (
    customers
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "city",
        "signup_date"
    )
    .dropDuplicates(["customer_id"])
)

dim_customer.writeTo(
    "retailx.gold.dim_customer"
).using("iceberg").createOrReplace()

print(
    "DIM_CUSTOMER:",
    spark.sql(
        "SELECT COUNT(*) FROM retailx.gold.dim_customer"
    ).collect()[0][0],
    "rows"
)


# ============================================================
# 2. DIM_PRODUCT
# ============================================================

print("\n[2/10] Creating DIM_PRODUCT...")

dim_product = (
    products
    .select(
        "product_id",
        "product_name",
        "category",
        "price",
        "stock_quantity"
    )
    .dropDuplicates(["product_id"])
)

dim_product.writeTo(
    "retailx.gold.dim_product"
).using("iceberg").createOrReplace()

print(
    "DIM_PRODUCT:",
    spark.sql(
        "SELECT COUNT(*) FROM retailx.gold.dim_product"
    ).collect()[0][0],
    "rows"
)


# ============================================================
# 3. DIM_STORE
# ============================================================

print("\n[3/10] Creating DIM_STORE...")

dim_store = (
    stores
    .select(
        "store_id",
        "store_name",
        "city"
    )
    .dropDuplicates(["store_id"])
)

dim_store.writeTo(
    "retailx.gold.dim_store"
).using("iceberg").createOrReplace()

print(
    "DIM_STORE:",
    spark.sql(
        "SELECT COUNT(*) FROM retailx.gold.dim_store"
    ).collect()[0][0],
    "rows"
)


# ============================================================
# 4. FCT_ORDERS
# ============================================================

print("\n[4/10] Creating FCT_ORDERS...")

fct_orders = (
    orders.alias("o")
    .join(
        customers.alias("c"),
        col("o.customer_id") == col("c.customer_id"),
        "left"
    )
    .join(
        stores.alias("s"),
        col("o.store_id") == col("s.store_id"),
        "left"
    )
    .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("o.store_id"),
        col("o.order_date"),
        col("o.order_status"),
        col("c.city").alias("customer_city"),
        col("s.store_name")
    )
    .dropDuplicates(["order_id"])
)

fct_orders.writeTo(
    "retailx.gold.fct_orders"
).using("iceberg").createOrReplace()

print(
    "FCT_ORDERS:",
    spark.sql(
        "SELECT COUNT(*) FROM retailx.gold.fct_orders"
    ).collect()[0][0],
    "rows"
)


# ============================================================
# 5. FCT_ORDER_ITEMS
# ============================================================

print("\n[5/10] Creating FCT_ORDER_ITEMS...")

fct_order_items = (
    order_items.alias("oi")
    .join(
        products.alias("p"),
        col("oi.product_id") == col("p.product_id"),
        "left"
    )
    .select(
        col("oi.order_item_id"),
        col("oi.order_id"),
        col("oi.product_id"),
        col("oi.quantity"),
        col("oi.unit_price"),
        (
            col("oi.quantity") * col("oi.unit_price")
        ).alias("line_amount"),
        col("p.product_name"),
        col("p.category")
    )
)

fct_order_items.writeTo(
    "retailx.gold.fct_order_items"
).using("iceberg").createOrReplace()

print(
    "FCT_ORDER_ITEMS:",
    spark.sql(
        "SELECT COUNT(*) FROM retailx.gold.fct_order_items"
    ).collect()[0][0],
    "rows"
)


# ============================================================
# 6. FCT_PAYMENTS
# ============================================================

print("\n[6/10] Creating FCT_PAYMENTS...")

fct_payments = (
    payments.alias("p")
    .join(
        orders.alias("o"),
        col("p.order_id") == col("o.order_id"),
        "left"
    )
    .select(
        col("p.payment_id"),
        col("p.order_id"),
        col("p.payment_method"),
        col("p.payment_status"),
        col("p.amount"),
        col("o.customer_id"),
        col("o.store_id"),
        col("o.order_date")
    )
)

fct_payments.writeTo(
    "retailx.gold.fct_payments"
).using("iceberg").createOrReplace()

print(
    "FCT_PAYMENTS:",
    spark.sql(
        "SELECT COUNT(*) FROM retailx.gold.fct_payments"
    ).collect()[0][0],
    "rows"
)


# ============================================================
# 7. CUSTOMER_360
# ============================================================

print("\n[7/10] Creating CUSTOMER_360...")

customer_orders = (
    orders
    .groupBy("customer_id")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        min("order_date").alias("first_order_date"),
        max("order_date").alias("last_order_date")
    )
)

customer_spending = (
    order_items.alias("oi")
    .join(
        orders.alias("o"),
        col("oi.order_id") == col("o.order_id"),
        "inner"
    )
    .groupBy("customer_id")
    .agg(
        sum(
            col("oi.quantity") * col("oi.unit_price")
        ).alias("total_spend"),
        sum("oi.quantity").alias("total_items")
    )
)

customer_360 = (
    customers.alias("c")
    .join(
        customer_orders.alias("co"),
        col("c.customer_id") == col("co.customer_id"),
        "left"
    )
    .join(
        customer_spending.alias("cs"),
        col("c.customer_id") == col("cs.customer_id"),
        "left"
    )
    .select(
        col("c.customer_id"),
        col("c.first_name"),
        col("c.last_name"),
        col("c.email"),
        col("c.city"),
        col("c.signup_date"),
        coalesce(col("co.total_orders"), lit(0)).alias("total_orders"),
        coalesce(col("cs.total_spend"), lit(0)).alias("total_spend"),
        coalesce(col("cs.total_items"), lit(0)).alias("total_items"),
        col("co.first_order_date"),
        col("co.last_order_date")
    )
)

customer_360.writeTo(
    "retailx.gold.customer_360"
).using("iceberg").createOrReplace()

print(
    "CUSTOMER_360:",
    spark.sql(
        "SELECT COUNT(*) FROM retailx.gold.customer_360"
    ).collect()[0][0],
    "rows"
)


# ============================================================
# 8. PRODUCT_PERFORMANCE
# ============================================================

print("\n[8/10] Creating PRODUCT_PERFORMANCE...")

product_performance = (
    order_items
    .groupBy("product_id")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        sum("quantity").alias("units_sold"),
        sum(
            col("quantity") * col("unit_price")
        ).alias("total_sales"),
        avg("unit_price").alias("average_selling_price")
    )
    .join(
        products,
        "product_id",
        "left"
    )
    .select(
        "product_id",
        "product_name",
        "category",
        "price",
        "stock_quantity",
        "total_orders",
        "units_sold",
        "total_sales",
        "average_selling_price"
    )
)

product_performance.writeTo(
    "retailx.gold.product_performance"
).using("iceberg").createOrReplace()

print(
    "PRODUCT_PERFORMANCE:",
    spark.sql(
        "SELECT COUNT(*) FROM retailx.gold.product_performance"
    ).collect()[0][0],
    "rows"
)


# ============================================================
# 9. STORE_PERFORMANCE
# ============================================================

print("\n[9/10] Creating STORE_PERFORMANCE...")

store_order_stats = (
    orders
    .groupBy("store_id")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers")
    )
)

store_sales = (
    order_items.alias("oi")
    .join(
        orders.alias("o"),
        col("oi.order_id") == col("o.order_id"),
        "inner"
    )
    .groupBy("store_id")
    .agg(
        sum(
            col("oi.quantity") * col("oi.unit_price")
        ).alias("total_sales"),
        sum("oi.quantity").alias("units_sold")
    )
)

store_performance = (
    stores
    .join(
        store_order_stats,
        "store_id",
        "left"
    )
    .join(
        store_sales,
        "store_id",
        "left"
    )
    .select(
        "store_id",
        "store_name",
        "city",
        coalesce(col("total_orders"), lit(0)).alias("total_orders"),
        coalesce(col("unique_customers"), lit(0)).alias("unique_customers"),
        coalesce(col("units_sold"), lit(0)).alias("units_sold"),
        coalesce(col("total_sales"), lit(0)).alias("total_sales")
    )
)

store_performance.writeTo(
    "retailx.gold.store_performance"
).using("iceberg").createOrReplace()

print(
    "STORE_PERFORMANCE:",
    spark.sql(
        "SELECT COUNT(*) FROM retailx.gold.store_performance"
    ).collect()[0][0],
    "rows"
)


# ============================================================
# 10. MONTHLY_SALES
# ============================================================

print("\n[10/10] Creating MONTHLY_SALES...")

monthly_sales = (
    order_items.alias("oi")
    .join(
        orders.alias("o"),
        col("oi.order_id") == col("o.order_id"),
        "inner"
    )
    .groupBy(
        year("o.order_date").alias("year"),
        month("o.order_date").alias("month")
    )
    .agg(
        countDistinct("o.order_id").alias("total_orders"),
        countDistinct("o.customer_id").alias("unique_customers"),
        sum("oi.quantity").alias("units_sold"),
        sum(
            col("oi.quantity") * col("oi.unit_price")
        ).alias("total_sales"),
        avg("oi.unit_price").alias("average_item_price")
    )
    .orderBy("year", "month")
)

monthly_sales.writeTo(
    "retailx.gold.monthly_sales"
).using("iceberg").createOrReplace()

print(
    "MONTHLY_SALES:",
    spark.sql(
        "SELECT COUNT(*) FROM retailx.gold.monthly_sales"
    ).collect()[0][0],
    "rows"
)


# ============================================================
# FINAL VERIFICATION
# ============================================================

print("\n" + "=" * 70)
print("                    GOLD LAYER COMPLETE")
print("=" * 70)

print("\nGold tables:")

spark.sql(
    "SHOW TABLES IN retailx.gold"
).show(truncate=False)


print("\nGold row counts:")

gold_tables = [
    "dim_customer",
    "dim_product",
    "dim_store",
    "fct_orders",
    "fct_order_items",
    "fct_payments",
    "customer_360",
    "product_performance",
    "store_performance",
    "monthly_sales"
]

for table in gold_tables:

    count = spark.sql(
        f"SELECT COUNT(*) FROM retailx.gold.{table}"
    ).collect()[0][0]

    print(f"  {table:<25} : {count} rows")


print("\n" + "=" * 70)
print("              RETAILX GOLD LAYER FINISHED")
print("=" * 70)

spark.stop()