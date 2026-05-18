import os
import urllib.request
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, lit, when,
    round as spark_round, datediff, to_date,
    months_between, trim, upper, lower,
    coalesce,
)
from pyspark.sql.types import IntegerType, LongType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# ══════════════════════════════════════════════════════════════════
# ENVIRONMENT & SPARK SESSION
# ══════════════════════════════════════════════════════════════════
GCP_KEY_PATH    = "/app/3_spark_processing/config/gcp_service_account.json"
GCS_TEMP_BUCKET = "khoi-spark-temp"   # ← đổi thành tên GCS bucket thực tế
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_KEY_PATH
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

GCS_JAR_VERSION  = "2.2.22"
GCS_JAR_FILENAME = f"gcs-connector-hadoop3-{GCS_JAR_VERSION}.jar"
GCS_JAR_LOCAL    = f"/tmp/shaded_{GCS_JAR_FILENAME}"
GCS_JAR_URL = f"https://storage.googleapis.com/hadoop-lib/gcs/{GCS_JAR_FILENAME}"

if not os.path.exists(GCS_JAR_LOCAL):
    print(f"📥 Downloading {GCS_JAR_FILENAME} ...")
    urllib.request.urlretrieve(GCS_JAR_URL, GCS_JAR_LOCAL)
    print("✅ Download complete")
else:
    print(f"✅ JAR already exists: {GCS_JAR_LOCAL}")

BQ_CONNECTOR = "com.google.cloud.spark:spark-3.5-bigquery:0.37.0"

spark = SparkSession.builder \
    .appName("Enterprise_Medallion_Batch_Pipeline") \
    .config("spark.driver.host",        "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.sql.shuffle.partitions", "4") \
    \
    .config("spark.jars",          GCS_JAR_LOCAL) \
    .config("spark.jars.packages",
            f"{BQ_CONNECTOR},"
            "org.postgresql:postgresql:42.7.2,"
            "org.apache.spark:spark-avro_2.12:3.5.0") \
    \
    .config("spark.hadoop.google.cloud.auth.service.account.enable",       "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GCP_KEY_PATH) \
    .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile",        GCP_KEY_PATH) \
    .config("credentialsFile",    GCP_KEY_PATH) \
    .config("temporaryGcsBucket", GCS_TEMP_BUCKET) \
    \
    .config("spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ══════════════════════════════════════════════════════════════════
# CONSTANTS — 3 datasets riêng biệt trên BigQuery
# ══════════════════════════════════════════════════════════════════
GCP_PROJECT = "ecommerce-streaming-pipeline"

BQ_BRONZE = f"{GCP_PROJECT}.bronze_ecommerce"
BQ_SILVER = f"{GCP_PROJECT}.silver_ecommerce"
BQ_GOLD   = f"{GCP_PROJECT}.gold_ecommerce"

JDBC_URL = "jdbc:postgresql://postgres:5432/ecommerce_db"
JDBC_PROPS = {
    "user":     "ecom_user",
    "password": "ecom_password",
    "driver":   "org.postgresql.Driver",
}

# Shared batch identifier — nhóm tất cả bảng trong cùng một lần chạy
BATCH_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

# ══════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════
def read_pg(table: str) -> DataFrame:
    return spark.read.jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPS)

def write_bq(df: DataFrame, full_table: str, mode: str = "overwrite") -> None:
    """
    Ghi DataFrame lên BigQuery dùng writeMethod=indirect (qua GCS staging).
    temporaryGcsBucket đã được set ở session level — không cần lặp lại ở đây.
    """
    df.cache()
    n = df.count()
    label = full_table.split(".")[-1]
    layer = full_table.split(".")[1].split("_")[0].upper()
    print(f"    [{layer}][{mode}] {label}: {n:,} dòng → {full_table}")
    df.write.format("bigquery") \
        .option("table",       full_table) \
        .option("writeMethod", "indirect") \
        .option("intermediateFormat", "avro") \
        .option("useAvroLogicalTypes", "true") \
        .mode(mode) \
        .save()
    df.unpersist()


def quarantine(df: DataFrame, reason: str, target: str) -> None:
    """Ghi các bản ghi không hợp lệ vào quarantine table trong Silver."""
    df.cache()
    n = df.count()
    if n > 0:
        print(f"    ⚠️  Quarantine {target}: {n:,} dòng lỗi")
        bad = df.withColumn("quarantine_reason", lit(reason)) \
                .withColumn("quarantined_at",    current_timestamp())
        write_bq(bad, f"{BQ_SILVER}.{target}", mode="append")
    df.unpersist()

def dedup(df: DataFrame, pk: str) -> DataFrame:
    """
    Loại trùng theo primary key, giữ bản ghi có ingested_at mới nhất.
    Bảo vệ Silver khỏi việc chạy lại batch nhiều lần tạo ra bản sao.
    """
    w = Window.partitionBy(pk).orderBy(col("ingested_at").desc())
    return df.withColumn("_rn", row_number().over(w)) \
             .filter(col("_rn") == 1) \
             .drop("_rn")

# ══════════════════════════════════════════════════════════════════
#  ██████  ██████   ██████  ███    ██ ███████ ███████
#  ██   ██ ██   ██ ██    ██ ████   ██    ███  ██
#  ██████  ██████  ██    ██ ██ ██  ██   ███   █████
#  ██   ██ ██   ██ ██    ██ ██  ██ ██  ███    ██
#  ██████  ██   ██  ██████  ██   ████ ███████ ███████
#
#  Mục đích : Lưu trữ dữ liệu thô từ PostgreSQL, KHÔNG biến đổi gì.
#             Chỉ thêm 3 metadata columns: ingested_at, batch_id, source.
#  Mode     : APPEND — lịch sử được giữ nguyên, cho phép time-travel
#             và reprocess lại toàn bộ pipeline từ đầu nếu cần.
#  Lưu ý   : Password KHÔNG được đưa lên Bronze. Loại tại nguồn.
# ══════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print(f"🥉 BRONZE — Raw Snapshots  [batch_id={BATCH_ID}]")
print("="*60)

BRONZE_TABLES = [
    "products", "inventory", "promotions",
    "orders", "order_items", "inventory_logs", "reviews",
]
# users xử lý riêng để loại password ngay tại đây
BRONZE_TABLES_ALL = ["users"] + BRONZE_TABLES

bronze = {}

# users: loại bỏ password tại Bronze (không bao giờ rời khỏi nguồn)
print("\n  users  (password excluded at source)")
raw_users = read_pg("users").drop("password") \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("batch_id",    lit(BATCH_ID)) \
    .withColumn("source",      lit("postgresql.users"))
write_bq(raw_users, f"{BQ_BRONZE}.users", mode="append")
bronze["users"] = raw_users

for tbl in BRONZE_TABLES:
    print(f"\n  {tbl}")
    df = read_pg(tbl) \
        .withColumn("ingested_at", current_timestamp()) \
        .withColumn("batch_id",    lit(BATCH_ID)) \
        .withColumn("source",      lit(f"postgresql.{tbl}"))
    write_bq(df, f"{BQ_BRONZE}.{tbl}", mode="append")
    bronze[tbl] = df

print(f"\n✅ Bronze: {len(bronze)} bảng, batch_id={BATCH_ID}")

# ══════════════════════════════════════════════════════════════════
#   ██████  ██ ██      ██    ██ ███████ ██████
#  ██       ██ ██      ██    ██ ██      ██   ██
#   ██████  ██ ██      ██    ██ █████   ██████
#       ██  ██ ██       ██  ██  ██      ██   ██
#  ██████   ██ ███████   ████   ███████ ██   ██
#
#  Mục đích : Làm sạch và chuẩn hóa dữ liệu từ Bronze.
#             - Ép kiểu (cast)
#             - Chuẩn hóa chuỗi (trim, upper, lower)
#             - Xử lý null / giá trị mặc định (coalesce)
#             - Lọc bản ghi không hợp lệ → quarantine tables
#             - Dedup theo primary key
#             - KHÔNG có business logic (không tính margin, không join)
#  Mode     : OVERWRITE — Silver là ảnh chụp sạch nhất tại hiện tại.
# ══════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("🥈 SILVER — Cleaned & Validated")
print("="*60)

silver = {}

# ── silver.users ──────────────────────────────────────────────────
print("\n  users")
_bad = bronze["users"].filter(col("user_id").isNull() | col("username").isNull())
quarantine(_bad, "null user_id or username", "quarantine_users")

silver["users"] = dedup(
    bronze["users"]
    .filter(col("user_id").isNotNull() & col("username").isNotNull())
    .select(
        trim(col("user_id")).alias("user_id"),
        trim(lower(col("username"))).alias("username"),
        trim(col("full_name")).alias("full_name"),
        lower(trim(col("email"))).alias("email"),
        trim(col("phone")).alias("phone"),
        trim(col("gender")).alias("gender"),
        trim(col("location")).alias("location"),
        col("created_at").cast("timestamp").alias("created_at"),
        col("is_active").cast("boolean").alias("is_active"),
        col("ingested_at"),
        col("batch_id"),
    ),
    pk="user_id"
)
write_bq(silver["users"], f"{BQ_SILVER}.users")

# ── silver.products ───────────────────────────────────────────────
print("\n  products")
_bad = bronze["products"].filter(col("product_id").isNull() | col("price").isNull() | (col("price") <= 0))
quarantine(_bad, "null product_id or invalid price", "quarantine_products")

silver["products"] = dedup(
    bronze["products"]
    .filter(col("product_id").isNotNull() & col("price").isNotNull() & (col("price") > 0))
    .select(
        trim(col("product_id")).alias("product_id"),
        trim(col("product_name")).alias("product_name"),
        trim(col("brand")).alias("brand"),
        trim(col("category")).alias("category"),
        col("price").cast(IntegerType()).alias("price"),
        col("image_url"),
        col("created_at").cast("timestamp").alias("created_at"),
        col("is_active").cast("boolean").alias("is_active"),
        col("ingested_at"),
        col("batch_id"),
    ),
    pk="product_id"
)
write_bq(silver["products"], f"{BQ_SILVER}.products")

# ── silver.inventory ──────────────────────────────────────────────
print("\n  inventory")
silver["inventory"] = dedup(
    bronze["inventory"]
    .filter(col("product_id").isNotNull())
    .select(
        trim(col("product_id")).alias("product_id"),
        col("import_price").cast(IntegerType()).alias("import_price"),
        # stock không được âm
        when(col("stock_quantity") < 0, lit(0))
            .otherwise(col("stock_quantity"))
            .cast(IntegerType()).alias("stock_quantity"),
        col("last_restock_date").cast("timestamp").alias("last_restock_date"),
        col("ingested_at"),
        col("batch_id"),
    ),
    pk="product_id"
)
write_bq(silver["inventory"], f"{BQ_SILVER}.inventory")

# ── silver.promotions ─────────────────────────────────────────────
print("\n  promotions")
silver["promotions"] = dedup(
    bronze["promotions"]
    .filter(col("promo_id").isNotNull() & col("promo_code").isNotNull())
    .select(
        trim(col("promo_id")).alias("promo_id"),
        upper(trim(col("promo_code"))).alias("promo_code"),
        col("discount_percent").cast("decimal(5,2)").alias("discount_percent"),
        col("start_date").cast("timestamp").alias("start_date"),
        col("end_date").cast("timestamp").alias("end_date"),
        col("is_active").cast("boolean").alias("is_active"),
        col("ingested_at"),
        col("batch_id"),
    ),
    pk="promo_id"
)
write_bq(silver["promotions"], f"{BQ_SILVER}.promotions")

# ── silver.orders ─────────────────────────────────────────────────
print("\n  orders")
_bad = bronze["orders"].filter(
    col("order_id").isNull() | col("user_id").isNull() | (col("total_amount") <= 0)
)
quarantine(_bad, "null order_id/user_id or non-positive total", "quarantine_orders")

silver["orders"] = dedup(
    bronze["orders"]
    .filter(col("order_id").isNotNull() & col("user_id").isNotNull() & (col("total_amount") > 0))
    .select(
        trim(col("order_id")).alias("order_id"),
        trim(col("user_id")).alias("user_id"),
        trim(col("promo_id")).alias("promo_id"),
        col("subtotal").cast(IntegerType()).alias("subtotal"),
        col("shipping_fee").cast(IntegerType()).alias("shipping_fee"),
        coalesce(col("discount_amount"), lit(0)).cast(IntegerType()).alias("discount_amount"),
        col("total_amount").cast(IntegerType()).alias("total_amount"),
        trim(col("payment_method")).alias("payment_method"),
        trim(col("order_status")).alias("order_status"),
        col("order_date").cast("timestamp").alias("order_date"),
        col("updated_at").cast("timestamp").alias("updated_at"),
        col("ingested_at"),
        col("batch_id"),
    ),
    pk="order_id"
)
write_bq(silver["orders"], f"{BQ_SILVER}.orders")

# ── silver.order_items ────────────────────────────────────────────
print("\n  order_items")
_bad = bronze["order_items"].filter(
    col("order_id").isNull() | col("product_id").isNull() |
    (col("quantity") <= 0) | (col("unit_price") <= 0)
)
quarantine(_bad, "null FK or invalid qty/price", "quarantine_order_items")

silver["order_items"] = dedup(
    bronze["order_items"]
    .filter(
        col("order_id").isNotNull() & col("product_id").isNotNull() &
        (col("quantity") > 0) & (col("unit_price") > 0)
    )
    .select(
        col("item_id").cast(LongType()).alias("item_id"),
        trim(col("order_id")).alias("order_id"),
        trim(col("product_id")).alias("product_id"),
        col("quantity").cast(IntegerType()).alias("quantity"),
        col("unit_price").cast(IntegerType()).alias("unit_price"),
        col("unit_cost").cast(IntegerType()).alias("unit_cost"),
        col("item_total").cast(IntegerType()).alias("item_total"),
        col("ingested_at"),
        col("batch_id"),
    ),
    pk="item_id"
)
write_bq(silver["order_items"], f"{BQ_SILVER}.order_items")

# ── silver.inventory_logs ─────────────────────────────────────────
print("\n  inventory_logs")
silver["inventory_logs"] = dedup(
    bronze["inventory_logs"]
    .filter(col("product_id").isNotNull() & col("transaction_type").isNotNull())
    .select(
        col("log_id").cast(LongType()).alias("log_id"),
        trim(col("product_id")).alias("product_id"),
        trim(col("order_id")).alias("order_id"),
        upper(trim(col("transaction_type"))).alias("transaction_type"),
        col("quantity_changed").cast(IntegerType()).alias("quantity_changed"),
        col("note"),
        col("created_at").cast("timestamp").alias("created_at"),
        col("ingested_at"),
        col("batch_id"),
    ),
    pk="log_id"
)
write_bq(silver["inventory_logs"], f"{BQ_SILVER}.inventory_logs")

# ── silver.reviews ────────────────────────────────────────────────
print("\n  reviews")
_bad = bronze["reviews"].filter(
    col("rating").isNull() | (col("rating") < 1) | (col("rating") > 5)
)
quarantine(_bad, "null or out-of-range rating (1–5)", "quarantine_reviews")

silver["reviews"] = dedup(
    bronze["reviews"]
    .filter(
        col("product_id").isNotNull() & col("user_id").isNotNull() &
        col("rating").isNotNull() & (col("rating") >= 1) & (col("rating") <= 5)
    )
    .select(
        col("review_id").cast(LongType()).alias("review_id"),
        trim(col("product_id")).alias("product_id"),
        trim(col("user_id")).alias("user_id"),
        col("rating").cast(IntegerType()).alias("rating"),
        trim(col("comment")).alias("comment"),
        col("review_date").cast("timestamp").alias("review_date"),
        col("ingested_at"),
        col("batch_id"),
    ),
    pk="review_id"
)
write_bq(silver["reviews"], f"{BQ_SILVER}.reviews")

print("\n✅ Silver hoàn tất")

# ══════════════════════════════════════════════════════════════════
#   ██████   ██████  ██      ██████
#  ██       ██    ██ ██      ██   ██
#  ██   ███ ██    ██ ██      ██   ██
#  ██    ██ ██    ██ ██      ██   ██
#   ██████   ██████  ███████ ██████
#
#  Mục đích : Áp dụng business logic lên dữ liệu đã sạch từ Silver.
#             - DIM tables: denorm, derived fields, static lookups
#             - FACT tables: join, metric calculation, correct grain
#             - Dữ liệu sẵn sàng để BI / dbt query trực tiếp
#  Mode     : OVERWRITE — Gold luôn là phiên bản tính toán cuối cùng.
# ══════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("🥇 GOLD — Business-Ready DIM & FACT Tables")
print("="*60)

# Shorthand references đến Silver DataFrames (đã có trong memory)
su  = silver["users"]
sp  = silver["products"]
si  = silver["inventory"]
spr = silver["promotions"]
so  = silver["orders"]
soi = silver["order_items"]
sil = silver["inventory_logs"]
sr  = silver["reviews"]

# ─────────────────────────────────────────────────────────────────
# DIM TABLES
# ─────────────────────────────────────────────────────────────────
print("\n── DIM Tables ──")

# dim_date — generated in Spark, không phụ thuộc Silver
print("\n  dim_date")
spark.sql("""
    SELECT explode(
        sequence(to_date('2023-01-01'), to_date('2030-12-31'), interval 1 day)
    ) AS date_key
""").createOrReplaceTempView("_raw_dates")

dim_date = spark.sql("""
    SELECT
        date_key,
        year(date_key)                       AS year,
        quarter(date_key)                    AS quarter,
        month(date_key)                      AS month,
        date_format(date_key, 'MMMM')        AS month_name,
        day(date_key)                        AS day,
        dayofweek(date_key)                  AS day_of_week_num,
        date_format(date_key, 'EEEE')        AS day_of_week_name,
        weekofyear(date_key)                 AS week_of_year,
        CASE WHEN dayofweek(date_key) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend,
        date_format(date_key, 'yyyy-MM')     AS year_month
    FROM _raw_dates
""").withColumn("dw_updated_at", current_timestamp())
write_bq(dim_date, f"{BQ_GOLD}.dim_date")

# dim_users
print("\n  dim_users")
dim_users = su.select(
    col("user_id"),
    col("username"),
    col("full_name"),
    col("email"),
    col("phone"),
    col("gender"),
    col("location"),
    col("created_at").alias("account_created_at"),
    to_date(col("created_at")).alias("account_created_date"),
    col("is_active"),
).withColumn(
    "account_age_months",
    months_between(current_timestamp(), col("account_created_at")).cast("int")
).withColumn("dw_updated_at", current_timestamp())
write_bq(dim_users, f"{BQ_GOLD}.dim_users")

# dim_products (denorm Silver products + Silver inventory)
print("\n  dim_products")
dim_products = sp.join(si, on="product_id", how="left").select(
    col("product_id"),
    col("product_name"),
    col("brand"),
    col("category"),
    col("price").alias("current_selling_price"),
    col("import_price").alias("current_unit_cost"),
    spark_round(
        (col("price") - col("import_price")) / col("price") * 100, 2
    ).alias("gross_margin_pct"),
    col("stock_quantity").alias("current_stock"),
    when(col("stock_quantity") < 10,  lit("Critical"))
    .when(col("stock_quantity") < 20, lit("Low"))
    .when(col("stock_quantity") < 50, lit("Medium"))
    .otherwise(lit("Healthy")).alias("stock_status"),
    col("is_active"),
    col("last_restock_date"),
).withColumn("dw_updated_at", current_timestamp())
write_bq(dim_products, f"{BQ_GOLD}.dim_products")

# dim_promotions
print("\n  dim_promotions")
dim_promotions = spr.select(
    col("promo_id"),
    col("promo_code"),
    col("discount_percent"),
    col("start_date"),
    col("end_date"),
    when(
        col("start_date").isNotNull() & col("end_date").isNotNull(),
        datediff(col("end_date"), col("start_date"))
    ).alias("duration_days"),
    col("is_active"),
).withColumn("dw_updated_at", current_timestamp())
write_bq(dim_promotions, f"{BQ_GOLD}.dim_promotions")

# dim_payment_method — static lookup, không cần Silver
print("\n  dim_payment_method")
write_bq(
    spark.createDataFrame([
        ("COD",           "Cash on Delivery",  "Offline", False),
        ("Credit Card",   "Credit Card",        "Online",  True),
        ("E-Wallet",      "Electronic Wallet",  "Online",  True),
        ("Bank Transfer", "Bank Transfer",      "Online",  True),
    ], ["payment_method_code", "payment_method_name", "payment_channel", "is_digital"])
    .withColumn("dw_updated_at", current_timestamp()),
    f"{BQ_GOLD}.dim_payment_method"
)

# dim_order_status — static lookup
print("\n  dim_order_status")
write_bq(
    spark.createDataFrame([
        ("Pending",    "Chờ xác nhận",   False, False),
        ("Processing", "Đang xử lý",     False, False),
        ("Shipped",    "Đang giao hàng", False, False),
        ("Delivered",  "Đã giao hàng",   True,  False),
        ("Cancelled",  "Đã hủy",         False, True),
        ("Returned",   "Đã hoàn hàng",   False, True),
    ], ["status_code", "status_name_vi", "is_completed", "is_negative"])
    .withColumn("dw_updated_at", current_timestamp()),
    f"{BQ_GOLD}.dim_order_status"
)

# ─────────────────────────────────────────────────────────────────
# FACT TABLES
# ─────────────────────────────────────────────────────────────────
print("\n── FACT Tables ──")

# fact_orders
print("\n  fact_orders")
fact_orders = so.select(
    col("order_id"),
    col("user_id"),
    col("promo_id"),
    col("payment_method"),
    col("order_status"),
    to_date(col("order_date")).alias("date_key"),
    col("subtotal"),
    col("shipping_fee"),
    col("discount_amount"),
    col("total_amount"),
    (col("total_amount") - col("shipping_fee")).alias("net_revenue"),
    col("order_date").alias("order_created_at"),
    col("updated_at"),
).withColumn("dw_loaded_at", current_timestamp())
write_bq(fact_orders, f"{BQ_GOLD}.fact_orders")

# fact_order_items
print("\n  fact_order_items")
fact_order_items = soi.join(
    so.select("order_id", "user_id", "promo_id", "order_status",
              "payment_method", "order_date", "discount_amount", "shipping_fee"),
    on="order_id", how="left"
).select(
    col("item_id"),
    col("order_id"),
    col("user_id"),
    col("product_id"),
    col("promo_id"),
    col("order_status"),
    col("payment_method"),
    to_date(col("order_date")).alias("date_key"),
    col("quantity"),
    col("unit_price"),
    col("unit_cost"),
    col("item_total"),
    (col("unit_price") - col("unit_cost")).alias("unit_gross_profit"),
    ((col("unit_price") - col("unit_cost")) * col("quantity")).alias("line_gross_profit"),
    spark_round(
        (col("unit_price") - col("unit_cost")) / col("unit_price") * 100, 2
    ).alias("gross_margin_pct"),
    col("order_date").alias("order_created_at"),
).withColumn("dw_loaded_at", current_timestamp())
write_bq(fact_order_items, f"{BQ_GOLD}.fact_order_items")

# fact_sales — main analysis table, fully denormalized, 1 row per line item
print("\n  fact_sales  ← BẢNG PHÂN TÍCH CHÍNH")
sp_slim  = sp.select("product_id", "product_name", "brand", "category")
si_slim  = si.select("product_id", "import_price")

fact_sales = (
    soi
    .join(so,      on="order_id",   how="left")
    .join(sp_slim, on="product_id", how="left")
    .join(si_slim, on="product_id", how="left")
    .select(
        col("item_id"),
        col("order_id"),
        col("user_id"),
        col("product_id"),
        col("promo_id"),
        col("payment_method"),
        col("order_status"),
        to_date(col("order_date")).alias("date_key"),
        # Denorm product attributes
        col("product_name"),
        col("brand"),
        col("category"),
        # Revenue
        col("quantity"),
        col("unit_price"),
        col("item_total").alias("gross_revenue"),
        spark_round(
            col("discount_amount") * col("item_total") / col("subtotal"), 0
        ).cast(IntegerType()).alias("allocated_discount"),
        spark_round(
            col("item_total") - col("discount_amount") * col("item_total") / col("subtotal"), 0
        ).cast(IntegerType()).alias("net_revenue"),
        # Cost & Profit
        col("unit_cost"),
        (col("unit_cost") * col("quantity")).alias("total_cost"),
        spark_round(
            col("item_total") - col("discount_amount") * col("item_total") / col("subtotal")
            - col("unit_cost") * col("quantity"), 0
        ).cast(IntegerType()).alias("gross_profit"),
        spark_round(
            (
                col("item_total") - col("discount_amount") * col("item_total") / col("subtotal")
                - col("unit_cost") * col("quantity")
            ) / (
                col("item_total") - col("discount_amount") * col("item_total") / col("subtotal")
            ) * 100, 2
        ).alias("gross_margin_pct"),
        # Order-level context
        col("subtotal").alias("order_subtotal"),
        col("shipping_fee"),
        col("discount_amount").alias("order_discount_amount"),
        col("total_amount").alias("order_total_amount"),
        col("order_date").alias("order_created_at"),
        col("updated_at").alias("order_updated_at"),
    )
    .withColumn("dw_loaded_at", current_timestamp())
)
write_bq(fact_sales, f"{BQ_GOLD}.fact_sales")

# fact_inventory_log
print("\n  fact_inventory_log")
fact_inventory_log = sil.select(
    col("log_id"),
    col("product_id"),
    col("order_id"),
    col("transaction_type"),
    col("quantity_changed"),
    when(col("transaction_type") == "IN",  col("quantity_changed"))
    .otherwise(-col("quantity_changed")).alias("quantity_delta"),
    col("note"),
    to_date(col("created_at")).alias("date_key"),
    col("created_at").alias("log_created_at"),
).withColumn("dw_loaded_at", current_timestamp())
write_bq(fact_inventory_log, f"{BQ_GOLD}.fact_inventory_log")

# fact_reviews
print("\n  fact_reviews")
fact_reviews = sr.select(
    col("review_id"),
    col("product_id"),
    col("user_id"),
    col("rating"),
    when(col("rating") >= 4, lit("Positive"))
    .when(col("rating") == 3, lit("Neutral"))
    .otherwise(lit("Negative")).alias("sentiment_label"),
    col("comment"),
    when(col("comment").isNotNull() & (trim(col("comment")) != ""), True)
    .otherwise(False).alias("has_comment"),
    to_date(col("review_date")).alias("date_key"),
    col("review_date").alias("review_created_at"),
).withColumn("dw_loaded_at", current_timestamp())
write_bq(fact_reviews, f"{BQ_GOLD}.fact_reviews")

# ══════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("✅ BATCH ETL HOÀN TẤT")
print(f"   🥉 Bronze  → {BQ_BRONZE}   (append, {len(bronze)} tables)")
print(f"   🥈 Silver  → {BQ_SILVER}   (overwrite, clean)")
print(f"   🥇 Gold    → {BQ_GOLD}     (overwrite, business-ready)")
print(f"   Batch ID  : {BATCH_ID}")
print("="*60)
spark.stop()