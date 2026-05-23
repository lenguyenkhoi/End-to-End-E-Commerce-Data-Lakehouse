import os
import urllib.request
import functools
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_date, lit, when,
    round as spark_round, explode,
    current_timestamp, trim, upper, lower,
    coalesce,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, LongType, ArrayType,
)

# ══════════════════════════════════════════════════════════════════
# ENVIRONMENT & SPARK SESSION
# ══════════════════════════════════════════════════════════════════
GCP_KEY_PATH    = "/app/3_spark_processing/config/gcp_service_account.json"
GCS_TEMP_BUCKET = "khoi-spark-temp"   # ← đổi thành tên GCS bucket thực tế
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_KEY_PATH
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# ══════════════════════════════════════════════════════════════════
# ROOT CAUSE & FIX
# ══════════════════════════════════════════════════════════════════
# ClassNotFoundException: GoogleHadoopFileSystem xảy ra vì:
#   spark.jars.packages  → JAR được load vào Spark USER classloader
#   Hadoop FileSystem registry → dùng SYSTEM classloader
#   Hai classloader này KHÔNG thấy nhau trong Docker
#
# FIX DỨT ĐIỂM:
#   1. Download gcs-connector JAR về local path bằng urllib
#   2. Pass vào spark.jars (absolute path) → JAR vào SYSTEM classloader
#   3. Hadoop FileSystem registry tìm thấy GoogleHadoopFileSystem ✓
#
# spark.jars.packages vẫn giữ cho BQ connector và Kafka (chúng không
# cần system classloader vì Spark tự quản lý chúng qua DataSource API)
# ══════════════════════════════════════════════════════════════════
GCS_JAR_VERSION  = "2.2.22"
GCS_JAR_FILENAME = f"gcs-connector-hadoop3-{GCS_JAR_VERSION}.jar"
GCS_JAR_LOCAL    = f"/tmp/{GCS_JAR_FILENAME}"
GCS_JAR_URL = f"https://storage.googleapis.com/hadoop-lib/gcs/{GCS_JAR_FILENAME}"
if not os.path.exists(GCS_JAR_LOCAL):
    print(f"📥 Downloading {GCS_JAR_FILENAME} → {GCS_JAR_LOCAL} ...")
    urllib.request.urlretrieve(GCS_JAR_URL, GCS_JAR_LOCAL)
    print("✅ Download complete")
else:
    print(f"✅ JAR already exists: {GCS_JAR_LOCAL}")

BQ_CONNECTOR = "com.google.cloud.spark:spark-3.5-bigquery:0.37.0"

spark = SparkSession.builder \
    .appName("Enterprise_Streaming_Pipeline") \
    .config("spark.driver.host",        "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.sql.shuffle.partitions", "4") \
    \
    .config("spark.jars",          GCS_JAR_LOCAL) \
    .config("spark.jars.packages",
            f"{BQ_CONNECTOR},"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.spark:spark-avro_2.12:3.5.0")\
    \
    .config("spark.hadoop.google.cloud.auth.service.account.enable","true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",GCP_KEY_PATH) \
    .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile",GCP_KEY_PATH) \
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
# CONSTANTS
# ══════════════════════════════════════════════════════════════════
KAFKA_SERVER = "kafka:29092"
GCP_PROJECT  = "ecommerce-streaming-pipeline"

BQ_BRONZE = f"{GCP_PROJECT}.bronze_ecommerce"
BQ_SILVER = f"{GCP_PROJECT}.silver_ecommerce"
BQ_GOLD   = f"{GCP_PROJECT}.gold_ecommerce"

TRIGGER_INTERVAL = "60 seconds"

TOPIC_USER      = "ecommerce.user"
TOPIC_PAGE_VIEW = "ecommerce.page_view"
TOPIC_CART      = "ecommerce.cart"
TOPIC_PROMO     = "ecommerce.promo"
TOPIC_ORDER     = "ecommerce.order"
TOPIC_INVENTORY = "ecommerce.inventory"
TOPIC_REVIEW    = "ecommerce.review"

ALL_TOPICS = [
    TOPIC_USER, TOPIC_PAGE_VIEW, TOPIC_CART,
    TOPIC_PROMO, TOPIC_ORDER, TOPIC_INVENTORY, TOPIC_REVIEW,
]

# ══════════════════════════════════════════════════════════════════
# SCHEMAS
# ══════════════════════════════════════════════════════════════════
def envelope_schema(extra: list) -> StructType:
    return StructType([
        StructField("event_id",    StringType(), True),
        StructField("event_type",  StringType(), True),
        StructField("timestamp",   StringType(), True),
        StructField("session_id",  StringType(), True),
        StructField("user_id",     StringType(), True),
        StructField("user_name",   StringType(), True),
        StructField("location",    StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("os",          StringType(), True),
    ] + extra)

user_schema = envelope_schema([
    StructField("username", StringType(), True),
    StructField("gender",   StringType(), True),
])

page_schema = envelope_schema([
    StructField("page_name",     StringType(),  True),
    StructField("product_id",    StringType(),  True),
    StructField("product_name",  StringType(),  True),
    StructField("category",      StringType(),  True),
    StructField("brand",         StringType(),  True),
    StructField("price",         IntegerType(), True),
    StructField("trigger",       StringType(),  True),
    StructField("search_term",   StringType(),  True),
    StructField("results_count", IntegerType(), True),
])

cart_schema = envelope_schema([
    StructField("product_id",          StringType(),  True),
    StructField("product_name",        StringType(),  True),
    StructField("brand",               StringType(),  True),
    StructField("category",            StringType(),  True),
    StructField("unit_price",          IntegerType(), True),
    StructField("quantity",            IntegerType(), True),
    StructField("cart_quantity_after", IntegerType(), True),
    StructField("old_quantity",        IntegerType(), True),
    StructField("new_quantity",        IntegerType(), True),
])

promo_schema = envelope_schema([
    StructField("promo_id",         StringType(), True),
    StructField("promo_code",       StringType(), True),
    StructField("discount_percent", FloatType(),  True),
])

_item_struct = StructType([
    StructField("product_id",   StringType(),  True),
    StructField("product_name", StringType(),  True),
    StructField("quantity",     IntegerType(), True),
    StructField("unit_price",   IntegerType(), True),
    StructField("unit_cost",    IntegerType(), True),
])
order_schema = envelope_schema([
    StructField("order_id",          StringType(),            True),
    StructField("subtotal",          IntegerType(),           True),
    StructField("shipping_fee",      IntegerType(),           True),
    StructField("discount_amount",   IntegerType(),           True),
    StructField("total_amount",      IntegerType(),           True),
    StructField("payment_method",    StringType(),            True),
    StructField("promo_id",          StringType(),            True),
    StructField("promo_code",        StringType(),            True),
    StructField("order_status",      StringType(),            True),
    StructField("reason",            StringType(),            True),
    StructField("items",             ArrayType(_item_struct), True),
    StructField("product_id",        StringType(),            True),
    StructField("quantity_returned", IntegerType(),           True),
])

inventory_schema = envelope_schema([
    StructField("product_id",        StringType(),  True),
    StructField("product_name",      StringType(),  True),
    StructField("order_id",          StringType(),  True),
    StructField("quantity_sold",     IntegerType(), True),
    StructField("quantity_returned", IntegerType(), True),
    StructField("quantity_added",    IntegerType(), True),
    StructField("stock_remaining",   IntegerType(), True),
])

review_schema = envelope_schema([
    StructField("product_id",     StringType(),  True),
    StructField("rating",         IntegerType(), True),
    StructField("comment_length", IntegerType(), True),
])

TOPIC_SCHEMAS = {
    TOPIC_USER:      user_schema,
    TOPIC_PAGE_VIEW: page_schema,
    TOPIC_CART:      cart_schema,
    TOPIC_PROMO:     promo_schema,
    TOPIC_ORDER:     order_schema,
    TOPIC_INVENTORY: inventory_schema,
    TOPIC_REVIEW:    review_schema,
}

TOPIC_VALID_EVENT_TYPES = {
    TOPIC_USER:      ["user_registered", "user_login", "user_logout",
                      "user_login_attempt", "user_login_failed"],
    TOPIC_PAGE_VIEW: ["page_viewed", "product_viewed",
                      "category_filtered", "search_performed"],
    TOPIC_CART:      ["item_added", "item_removed", "item_qty_updated"],
    TOPIC_PROMO:     ["promo_applied", "promo_removed", "promo_invalid"],
    TOPIC_ORDER:     ["order_created", "order_paid",
                      "order_cancelled", "order_returned"],
    TOPIC_INVENTORY: ["stock_updated_out", "stock_updated_in", "low_stock_alert",
                      "restock_manual", "restock_auto"],
    TOPIC_REVIEW:    ["review_submitted"],
}

# ══════════════════════════════════════════════════════════════════
# BQ WRITE HELPER
#
# FIX CHÍNH: Không dùng df.count() nữa.
# count() trong foreachBatch là action thứ hai — nó buộc Spark phải
# tính toán lại toàn bộ batch một lần nữa trước khi write, nhân đôi
# thời gian xử lý. Thay bằng cache() + count() một lần duy nhất
# ngay đầu foreachBatch, sau đó reuse cached result cho write.
# ══════════════════════════════════════════════════════════════════
def bq_append(batch_df: DataFrame, table: str, batch_id: int) -> None:
    """
    Ghi một batch lên BigQuery.
    - writeMethod=indirect: dùng GCS staging (bucket đã set ở session level)
    - cache() → count() + write() đọc cùng một materialized result → không tính lại 2 lần
    - unpersist() ngay sau để giải phóng memory executor
    """
    batch_df.cache()
    n = batch_df.count()
    if n > 0:
        label = table.split(".")[-1]
        layer = table.split(".")[1].split("_")[0].upper()
        print(f"  [{layer}][Batch {batch_id}] {label}: {n:,} dòng")
        batch_df.write \
            .format("bigquery") \
            .option("table", table) \
            .option("writeMethod", "indirect") \
            .option("intermediateFormat", "avro") \
            .option("useAvroLogicalTypes", "true") \
            .mode("append") \
            .save()
    batch_df.unpersist()

# ══════════════════════════════════════════════════════════════════
# CORE ARCHITECTURE
#
# Thay vì mở 15+ Kafka streams riêng biệt (1 Bronze +
# 7 Silver + 7 Gold), bây giờ mỗi topic chỉ mở ĐÚNG 1 stream.
#
# Toàn bộ Bronze + Silver + Gold logic đều chạy bên trong MỘT
# foreachBatch handler duy nhất cho mỗi topic. Trong foreachBatch,
# batch_df là một DataFrame tĩnh (static) nên có thể:
#   1. Ghi Bronze (raw json_str)
#   2. Parse, validate → ghi Silver
#   3. Apply business logic → ghi Gold
# Tất cả trong cùng một lần xử lý batch.
#
# Kết quả: 7 streams thay vì 15+. Tiết kiệm ~50% Kafka consumer
# connections, ~50% scheduler overhead, ~50% BigQuery round-trips.
# ══════════════════════════════════════════════════════════════════

def build_pipeline(topic: str, schema: StructType,
                   valid_event_types: list[str]) -> "StreamingQuery":
    """
    Tạo một streaming query duy nhất cho một topic.
    Trong foreachBatch: Bronze → Silver → Gold được xử lý tuần tự.
    """

    # ── Đọc Kafka (chỉ 1 lần cho mỗi topic) ──────────────────────
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        # maxOffsetsPerTrigger: giới hạn số message mỗi batch
        # để tránh một batch quá lớn làm vỡ memory
        .option("maxOffsetsPerTrigger", 5000)
        .load()
        .select(
            col("topic"),
            col("partition").cast(IntegerType()).alias("partition"),
            col("offset").cast(LongType()).alias("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("value").cast(StringType()).alias("json_str"),
        )
    )

    checkpoint = f"/app/4_checkpoints/{topic.replace('.', '_')}"

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        # Nếu batch rỗng thì bỏ qua toàn bộ — tránh BigQuery write với 0 dòng
        if batch_df.isEmpty():
            return

        # ── BRONZE ────────────────────────────────────────────────
        # Lưu raw payload + Kafka metadata, không parse.
        bronze_df = batch_df.select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            col("json_str"),                      # Raw, untouched
            current_timestamp().alias("ingested_at"),
        )
        bq_append(bronze_df, f"{BQ_BRONZE}.kafka_raw_events", batch_id)

        # ── PARSE JSON ────────────────────────────────────────────
        # Parse một lần, dùng lại cho cả Silver và Gold.
        parsed_df = (
            batch_df
            .select(
                from_json(col("json_str"), schema).alias("d"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("kafka_timestamp"),
            )
            .select("d.*", "topic", "partition", "offset", "kafka_timestamp")
            .withColumn("ingested_at", current_timestamp())
        )

        # ── DEDUP (trong batch) ───────────────────────────────────
        # dropDuplicates trên static batch DF (không cần watermark
        # vì đây là batch processing, bukan streaming transform)
        parsed_df = parsed_df.dropDuplicates(["event_id"])

        # ── SPLIT GOOD / BAD ──────────────────────────────────────
        is_valid = (
            col("event_id").isNotNull() &
            col("timestamp").isNotNull() &
            col("user_id").isNotNull() &
            col("event_type").isin(valid_event_types)
        )
        good_df = parsed_df.filter(is_valid)
        bad_df  = parsed_df.filter(~is_valid).select(
            col("event_id"),
            col("event_type"),
            col("timestamp"),
            col("user_id"),
            lit(topic).alias("source_topic"),
            when(col("event_id").isNull(),   lit("null event_id"))
            .when(col("timestamp").isNull(), lit("null timestamp"))
            .when(col("user_id").isNull(),   lit("null user_id"))
            .otherwise(lit("unknown event_type")).alias("quarantine_reason"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            col("ingested_at"),
        )

        # ── SILVER QUARANTINE ─────────────────────────────────────
        bq_append(bad_df, f"{BQ_SILVER}.quarantine_events", batch_id)

        # Nếu không có event hợp lệ thì dừng ở đây
        if good_df.isEmpty():
            return

        # ── SILVER + GOLD (theo từng topic) ──────────────────────
        _silver_and_gold(topic, good_df, batch_id)

    return (
        raw_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


def _silver_and_gold(topic: str, good_df: DataFrame, batch_id: int) -> None:
    """
    Áp dụng Silver cleaning + Gold business logic cho batch_df đã validated.
    Tách ra hàm riêng để process_batch gọn hơn.
    """

    # ══════════════════════════════════════════════════════════════
    # TOPIC: ecommerce.user
    # ══════════════════════════════════════════════════════════════
    if topic == TOPIC_USER:
        # Silver
        sv = good_df.select(
            trim(col("event_id")).alias("event_id"),
            trim(col("event_type")).alias("event_type"),
            col("timestamp").cast("timestamp").alias("event_ts"),
            trim(col("session_id")).alias("session_id"),
            trim(col("user_id")).alias("user_id"),
            trim(col("user_name")).alias("user_name"),
            trim(lower(col("username"))).alias("username"),
            trim(col("gender")).alias("gender"),
            trim(col("location")).alias("location"),
            trim(col("device_type")).alias("device_type"),
            trim(col("os")).alias("os"),
            col("topic"), col("partition"), col("offset"),
            col("kafka_timestamp"), col("ingested_at"),
        )
        bq_append(sv, f"{BQ_SILVER}.user_events", batch_id)

        # Gold
        gold = sv.select(
            col("event_id"), col("event_type"),
            col("user_id"), col("username"), col("gender"),
            col("location"), col("device_type"), col("os"),
            col("session_id"),
            to_date(col("event_ts")).alias("date_key"),
            col("event_ts"),
            current_timestamp().alias("dw_loaded_at"),
        )
        bq_append(gold, f"{BQ_GOLD}.fact_user_events", batch_id)

    # ══════════════════════════════════════════════════════════════
    # TOPIC: ecommerce.page_view
    # ══════════════════════════════════════════════════════════════
    elif topic == TOPIC_PAGE_VIEW:
        sv = good_df.select(
            trim(col("event_id")).alias("event_id"),
            trim(col("event_type")).alias("event_type"),
            col("timestamp").cast("timestamp").alias("event_ts"),
            trim(col("session_id")).alias("session_id"),
            trim(col("user_id")).alias("user_id"),
            trim(col("device_type")).alias("device_type"),
            trim(col("os")).alias("os"),
            trim(col("location")).alias("location"),
            trim(col("page_name")).alias("page_name"),
            trim(col("product_id")).alias("product_id"),
            trim(col("product_name")).alias("product_name"),
            trim(col("category")).alias("category"),
            trim(col("brand")).alias("brand"),
            col("price").cast(IntegerType()).alias("price"),
            trim(col("search_term")).alias("search_term"),
            col("results_count").cast(IntegerType()).alias("results_count"),
            trim(col("trigger")).alias("trigger"),
            col("topic"), col("partition"), col("offset"),
            col("kafka_timestamp"), col("ingested_at"),
        )
        bq_append(sv, f"{BQ_SILVER}.page_view_events", batch_id)

        gold = sv.select(
            col("event_id"), col("event_type"),
            col("user_id"), col("session_id"),
            col("device_type"), col("os"), col("location"),
            col("page_name"), col("product_id"), col("product_name"),
            col("category"), col("brand"), col("price"),
            col("search_term").alias("search_item"),
            col("results_count").alias("result_count"),
            col("trigger"),
            to_date(col("event_ts")).alias("date_key"),
            col("event_ts"),
            current_timestamp().alias("dw_loaded_at"),
        )
        bq_append(gold, f"{BQ_GOLD}.fact_page_views", batch_id)

    # ══════════════════════════════════════════════════════════════
    # TOPIC: ecommerce.cart
    # ══════════════════════════════════════════════════════════════
    elif topic == TOPIC_CART:
        sv = good_df.select(
            trim(col("event_id")).alias("event_id"),
            trim(col("event_type")).alias("event_type"),
            col("timestamp").cast("timestamp").alias("event_ts"),
            trim(col("session_id")).alias("session_id"),
            trim(col("user_id")).alias("user_id"),
            trim(col("device_type")).alias("device_type"),
            trim(col("os")).alias("os"),
            trim(col("product_id")).alias("product_id"),
            trim(col("product_name")).alias("product_name"),
            trim(col("brand")).alias("brand"),
            trim(col("category")).alias("category"),
            col("unit_price").cast(IntegerType()).alias("unit_price"),
            col("quantity").cast(IntegerType()).alias("quantity"),
            col("cart_quantity_after").cast(IntegerType()).alias("cart_quantity_after"),
            col("old_quantity").cast(IntegerType()).alias("old_quantity"),
            col("new_quantity").cast(IntegerType()).alias("new_quantity"),
            col("topic"), col("partition"), col("offset"),
            col("kafka_timestamp"), col("ingested_at"),
        )
        bq_append(sv, f"{BQ_SILVER}.cart_events", batch_id)

        gold = sv.select(
            col("event_id"), col("event_type"),
            col("user_id"), col("session_id"),
            col("device_type"), col("os"),
            col("product_id"), col("product_name"),
            col("brand"), col("category"), col("unit_price"),
            when(col("event_type") == "item_added",        col("quantity"))
            .when(col("event_type") == "item_qty_updated", col("new_quantity"))
            .otherwise(lit(None)).cast(IntegerType()).alias("quantity"),
            col("cart_quantity_after"),
            col("old_quantity"), col("new_quantity"),
            to_date(col("event_ts")).alias("date_key"),
            col("event_ts"),
            current_timestamp().alias("dw_loaded_at"),
        )
        bq_append(gold, f"{BQ_GOLD}.fact_cart_events", batch_id)

    # ══════════════════════════════════════════════════════════════
    # TOPIC: ecommerce.promo
    # ══════════════════════════════════════════════════════════════
    elif topic == TOPIC_PROMO:
        sv = good_df.select(
            trim(col("event_id")).alias("event_id"),
            trim(col("event_type")).alias("event_type"),
            col("timestamp").cast("timestamp").alias("event_ts"),
            trim(col("session_id")).alias("session_id"),
            trim(col("user_id")).alias("user_id"),
            trim(col("promo_id")).alias("promo_id"),
            upper(trim(col("promo_code"))).alias("promo_code"),
            col("discount_percent").cast("float").alias("discount_percent"),
            col("topic"), col("partition"), col("offset"),
            col("kafka_timestamp"), col("ingested_at"),
        )
        bq_append(sv, f"{BQ_SILVER}.promo_events", batch_id)

        gold = sv.select(
            col("event_id"), col("event_type"),
            col("user_id"), col("session_id"),
            col("promo_id"), col("promo_code"), col("discount_percent"),
            when(col("event_type") == "promo_applied", True).otherwise(False).alias("is_applied"),
            when(col("event_type") == "promo_invalid", True).otherwise(False).alias("is_invalid"),
            to_date(col("event_ts")).alias("date_key"),
            col("event_ts"),
            current_timestamp().alias("dw_loaded_at"),
        )
        bq_append(gold, f"{BQ_GOLD}.fact_promo_events", batch_id)

    # ══════════════════════════════════════════════════════════════
    # TOPIC: ecommerce.order
    # ══════════════════════════════════════════════════════════════
    elif topic == TOPIC_ORDER:
        sv = good_df.select(
            trim(col("event_id")).alias("event_id"),
            trim(col("event_type")).alias("event_type"),
            col("timestamp").cast("timestamp").alias("event_ts"),
            trim(col("session_id")).alias("session_id"),
            trim(col("user_id")).alias("user_id"),
            trim(col("device_type")).alias("device_type"),
            trim(col("os")).alias("os"),
            trim(col("location")).alias("location"),
            trim(col("order_id")).alias("order_id"),
            col("subtotal").cast(IntegerType()).alias("subtotal"),
            col("shipping_fee").cast(IntegerType()).alias("shipping_fee"),
            coalesce(col("discount_amount"), lit(0)).cast(IntegerType()).alias("discount_amount"),
            col("total_amount").cast(IntegerType()).alias("total_amount"),
            trim(col("payment_method")).alias("payment_method"),
            trim(col("promo_id")).alias("promo_id"),
            upper(trim(col("promo_code"))).alias("promo_code"),
            trim(col("order_status")).alias("order_status"),
            trim(col("reason")).alias("reason"),
            col("items"),
            col("topic"), col("partition"), col("offset"),
            col("kafka_timestamp"), col("ingested_at"),
        )
        bq_append(sv, f"{BQ_SILVER}.order_events", batch_id)

        # Gold — fact_orders (all event types, 1 row per event)
        fact_orders = sv.select(
            col("event_id"), col("event_type"),
            col("order_id"), col("user_id"), col("session_id"),
            col("device_type"), col("os"), col("location"),
            col("promo_id"), col("promo_code"),
            col("payment_method"), col("order_status"),
            col("subtotal"), col("shipping_fee"),
            col("discount_amount"), col("total_amount"),
            # net_revenue: order_paid only sends total_amount (no shipping_fee) → guard with coalesce
            coalesce(col("total_amount") - col("shipping_fee"), col("total_amount"), lit(0)).cast(IntegerType()).alias("net_revenue"),
            col("reason"),
            when(col("event_type") == "order_created",   True).otherwise(False).alias("is_new_order"),
            when(col("event_type") == "order_cancelled", True).otherwise(False).alias("is_cancelled"),
            when(col("event_type") == "order_returned",  True).otherwise(False).alias("is_returned"),
            to_date(col("event_ts")).alias("date_key"),
            col("event_ts"),
            current_timestamp().alias("dw_loaded_at"),
        )
        bq_append(fact_orders, f"{BQ_GOLD}.fact_orders_stream", batch_id)

        # Gold — fact_order_items + fact_sales (only order_created has items array)
        created_df = sv.filter(col("event_type") == "order_created")
        if not created_df.isEmpty():
            exploded = created_df.withColumn("item", explode(col("items")))

            # fact_order_items
            fact_oi = exploded.select(
                col("event_id").alias("source_event_id"),
                col("order_id"), col("user_id"),
                col("promo_id"), col("order_status"), col("payment_method"),
                col("item.product_id").alias("product_id"),
                col("item.product_name").alias("product_name"),
                col("item.quantity").alias("quantity"),
                col("item.unit_price").alias("unit_price"),
                col("item.unit_cost").alias("unit_cost"),
                (col("item.quantity") * col("item.unit_price")).alias("item_total"),
                (col("item.unit_price") - col("item.unit_cost")).alias("unit_gross_profit"),
                ((col("item.unit_price") - col("item.unit_cost")) * col("item.quantity")).alias("line_gross_profit"),
                spark_round(
                    (col("item.unit_price") - col("item.unit_cost")) / col("item.unit_price") * 100, 2
                ).alias("gross_margin_pct"),
                to_date(col("event_ts")).alias("date_key"),
                col("event_ts").alias("order_ts"),
                current_timestamp().alias("dw_loaded_at"),
            )
            bq_append(fact_oi, f"{BQ_GOLD}.fact_order_items_stream", batch_id)

            # fact_sales (with discount allocation + full profit metrics per line item)
            fact_sales = exploded.select(
                col("event_id").alias("source_event_id"),
                col("order_id"), col("user_id"),
                col("promo_id"), col("promo_code"),
                col("payment_method"), col("order_status"),
                col("item.product_id").alias("product_id"),
                col("item.product_name").alias("product_name"),
                col("item.quantity").alias("quantity"),
                col("item.unit_price").alias("unit_price"),
                col("item.unit_cost").alias("unit_cost"),
                # Gross revenue = qty * unit_price (before discount)
                (col("item.quantity") * col("item.unit_price")).alias("gross_revenue"),
                # Allocated discount: proportional share of order-level discount
                when(
                    col("subtotal") > 0,
                    spark_round(
                        col("discount_amount")
                        * (col("item.quantity") * col("item.unit_price"))
                        / col("subtotal"), 0
                    )
                ).otherwise(lit(0)).cast(IntegerType()).alias("allocated_discount"),
                # Net revenue = gross_revenue - allocated_discount
                when(
                    col("subtotal") > 0,
                    spark_round(
                        (col("item.quantity") * col("item.unit_price"))
                        - col("discount_amount")
                          * (col("item.quantity") * col("item.unit_price"))
                          / col("subtotal"), 0
                    )
                ).otherwise(
                    col("item.quantity") * col("item.unit_price")
                ).cast(IntegerType()).alias("net_revenue"),
                # Total cost = unit_cost * quantity
                (col("item.unit_cost") * col("item.quantity")).alias("total_cost"),
                # Gross profit = net_revenue - total_cost
                when(
                    col("subtotal") > 0,
                    spark_round(
                        (col("item.quantity") * col("item.unit_price"))
                        - col("discount_amount")
                          * (col("item.quantity") * col("item.unit_price"))
                          / col("subtotal")
                        - (col("item.unit_cost") * col("item.quantity")), 0
                    )
                ).otherwise(
                    (col("item.quantity") * col("item.unit_price"))
                    - (col("item.unit_cost") * col("item.quantity"))
                ).cast(IntegerType()).alias("gross_profit"),
                # Gross margin %
                spark_round(
                    when(
                        col("subtotal") > 0,
                        (
                            (col("item.quantity") * col("item.unit_price"))
                            - col("discount_amount")
                              * (col("item.quantity") * col("item.unit_price"))
                              / col("subtotal")
                            - (col("item.unit_cost") * col("item.quantity"))
                        ) / (
                            (col("item.quantity") * col("item.unit_price"))
                            - col("discount_amount")
                              * (col("item.quantity") * col("item.unit_price"))
                              / col("subtotal")
                        )
                    ).otherwise(
                        (col("item.unit_price") - col("item.unit_cost"))
                        / col("item.unit_price")
                    ) * 100, 2
                ).alias("gross_margin_pct"),
                # Order-level context
                col("subtotal").alias("order_subtotal"),
                col("shipping_fee"),
                col("discount_amount").alias("order_discount_amount"),
                col("total_amount").alias("order_total_amount"),
                to_date(col("event_ts")).alias("date_key"),
                col("event_ts").alias("order_ts"),
                current_timestamp().alias("dw_loaded_at"),
            )
            bq_append(fact_sales, f"{BQ_GOLD}.fact_sales_stream", batch_id)

    # ══════════════════════════════════════════════════════════════
    # TOPIC: ecommerce.inventory
    # ══════════════════════════════════════════════════════════════
    elif topic == TOPIC_INVENTORY:
        sv = good_df.select(
            trim(col("event_id")).alias("event_id"),
            trim(col("event_type")).alias("event_type"),
            col("timestamp").cast("timestamp").alias("event_ts"),
            trim(col("user_id")).alias("user_id"),
            trim(col("product_id")).alias("product_id"),
            trim(col("product_name")).alias("product_name"),
            trim(col("order_id")).alias("order_id"),
            col("quantity_sold").cast(IntegerType()).alias("quantity_sold"),
            col("quantity_returned").cast(IntegerType()).alias("quantity_returned"),
            col("quantity_added").cast(IntegerType()).alias("quantity_added"),
            col("stock_remaining").cast(IntegerType()).alias("stock_remaining"),
            col("topic"), col("partition"), col("offset"),
            col("kafka_timestamp"), col("ingested_at"),
        )
        bq_append(sv, f"{BQ_SILVER}.inventory_events", batch_id)

        gold = sv.select(
            col("event_id"), col("event_type"),
            col("product_id"), col("product_name"), col("order_id"),
            when(col("event_type") == "stock_updated_out",                 col("quantity_sold"))
            .when(col("event_type") == "stock_updated_in",                 col("quantity_returned"))
            .when(col("event_type").isin("restock_manual", "restock_auto"),col("quantity_added"))
            .otherwise(lit(None)).cast(IntegerType()).alias("quantity_changed"),
            when(col("event_type").isin("stock_updated_in",
                                        "restock_manual", "restock_auto"), lit(1))
            .when(col("event_type") == "stock_updated_out",                lit(-1))
            .otherwise(lit(0)).cast(IntegerType()).alias("stock_direction"),
            col("stock_remaining"),
            when(col("event_type") == "low_stock_alert",  True).otherwise(False).alias("is_alert"),
            when(col("event_type").isin("restock_manual", "restock_auto"), True)
            .otherwise(False).alias("is_restock"),
            to_date(col("event_ts")).alias("date_key"),
            col("event_ts"),
            current_timestamp().alias("dw_loaded_at"),
        )
        bq_append(gold, f"{BQ_GOLD}.fact_inventory_events", batch_id)

    # ══════════════════════════════════════════════════════════════
    # TOPIC: ecommerce.review
    # ══════════════════════════════════════════════════════════════
    elif topic == TOPIC_REVIEW:
        # Extra validation: rating phải nằm trong [1, 5]
        valid_reviews = good_df.filter(
            col("rating").isNotNull() &
            (col("rating") >= 1) & (col("rating") <= 5)
        )
        sv = valid_reviews.select(
            trim(col("event_id")).alias("event_id"),
            trim(col("event_type")).alias("event_type"),
            col("timestamp").cast("timestamp").alias("event_ts"),
            trim(col("user_id")).alias("user_id"),
            trim(col("product_id")).alias("product_id"),
            col("rating").cast(IntegerType()).alias("rating"),
            col("comment_length").cast(IntegerType()).alias("comment_length"),
            col("topic"), col("partition"), col("offset"),
            col("kafka_timestamp"), col("ingested_at"),
        )
        bq_append(sv, f"{BQ_SILVER}.review_events", batch_id)

        gold = sv.select(
            col("event_id"), col("event_type"),
            col("user_id"), col("product_id"), col("rating"),
            when(col("rating") >= 4, lit("Positive"))
            .when(col("rating") == 3, lit("Neutral"))
            .otherwise(lit("Negative")).alias("sentiment_label"),
            col("comment_length"),
            when(col("comment_length") > 0, True).otherwise(False).alias("has_comment"),
            to_date(col("event_ts")).alias("date_key"),
            col("event_ts"),
            current_timestamp().alias("dw_loaded_at"),
        )
        bq_append(gold, f"{BQ_GOLD}.fact_review_events", batch_id)


# ══════════════════════════════════════════════════════════════════
# START ALL PIPELINES
# One stream per topic — 7 streams total (down from 15+)
# ══════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("📡 Khởi động Kafka Streaming — Medallion Architecture")
print(f"   Trigger  : {TRIGGER_INTERVAL}")
print(f"   Bronze   → {BQ_BRONZE}")
print(f"   Silver   → {BQ_SILVER}")
print(f"   Gold     → {BQ_GOLD}")
print("="*60)

active_queries = []
for topic in ALL_TOPICS:
    schema = TOPIC_SCHEMAS[topic]
    valid_types = TOPIC_VALID_EVENT_TYPES[topic]
    q = build_pipeline(topic, schema, valid_types)
    active_queries.append(q)
    print(f"  ✅ Stream started: {topic}")

print("\n" + "="*60)
print(f"🌟 {len(active_queries)} streams running  (Bronze+Silver+Gold per stream)")
print("="*60)
print("""
  Flow per stream:
    Kafka → [foreachBatch]
                ├─ 🥉 Bronze : raw json_str → bronze.kafka_raw_events
                ├─ 🥈 Silver : parsed + validated → silver.<topic>_events
                │              bad events  → silver.quarantine_events
                └─ 🥇 Gold   : business logic → gold.fact_*
""")

spark.streams.awaitAnyTermination()