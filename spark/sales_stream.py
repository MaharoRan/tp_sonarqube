import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sales.orders")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sales")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sales")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "sales")

spark = (
    SparkSession.builder
    .appName("SalesStreamingPipeline")
    .getOrCreate()
)

schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("timestamp", TimestampType(), False),
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

orders = (
    raw.select(
        from_json(col("value").cast("string"), schema).alias("data")
    )
    .select("data.*")
    .filter((col("quantity") > 0) & (col("unit_price") > 0))
)


def write_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    output = batch_df.select(
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "total_amount",
        col("timestamp").alias("event_timestamp"),
    )

    def insert_partition(rows):
        import psycopg2

        connection = psycopg2.connect(
            host=POSTGRES_HOST,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        try:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO processed_orders (
                        order_id, customer_id, product_id, quantity,
                        unit_price, total_amount, event_timestamp
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_id) DO NOTHING
                    """,
                    [
                        (
                            row.order_id,
                            row.customer_id,
                            row.product_id,
                            row.quantity,
                            row.unit_price,
                            row.total_amount,
                            row.event_timestamp,
                        )
                        for row in rows
                    ],
                )
            connection.commit()
        finally:
            connection.close()

    output.rdd.foreachPartition(insert_partition)


query = (
    orders.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .option("checkpointLocation", "/tmp/sales-checkpoint")
    .start()
)

query.awaitTermination()
