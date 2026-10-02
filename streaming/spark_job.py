from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    max,
    sum,
    when,
    window,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    BooleanType,
)


spark = (
    SparkSession.builder
    .appName("ChickenTelemetryStreaming")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------
# TELEMETRY SCHEMA
# ---------------------------------------------------------

schema = StructType([
    StructField("timestamp", StringType()),
    StructField("service", StringType()),
    StructField("request_id", StringType()),
    StructField("trace_id", StringType()),
    StructField("endpoint", StringType()),
    StructField("method", StringType()),

    StructField("latency_ms", DoubleType()),
    StructField("status_code", IntegerType()),
    StructField("error", BooleanType()),

    StructField("cpu_percent", DoubleType()),
    StructField("memory_percent", DoubleType()),

    StructField("db_latency_ms", DoubleType()),
    StructField("db_connections", IntegerType()),
    StructField("db_error", BooleanType()),

    # Ground truth fields — evaluation only.
    StructField("failure_type", StringType()),
    StructField("incident_id", StringType()),
])


# ---------------------------------------------------------
# KAFKA
# ---------------------------------------------------------

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "telemetry")
    .option("startingOffsets", "latest")
    .load()
)


events = (
    raw
    .selectExpr("CAST(value AS STRING) AS json")
    .selectExpr(
        "from_json(json, '{}') AS event".format(
            schema.simpleString()
        )
    )
    .select("event.*")
    .withColumn(
        "event_time",
        col("timestamp").cast("timestamp")
    )
)


# ---------------------------------------------------------
# STREAMING FEATURES
#
# Lightweight aggregations are kept in the real-time path.
# ---------------------------------------------------------

features = (
    events
    .withWatermark("event_time", "5 seconds")
    .groupBy(
        window(col("event_time"), "2 seconds"),
        col("service")
    )
    .agg(

        # 1. Request volume
        count("*").alias("request_rate"),

        # 2. Average latency
        avg("latency_ms").alias("avg_latency"),

        # 3. Error rate
        avg(
            when(
                col("error") == True,
                1.0
            ).otherwise(0.0)
        ).alias("error_rate"),

        # 4. HTTP 5xx rate
        avg(
            when(
                col("status_code") >= 500,
                1.0
            ).otherwise(0.0)
        ).alias("http_5xx_rate"),

        # 5. CPU
        avg("cpu_percent").alias("cpu_percent"),

        # 6. Memory
        avg("memory_percent").alias("memory_percent"),

        # 7. Database latency
        avg("db_latency_ms").alias("db_latency"),

        # 8. Database connections
        avg("db_connections").alias(
            "db_connection_count"
        ),

        # 9. Database error rate
        avg(
            when(
                col("db_error") == True,
                1.0
            ).otherwise(0.0)
        ).alias("db_error_rate"),

        # 10. Maximum latency
        max("latency_ms").alias("max_latency"),

        # 11. Maximum CPU
        max("cpu_percent").alias("max_cpu"),

        # 12. Maximum memory
        max("memory_percent").alias("max_memory"),

        # 13. Error count
        sum(
            when(
                col("error") == True,
                1
            ).otherwise(0)
        ).alias("error_count"),

        # Latest event timestamp in this window.
        max("event_time").alias("latest_event_time"),

        # Ground truth — NOT used for detection.
        max("failure_type").alias(
            "ground_truth_failure"
        ),

        max("incident_id").alias(
            "ground_truth_incident"
        ),
    )
)


# ---------------------------------------------------------
# ANOMALY DETECTION
#
# Ground-truth fields are NOT used here.
# ---------------------------------------------------------

scored = (
    features

    # CPU spike
    .withColumn(
        "cpu_signal",
        when(
            col("cpu_percent") >= 85,
            1
        ).otherwise(0)
    )

    # Memory leak
    .withColumn(
        "memory_signal",
        when(
            col("memory_percent") >= 85,
            1
        ).otherwise(0)
    )

    # Database latency
    .withColumn(
        "db_latency_signal",
        when(
            col("db_latency") >= 200,
            1
        ).otherwise(0)
    )

    # Database connection exhaustion
    .withColumn(
        "db_connection_signal",
        when(
            col("db_connection_count") >= 80,
            1
        ).otherwise(0)
    )

    # Error spike
    .withColumn(
        "error_signal",
        when(
            col("error_rate") >= 0.30,
            1
        ).otherwise(0)
    )

    # Latency spike
    .withColumn(
        "latency_signal",
        when(
            col("avg_latency") >= 300,
            1
        ).otherwise(0)
    )

    # Traffic spike
    .withColumn(
        "traffic_signal",
        when(
            col("request_rate") >= 50,
            1
        ).otherwise(0)
    )

    # Overall anomaly score
    .withColumn(
        "anomaly_score",
        col("cpu_signal")
        + col("memory_signal")
        + col("db_latency_signal")
        + col("db_connection_signal")
        + col("error_signal")
        + col("latency_signal")
        + col("traffic_signal")
    )

    # Any signal = anomaly
    .withColumn(
        "predicted_anomaly",
        when(
            col("anomaly_score") >= 1,
            1
        ).otherwise(0)
    )

    # Spark processing timestamp.
    .withColumn(
        "detection_time",
        current_timestamp()
    )

    # End-to-end event-to-processing latency.
    .withColumn(
        "detection_latency_ms",
        (
            col("detection_time").cast("double")
            - col("latest_event_time").cast("double")
        ) * 1000
    )
)


# ---------------------------------------------------------
# OUTPUT
# ---------------------------------------------------------

output = scored.select(
    "window",
    "service",

    "request_rate",
    "avg_latency",

    "error_rate",
    "http_5xx_rate",

    "cpu_percent",
    "memory_percent",

    "db_latency",
    "db_connection_count",
    "db_error_rate",

    "max_latency",
    "max_cpu",
    "max_memory",

    "error_count",

    "anomaly_score",
    "predicted_anomaly",

    "latest_event_time",
    "detection_time",
    "detection_latency_ms",

    "ground_truth_failure",
    "ground_truth_incident",
)

# ---------------------------------------------------------
# STREAMING OUTPUT
# ---------------------------------------------------------

# Console output for visibility
console_query = (
    output
    .writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("numRows", 5)
    .option(
        "checkpointLocation",
        "/tmp/chicken-checkpoint-console"
    )
    .trigger(processingTime="1 second")
    .start()
)


# Kafka output for latency evaluation
kafka_query = (
    output
    .selectExpr(
        "to_json(struct(*)) AS value"
    )
    .writeStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "kafka:9092"
    )
    .option(
        "topic",
        "spark-detections"
    )
    .option(
        "checkpointLocation",
        "/tmp/chicken-checkpoint-detection-kafka"
    )
    .outputMode("update")
    .trigger(processingTime="1 second")
    .start()
)


console_query.awaitTermination()