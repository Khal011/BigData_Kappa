from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import functions as F



if __name__ == "__main__":
    # Initialize the SparkSession
    spark = SparkSession.builder \
        .appName("KafkaStreamProcessing") \
        .config("spark.cassandra.connection.host", "localhost") \
        .config("spark.cassandra.connection.port", "9042") \
        .master("spark://localhost:7077") \
        .getOrCreate()

    # Define the schema for your Kafka message
    schema = StructType([
        StructField("invoice_no", IntegerType()),
        StructField("stock_code", StringType()),
        StructField("description", StringType()),
        StructField("quantity", IntegerType()),
        StructField("invoice_date", StringType()),
        StructField("unit_price", FloatType()),
        StructField("customer_id", IntegerType()),
        StructField("country", StringType())
    ])

    # Read the stream from Kafka topic "sales_topic"
    kafka_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092")  \
        .option("subscribe", "sales_topic") \
        .load()

    # The Kafka message is in 'value' column, so we extract it as JSON and parse it
    json_data = kafka_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Show the data schema to verify the incoming data
    json_data.printSchema()

    # Aggregating the total quantity sold per stock_code
    agg_stream = json_data.groupBy("stock_code").agg(
        F.sum("quantity").alias("total_quantity"),
        F.avg("unit_price").alias("avg_unit_price")
    )

    # Aggregating over 5-minute windows with watermarking
    agg_stream_with_window = json_data \
        .withWatermark("invoice_date", "10 minutes") \
        .groupBy(F.window(col("invoice_date"), "5 minutes"), "stock_code") \
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.avg("unit_price").alias("avg_unit_price")
        )

    # Define output Cassandra settings (replace 'sales_data' with your keyspace and 'aggregated_sales' with your table)
    agg_stream_with_window \
        .writeStream \
        .outputMode("update") \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "sales_data") \
        .option("table", "aggregated_sales") \
        .start()

    # Start the streaming query to process data from Kafka and write to Cassandra
    spark.streams.awaitAnyTermination()

