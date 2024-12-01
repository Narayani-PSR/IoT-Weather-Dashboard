from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType

spark = SparkSession.builder.appName("WeatherDataStreaming").getOrCreate()

kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "weather-data"

weather_schema = StructType().add("city", StringType()).add("temperature", FloatType()).add("humidity", FloatType())

weather_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap_servers).option("subscribe", kafka_topic).load()

weather_data = weather_stream.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), weather_schema).alias("data")).select("data.*")

query = weather_data.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
