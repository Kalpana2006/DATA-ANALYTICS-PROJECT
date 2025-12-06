# Databricks notebook (py) - Traffic Streaming
# Use this file as a Databricks importable .py notebook where cells are split by '# COMMAND ----------'

# COMMAND ----------
from pyspark.sql.functions import from_json, col, window, avg, sum as spark_sum, to_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------
schema = StructType([
    StructField('sensor_id', StringType()),
    StructField('location_id', StringType()),
    StructField('lat', DoubleType()),
    StructField('lon', DoubleType()),
    StructField('vehicle_count', IntegerType()),
    StructField('avg_speed', DoubleType()),
    StructField('timestamp', StringType()),
])

# COMMAND ----------
kafka_bootstrap = 'kafka:9092'  # change to your cluster's bootstrap servers in Databricks

raw = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', kafka_bootstrap) \
    .option('subscribe', 'traffic_stream') \
    .option('startingOffsets', 'latest') \
    .load()

json_str = raw.selectExpr('CAST(value AS STRING) as json')
parsed = json_str.select(from_json(col('json'), schema).alias('data')).select('data.*')
parsed = parsed.withColumn('event_time', to_timestamp(col('timestamp')))

# COMMAND ----------
agg = parsed.groupBy(
    window(col('event_time'), '1 minute', '30 seconds'),
    col('location_id')
).agg(
    spark_sum('vehicle_count').alias('vehicle_count_sum'),
    avg('avg_speed').alias('avg_speed_mean')
)

result = agg.withColumn('congestion_index', expr('vehicle_count_sum / NULLIF(avg_speed_mean, 0)'))

# COMMAND ----------
displayStream = result.writeStream.format('console').outputMode('update').start()
displayStream.awaitTermination()
