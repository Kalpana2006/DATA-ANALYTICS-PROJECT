# spark/traffic_streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField('sensor_id', StringType()),
    StructField('location_id', StringType()),
    StructField('lat', DoubleType()),
    StructField('lon', DoubleType()),
    StructField('vehicle_count', IntegerType()),
    StructField('avg_speed', DoubleType()),
    StructField('timestamp', StringType()),
])

spark = SparkSession.builder.appName('TrafficStreaming') \.getOrCreate()

raw = spark.readStream.format('kafka')             .option('kafka.bootstrap.servers', 'localhost:9092')             .option('subscribe', 'traffic_stream')             .option('startingOffsets', 'latest')             .load()

json_str = raw.selectExpr('CAST(value AS STRING) as json')
parsed = json_str.select(from_json(col('json'), schema).alias('data')).select('data.*')

from pyspark.sql.functions import to_timestamp
parsed = parsed.withColumn('event_time', to_timestamp(col('timestamp')))

agg = parsed.groupBy(
    window(col('event_time'), '1 minute', '30 seconds'),
    col('location_id')
).agg(
    spark_sum('vehicle_count').alias('vehicle_count_sum'),
    avg('avg_speed').alias('avg_speed_mean')
)

from pyspark.sql.functions import expr
result = agg.withColumn('congestion_index', expr('vehicle_count_sum / NULLIF(avg_speed_mean, 0)'))

console_q = result.writeStream.outputMode('update').format('console').option('truncate', False).start()
console_q.awaitTermination()
