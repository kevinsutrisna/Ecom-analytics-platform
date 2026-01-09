from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .appName("Daily Click Aggregation") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set(
    "fs.s3a.endpoint", "http://minio:9000"
)
spark._jsc.hadoopConfiguration().set(
    "fs.s3a.access.key", "minioadmin"
)
spark._jsc.hadoopConfiguration().set(
    "fs.s3a.secret.key", "minioadmin"
)
spark._jsc.hadoopConfiguration().set(
    "fs.s3a.path.style.access", "true"
)
spark._jsc.hadoopConfiguration().set(
    "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
)

df = spark.read.json("s3a://raw-events/clicks/")

daily_clicks = (
    df.filter(col("event_type") == "click")
      .withColumn("event_date", to_date(col("timestamp")))
      .groupBy("event_date", "product_id")
      .count()
      .withColumnRenamed("count", "total_clicks")
)

daily_clicks.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/analytics") \
    .option("dbtable", "daily_product_clicks") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

spark.stop()
