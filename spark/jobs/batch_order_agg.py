from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum, count

spark = SparkSession.builder \
    .appName("Order Batch Aggregation") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

orders_df = spark.read.json("s3a://raw-events/orders/")

orders_df = orders_df \
    .withColumn("order_date", to_date(col("timestamp"))) \
    .withColumn("total_amount", col("total_amount").cast("double"))

daily_orders = orders_df.groupBy(
    "order_date", "order_id"
).agg(
    count("*").alias("order_count"),
    _sum("total_amount").alias("total_revenue")
)

daily_orders.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/analytics") \
    .option("dbtable", "daily_product_orders") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

spark.stop()
