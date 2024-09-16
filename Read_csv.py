from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("temp").getOrCreate()

df = spark.read.csv("data/recs2015_public_v4.csv", header=True, inferSchema=True)

df.show()

spark.stop
