from pyspark.sql import SparkSession

# spark = SparkSession.builder.appname("temp").getorcreate()
# spark = SparkSession.builder.appName("HelloWorldApp").getOrCreate()

spark = SparkSession.builder \
    .appName("HelloWorldApp") \
    .getOrCreate()


data = [("hello", "word")]
columns = ["greeting","target"]

# df = spark.createDataFrame(data, columns)
df = spark.createDataFrame(data,columns)

df.show()

spark.stop

