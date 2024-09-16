from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("temp").getOrCreate()

data = [("milind","keer")]
columns = ["first", "Second"]

df = spark.createDataFrame(data,columns)

df.show()

spark.stop

