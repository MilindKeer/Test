from pyspark.sql import SparkSession

# spark = SparkSession.builder.appname("temp").getorcreate()
# spark = SparkSession.builder.appName("HelloWorldApp").getOrCreate()

spark = SparkSession.builder \
    .appName("HelloWorldApp") \
    .getOrCreate()


data = [("hello", "word"),("milind","Keer")]
columns = ["greeting","target"]
df = spark.createDataFrame(data,columns)
df.show()

data = [["milind","Keer"]]
columns = ["col1","col2"]
df=spark.createDataFrame(data)
df.show()

data = [{"col12":"milind","col21":"keer"}]
df=spark.createDataFrame(data)
df.show()

# data = spark.read.csv("data/temp.csv")
df = spark.read.csv('data/temp.csv', header=True, inferSchema=True)
df.show()


spark.stop

