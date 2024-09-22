from pyspark.sql import SparkSession
from pyspark.sql.functions import when

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
# df.show()

data = [{"col12":"milind","col21":"keer"}]
df=spark.createDataFrame(data)
# df.show()

# data = spark.read.csv("data/temp.csv")
# df = spark.read.csv('data/temp.csv', header=True, inferSchema=True)
df = spark.read.csv("data/people.csv", header = True)
df.printSchema()
# df.show()
# df = df.select(df.id,df.name)
# df = df.dropDuplicates(["name"])
# df = df.select(df.id,df.name).show()

# df1 = df.filter(df.department.isNotNull())
# df1.show()
# df1.describe().show()
# print(df1)


df =  df.withColumn("age_stage", when(df.age<18,"teenager").when(df.age >= 18,"adult").otherwise("age_missing"))
df.show()

spark.stop

