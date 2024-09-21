from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("temp").getOrCreate()

df = spark.read.csv("/workspaces/Test/data/people.csv", header=True, inferSchema=True)
df.show(20)
df.printSchema()
df.select('id','name','age').show()

df_clean = df.na.drop()
df_clean.show()

df.createOrReplaceTempView("people")

df_clean = spark.sql("Select * from people where age is not null")
df_clean.show()

spark.stop

