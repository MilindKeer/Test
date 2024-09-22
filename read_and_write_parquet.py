from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("temp").getOrCreate()

source_file = "/workspaces/Test/data/sample3.parquet"
destination_file = "/workspaces/Test/data/sample3_no_dup.parquet"

df = spark.read.parquet(source_file,header=True,inferSchema=True)

# df.show()

df.createOrReplaceTempView("dup")
df_dup = spark.sql("select Count(*) from dup")
df_dup.show()

df_no_dup = df.dropDuplicates()
df_no_dup.show()

df_no_dup.write.parquet(destination_file, mode = 'overwrite')
df = spark.read.parquet(destination_file)
# df.show()

df.createOrReplaceTempView("no_dup")
df_no_dup = spark.sql("select Count(*) from no_dup")
df_no_dup.show()

