from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, collect_set, approx_count_distinct, count

spark = SparkSession.builder.appName("temp").getOrCreate()

# ****************
# data = [("milind","keer")]
# columns = ["first", "Second"]
# df = spark.createDataFrame(data,columns)
# df.show()
# ****************


# ****************
# In and Out Dataframe

data = [('a', 'aa', 1), ('a', 'aa', 2), ('b', 'bb', 1), ('b', 'bb', 2), ('b', 'bb', 3) ]
schema = ["col1","col2", "col3"]

df = spark.createDataFrame(data,schema = schema)
# df.printSchema()
# df.show()

df_count = df.agg(count("col1").alias('count')).show()
df_count = df.agg(approx_count_distinct("col1").alias('distinct_values')).show()

df_cl = df.groupBy("col1","col2").agg(collect_list("col3")).alias("col3")
df_cl.show()

df_cs = df.groupBy("col1","col2").agg(collect_set("col3")).alias("col3")
df_cs.show()

# ****************

spark.stop

