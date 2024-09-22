from pyspark.sql import SparkSession
from pyspark.sql.functions import split,explode, lower, regexp_extract
from pyspark.sql.functions import desc

spark = SparkSession.builder.appName("temp").getOrCreate()

file_path = "data/pride_and_prejudice.txt"

df = spark.read.text(file_path)
# df.printSchema()
# df.show()

df = df.select(split("value"," ").alias("line"))
df = df.select(explode("line").alias("words"))
df = df.select(lower("words").alias("lower_words"))
# for r in df:
#     print(r.lower_words)

df = df.select(regexp_extract("lower_words", "[A-Za-z]+", 0).alias("word")).collect()
# print(df)
# df.show()
for r in df:
    print(r.word)



# df = df.groupBy("word").count()
# # df = df.orderBy(desc("count"))  # Order by count in descending order
# # df.show(10)  # Show top 10 rows
# df.orderBy("count",ascending=False).show(10)


















