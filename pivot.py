from pyspark.sql import SparkSession
import pandas as ps
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("temp1").getOrCreate()

df = ps.DataFrame({'foo': ['one', 'one', 'one', 'two', 'two',
                           'two'],
                   'bar': ['A', 'B', 'C', 'A', 'B', 'C'],
                   'baz': [1, 2, 3, 4, 5, 6],
                   'zoo': ['x', 'y', 'z', 'q', 'w', 't']},
                  columns=['foo', 'bar', 'baz', 'zoo'])



df = spark.createDataFrame(df)
df.printSchema()
df.show()  

# df_pivot = df.pivot(index='foo', columns='bar', values='baz').sort_index()
df_pivot = df.groupBy("foo").pivot("bar").agg({"baz": "first"}).sort("foo")
df_pivot.show()




