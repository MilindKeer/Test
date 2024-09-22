from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, min

spark = SparkSession.builder.appName("randon").getOrCreate()

# from list n tuples
data = [
   ("Milind", "44", "Sutton"),
   ("Milind1", "20", "Sutton1"),
   ("Milind2", "31", "Sutton2"),
   ("Milind3", "23", "Sutton3"),
   ("Milind4", "44", "Sutton4"),
    ]
schema = ["name","age","city"]
df = spark.createDataFrame(data, schema = schema)
df.show()
df.printSchema()

#from dictionary /json

data = [ {"name" : "milind", "age": 44, "city": "sutton", "date": "01-01-2024"},
         {"name" : "milind", "age": 4, "city": "sutton1", "date": "01-Jan-2024"},   
         {"name" : "milind", "age": 45, "city": "sutton2", "date": "01-Jan-2024"},
         {"name" : "milind", "age": 21, "city": "sutton3", "date": "01-Jan-2024"},
         {"name" :None, "age": 21, "city": "sutton3", "date": "01-Jan-2024"},
         {"name" :None, "age": 20, "city": "sutton3", "date": "01-Jan-2024"},
        ]

df = spark.createDataFrame(data)
df.show()
df.printSchema()

df1 = df.withColumn("date1", to_date("date","dd-MMM-yyyy"))
df1.show()

df2 = df.select(to_date("date","dd-MMM-yyyy").alias("date1"))
df2.show()

df3 = df.withColumn("new_age", df.age * 10)
df3.show()

df4 = df.filter(col("age") > 20)
df4.show()

df4 = df.select(when(col("age") > 18, "adult").otherwise("young").alias("age_group"))
df4.show()


df5 = df.groupBy("name").avg("age")
df5.show()

df5 = df.groupBy("name").max("age")
df5.show()

df5 = df.groupBy("name").sum("age").alias("sum_age")
df5.show()

df5 = df.groupBy("name").agg(min(df.age).alias("min_age"))
df5.show()

df6 = df