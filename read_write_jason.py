from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.functions import avg

spark = SparkSession.builder.appName("temp").getOrCreate()

data = [{"date":"01-01-2024", "stock":"ABC", "price": 150},
      {"date":"01-01-2024", "stock":"ABC", "price": 155},
      {"date":"01-02-2024", "stock":"ABC", "price": 150},
      {"date":"01-02-2024", "stock":"ABC", "price": 200}
      ]

df = spark.createDataFrame(data)

df.printSchema()
df.show()

df1 = df.withColumn("date", to_date("date","dd-MM-yyyy"))
df1.show()

df_avg = df1.groupBy("date", "stock").agg(avg("price").alias("avg_price"))
df_avg.show()                                          
