from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("temp1").getOrCreate()

data1 = [
    {'PassengerId': 1, 'Name': 'Owen', 'Sex': 'male', 'Survived': 0},
    {'PassengerId': 2, 'Name': 'Florence', 'Sex': 'female', 'Survived': 1},
    {'PassengerId': 3, 'Name': 'Laina', 'Sex': 'female', 'Survived': 1},
    {'PassengerId': 4, 'Name': 'Lily', 'Sex': 'female', 'Survived': 1},
    {'PassengerId': 5, 'Name': 'William', 'Sex': 'male', 'Survived': 0}
]

data2 = [
    {'PassengerId': 1, 'Age': 22, 'Fare': 7.3, 'Pclass': 3},
    {'PassengerId': 2, 'Age': 38, 'Fare': 71.3, 'Pclass': 1},
    {'PassengerId': 3, 'Age': 26, 'Fare': 7.9, 'Pclass': 3},
    {'PassengerId': 4, 'Age': 35, 'Fare': 53.1, 'Pclass': 1},
    {'PassengerId': 5, 'Age': 35, 'Fare': 8.0, 'Pclass': 3}
]


df = spark.createDataFrame(data1)
df.printSchema()
df.show()

df1 = spark.createDataFrame(data2)
df1.printSchema()
df1.show()

# df2 = df.select("Name","PassengerId").show()
# col = ["Name","PassengerId"]
# df2 = df.select(col).show()

df = df.Groupby(df.sex).pivot()