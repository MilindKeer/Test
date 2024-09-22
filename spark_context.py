from pyspark import SparkContext
# from pyspark.sql import SQLContext

sc = SparkContext.getOrCreate()

# Read text file
lines_rdd = sc.textFile("data/pride_and_prejudice.txt")
# Collect the lines as a list
lines_list = lines_rdd.collect()
# Print each line
for line in lines_list:
    print(line)

# Split each line into words and convert to lower case
words_rdd = lines_rdd.flatMap(lambda line: line.strip().split(" ")).map(lambda word: word.lower())

# You can't directly use Spark SQL functions on RDDs
# However, you can achieve similar transformations using map and filter

# Example: Filter out empty words
filtered_words_rdd = words_rdd.filter(lambda word: len(word) > 0)

# Example: Count word occurrences (similar to groupBy and count)
word_counts_rdd = words_rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# Collect results as a list (optional)
word_counts_list = word_counts_rdd.collect()

# Print the results
# for word, count in word_counts_list:
#     print(f"Word: {word}, Count: {count}")