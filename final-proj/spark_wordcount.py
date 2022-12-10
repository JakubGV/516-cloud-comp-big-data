from pyspark.sql import SparkSession
from operator import add

FILENAME = 'pride_prej.txt'

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Word_Freq") \
    .getOrCreate()

# Read file
lines = spark.sparkContext.textFile('./' + FILENAME)

# MapReduce to get (k ,v) of (word, count)
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
counts =  pairs.reduceByKey(add)

# Sort by frequency (greatest -> smallest)
counts_desc = counts.sortBy(lambda x: x[1], ascending=False)

# Collect and print top 5 most frequent
r = counts_desc.take(5)
print(r)