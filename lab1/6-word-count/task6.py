from pyspark.sql import SparkSession
from operator import add

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Word_Freq") \
    .getOrCreate()

lines = spark.sparkContext.textFile('./file01_Hd_Sp_Freq')

# Get counts in the form of (word, count)
counts = lines.flatMap(lambda line: line.split(" ")) \
                .map(lambda word: (word, 1)) \
                .reduceByKey(add)

# Collect
output = counts.collect()

# Sort the words by frequency and get the top 3
top3 = sorted(output, key=lambda x: x[1], reverse=True)[:3]

# Shows [('I', 10), ('to', 8), ('Hadoop', 6)]
print(top3)

spark.stop()