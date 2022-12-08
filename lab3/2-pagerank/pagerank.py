from pyspark.sql import SparkSession
from operator import add

N = 10
file = 'example1.txt'

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Spark_Data_Types") \
    .getOrCreate()

sc = spark.sparkContext

# example1.txt holds the example1 graph as (pageid, linklist)
# example2.txt holds the example2 graph as (pageid, linklist), with 1->4 going clockwise from the top node
links = sc.textFile('./' + file)
# Change the lines to key value pairs of (pageid, linklist)
links = links.map(lambda line: (line.split(" ")[0], line.split(" ")[1:]))
# Partition and persist
links = links.partitionBy(5).persist()

# Initialize ranks to 1.0
ranks = links.mapValues(lambda _: 1.0)

# Run N iterations of PageRank
def send_contributions(page):
    page_id = page[0]
    links = page[1][0]
    rank = page[1][1]

    return map(lambda dest: (dest, rank / len(links)), links)

for i in range(N):
    contributions = links.join(ranks).flatMap(send_contributions)
    ranks = contributions.reduceByKey(add).mapValues(lambda x: 0.15 + 0.85*x)

ranks.saveAsTextFile('ranks')