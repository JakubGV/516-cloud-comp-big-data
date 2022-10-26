from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Word_Freq") \
    .getOrCreate()

sc = spark.sparkContext

# Read file
lines = sc.textFile('./file01_Hd_Sp_Freq')

# Filter lines that start with "I"
selfish = lines.filter(lambda x: x.startswith('I'))

# Split up the lines on tabs and get the second part
messages = selfish.map(lambda x: x.split("\t")).map(lambda r: r[1])

messages.cache()

# Action 1
messages.filter(lambda x: "Spark" in x).count()