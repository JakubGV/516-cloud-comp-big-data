from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Spark_Data_Types") \
    .getOrCreate()

sc = spark.sparkContext

# Read friends file and get it as (key, value)
friends = sc.textFile('./friends_v2.txt')
friends = friends.map(lambda line: line.split(' '))

# Get friends as (key, value) where value is now a list of all key's friends
def to_list(x):
    return [x]
def append(x, y):
    x.append(y)
    return x
def extend(x, y):
    x.extend(y)
    return x

friends = friends.combineByKey(to_list, append, extend)

# Read criminal record file
criminals = sc.textFile('./criminal_records_v2.txt')
criminals = criminals.map(lambda line: (line.split(' ')[0], line.split(' ')[1:]))

# Add the accomplices as criminals and only get their names as a key
def add_accomplice(r):
    names = [r[0]]           
    accomplices = r[1][2]
    if accomplices != 'NULL':
        names.extend(accomplices.split(','))

    return names
criminals = criminals.flatMap(add_accomplice)

# Remove duplicate names in criminals, keeping only unique criminal key names
criminals = criminals.distinct().collect()

# Combine into a friends, criminals RDD, having key:name, (friends, criminals)
combined = friends.map(lambda r: (r[0], (r[1], criminals)))

# Calculate percentage of friends who have criminal records
def count_criminals(r):
    name_key = r[0]
    friends = r[1][0]
    criminals = r[1][1]

    num_criminals = 0
    for friend in friends:
        if friend in criminals:
            num_criminals += 1

    return (name_key, (friends, criminals, num_criminals / len(friends)))

combined = combined.map(count_criminals)

# Find the citizens who are at risk by filtering
at_risk = combined.filter(lambda r: r[1][2] > 0.5) \
            .map(lambda r: r[0]) \
            .collect()

print(at_risk)