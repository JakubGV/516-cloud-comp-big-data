from pyspark import SparkContext
import random
NUM_SAMPLES = 10000

sc = SparkContext(appName="Estimate_Pi")

def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

count = sc.parallelize(range(0, NUM_SAMPLES)) \
             .filter(inside).count()
print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))

# On one run, pi was estimated to be 3.138400