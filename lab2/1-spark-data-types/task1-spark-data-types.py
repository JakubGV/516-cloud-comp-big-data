from pyspark.sql import SparkSession
from pyspark.mllib.linalg.distributed import CoordinateMatrix, MatrixEntry

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("Spark_Data_Types") \
    .getOrCreate()

sc = spark.sparkContext

# Get the tuples of matrix A
with open('part-00000') as f:
    row_col_val_tuples = []
    for line in f:
        line = line.strip()
        matrix, i, j, val = line.split(' ')
        
        if matrix == 'A':
            row_col_val_tuples.append(MatrixEntry(i, j, val))

# Create Coordinate Matrix
row_col_rdd = sc.parallelize(row_col_val_tuples)
coord_mat = CoordinateMatrix(row_col_rdd)

# Print attributes
print("Coordinate matrix:\nNum rows: {}\nNum cols: {}".format(coord_mat.numRows(), coord_mat.numCols()))

# Create Row Matrix
row_mat = coord_mat.toRowMatrix()

# Print attributes
print("Row matrix:\nNum rows: {}\nNum cols: {}".format(row_mat.numRows(), row_mat.numCols()))