
from pyspark.sql import SparkSession
 
if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .master("local[*]")\
        .getOrCreate()
 
    lines = spark.read.text("lkyfbdp/four1/input/input.csv").rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(',')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(lambda x, y: x + y)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))
 
    spark.stop()