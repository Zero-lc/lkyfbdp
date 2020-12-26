from __future__ import print_function

import sys
from operator import add
from pandas.core.frame import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

from pyspark.sql import SQLContext
from pyspark import SparkContext

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("MostPopularSellerCount")\
        .getOrCreate()

    lines=spark.read.csv(r"C:\\Users\\953031197\\Desktop\\data_format1\\result.csv", encoding='gbk', header=True, inferSchema=True)
    lines=lines.filter((0<lines.age_range) & (lines.age_range<4))
    lines=lines.withColumn("seller_id", lines["seller_id"].cast(StringType()))
    lines=lines[['seller_id']].rdd
    
    counts =lines.map(lambda x: (x, 1))\
                  .reduceByKey(add)\
                  .sortBy(keyfunc=(lambda x:x[1]),ascending=False)\
                  .take(100) 
    output = counts
    with open('seller.txt', 'w') as f:
        for (word, count) in output:
            print("%s: %i" % (word, count))
            f.write(str(word)+","+str(count)+"\r")
    spark.stop()