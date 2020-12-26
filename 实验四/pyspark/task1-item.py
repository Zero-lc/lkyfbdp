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
        .appName("MostPopularItemCount")\
        .getOrCreate()

    lines=spark.read.csv(r"C:\\Users\\953031197\\Desktop\\data_format1\\user_log_format1.csv", encoding='gbk', header=True, inferSchema=True)
    lines=lines.filter((lines.action_type!=0) & (lines.time_stamp==1111))
    lines=lines.withColumn("item_id", lines["item_id"].cast(StringType()))
    lines=lines[['item_id']].rdd
    
    counts =lines.map(lambda x: (x, 1))\
                  .reduceByKey(add)\
                  .sortBy(keyfunc=(lambda x:x[1]),ascending=False)\
                  .take(100) 
    output = counts
    with open('item.txt', 'w') as f:
        for (word, count) in output:
            print("%s: %i" % (word, count))
            f.write(str(word)+","+str(count)+"\r")
    spark.stop()