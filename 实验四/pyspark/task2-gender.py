from __future__ import print_function

import sys
from operator import add
from pandas.core.frame import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,IntegerType

from pyspark.sql import SQLContext
from pyspark import SparkContext

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("GenderCount")\
        .getOrCreate()

    lines=spark.read.csv(r"C:\\Users\\953031197\\Desktop\\data_format1\\result.csv", encoding='gbk', header=True, inferSchema=True)
    lines=lines.filter((lines.action_type==2) )
    lines=lines.dropDuplicates(subset=["user_id"])#去重
    lines=lines.filter((lines.gender==0) | (lines.gender==1))
    lines.show()
    lines=lines.withColumn("gender", lines["gender"].cast(IntegerType()))
    lines=lines[['gender']].rdd
    gender=["female","male"]
    counts =lines.map(lambda x: (x, 1))\
                  .reduceByKey(add)\
                  .sortBy(keyfunc=(lambda x:x[1]),ascending=False)\
                  .take(2) 
    output = counts
    sum=0
    with open('gender.txt', 'w') as f:
        for (word, count) in output:
            print(gender[word.gender]+","+str(count)+"\r")
            f.write(gender[word.gender]+","+str(count)+"\r")
    with open('gender_rate.txt', 'w') as f1:
        for (word, count) in output:
            sum+=count
        for (word, count) in output:
            print(gender[word.gender]+str(round(count*100/sum,2)+"%"))
            f1.write(gender[word.gender]+","+str(round(count*100/sum,2))+"%"+"\r")
    spark.stop()