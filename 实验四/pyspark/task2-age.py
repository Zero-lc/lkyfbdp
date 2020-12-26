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
        .appName("AgeCount")\
        .getOrCreate()

    lines=spark.read.csv(r"C:\\Users\\953031197\\Desktop\\data_format1\\result.csv", encoding='gbk', header=True, inferSchema=True)
    lines=lines.filter((lines.action_type==2) )
    lines=lines.dropDuplicates(subset=["user_id"])#去重
    lines=lines.filter((lines.age_range>=1)&(lines.age_range<=8))
    lines.show()
    lines=lines.withColumn("age_range", lines["age_range"].cast(IntegerType()))
    lines=lines[['age_range']]
    lines=lines.replace(8,7).rdd
    age=["<18","[18,24]","[25,29]","[30,34]","[35,39]","[40,49]",">=50"]
    counts =lines.map(lambda x: (x, 1))\
                  .reduceByKey(add)\
                  .sortBy(keyfunc=(lambda x:x[1]),ascending=False)\
                  .take(7) 
    output = counts
    sum=0
    with open('age.txt', 'w') as f:
        for (word, count) in output:
            print(age[word.age_range-1]+","+str(count)+"\r")
            f.write(age[word.age_range-1]+","+str(count)+"\r")
    with open('age_rate.txt', 'w') as f:
        for (word, count) in output:
            sum+=count
        for (word, count) in output:
            print(age[word.age_range-1]+":"+str(round(count*100/sum,2))+"%"+"\r")
            f.write(age[word.age_range-1]+":"+str(round(count*100/sum,2))+"%"+"\r")
    spark.stop()