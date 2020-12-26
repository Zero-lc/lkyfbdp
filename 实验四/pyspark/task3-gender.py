from __future__ import print_function

import sys
from operator import add
from pandas.core.frame import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
from pyspark.sql import SQLContext
from pyspark import SparkContext

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("GenderSQL")\
        .getOrCreate()

    df=spark.read.csv(r"C:\\Users\\953031197\\Desktop\\data_format1\\result.csv", encoding='gbk', header=True, inferSchema=True)
    df=df.filter(df.action_type==2) 
    df=df.dropDuplicates(subset=["user_id"])#去重
    df=df.filter((df.gender==0) | (df.gender==1))
    df=df.groupby('gender').agg(F.count(df['gender']))
    df.show()
    gender=["female","male"]
    list=df.collect()
    sum=list[0]['count(gender)']+list[1]['count(gender)']
    print(gender[int(list[0].gender)]+":"+str(round(list[0]['count(gender)']*100/sum,2))+"%")
    print(gender[int(list[1].gender)]+":"+str(round(list[1]['count(gender)']*100/sum,2))+"%")