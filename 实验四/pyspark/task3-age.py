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
        .appName("AgeSQL")\
        .getOrCreate()

    df=spark.read.csv(r"C:\\Users\\953031197\\Desktop\\data_format1\\result.csv", encoding='gbk', header=True, inferSchema=True)
    df=df.filter((df.action_type==2) )
    df=df.dropDuplicates(subset=["user_id"])#去重
    df=df.filter((df.age_range>=1)&(df.age_range<=8))
    df=df.replace(8,7)
    df=df.groupby('age_range').agg(F.count(df['age_range']))
    df.show()
    age=["<18","[18,24]","[25,29]","[30,34]","[35,39]","[40,49]",">=50"]
    list=df.collect()
    sum=0
    for i in range(0,len(list)):
        sum+=list[i]['count(age_range)']+list[1]['count(age_range)']
    for i in range(0,len(list)):
        print(age[int(list[i].age_range)-1]+":"+str(round(list[i]['count(age_range)']*100/sum,2))+"%")