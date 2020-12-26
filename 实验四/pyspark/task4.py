import sys
from operator import add
import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql.functions import pandas_udf, PandasUDFType     
from pyspark.mllib.classification import SVMModel,SVMWithSGD
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("ML")\
        .getOrCreate()
    schema_train=StructType(
        [
            StructField('user_id',DoubleType()),
            StructField('seller_id',DoubleType()),
            StructField('label',DoubleType()),
            StructField('age_range',DoubleType()),
            StructField('gender',DoubleType()),
            StructField('total_count',DoubleType()),
            StructField('total_unique_item',DoubleType()),
            StructField('unique_item',DoubleType()),
            StructField('total_unique_cat',DoubleType()),
            StructField('unique_cat',DoubleType()),
            StructField('total_browse_days',DoubleType()),
            StructField('browse_days',DoubleType()),
            StructField('one_clicks',DoubleType()),
            StructField('shopping_carts',DoubleType()),
            StructField('purchase_times',DoubleType()),
            StructField('favourite_times',DoubleType())
        ]
    )
    schema_predict=StructType(
        [
            StructField('user_id',DoubleType()),
            StructField('seller_id',DoubleType()),
            StructField('label',DoubleType()),
        ]
    )
    train_data=spark.read.format('csv').option('sep',',').option('header','true').load('C:\\Users\\953031197\\Desktop\\processed_train_data.csv',schema=schema_train)
    train_data.show(5)
    train_data=train_data.fillna(0, subset=['shopping_carts', 'favourite_times', 'one_clicks','browse_days','purchase_times']) #填充null值
    train_data.show(5)
    train_data = train_data.filter(train_data.gender. isNotNull())
    train_data = train_data.filter(train_data.age_range. isNotNull())
    train_data.show(5)
    train_data=train_data.rdd
    
   
    re=pd.DataFrame(columns=['sample_seed','SVM_error','LR_error'])
    (trainData, testData) = train_data.randomSplit([7,3])#划分训练集和测试集
    trainData=trainData.map(lambda line: LabeledPoint(line[2],[line[3:]]))
    testData=testData.map(lambda line: LabeledPoint(line[2],[line[3:]]))
    trainData.cache()
    trainCount=float(trainData.count())
    testData.cache()
    testCount=float(testData.count())
    LR=LogisticRegressionWithLBFGS()
    LR=LR.train(trainData,iterations=10)#逻辑回归
    SVM=SVMWithSGD.train(trainData,iterations=10)#支持向量机
    predictedSVM=trainData.map(lambda x:(x.label,SVM.predict(x.features)))
    predictedLR=trainData.map(lambda x:(x.label,LR.predict(x.features)))
    accuracySVM=predictedSVM.filter(lambda p:p[0]==p[1]).count()/trainCount
    accuracyLR=predictedLR.filter(lambda p:p[0]==p[1]).count()/trainCount
    print("SVM training accuracy="+str(round(accuracySVM*100,2))+"%")#SVM训练集准确度
    print("LR training accuracy="+str(round(accuracyLR*100,2))+"%")#LR训练集准确度
    te_predictedSVM=testData.map(lambda x:(x.label,SVM.predict(x.features)))
    te_predictedLR=testData.map(lambda x:(x.label,LR.predict(x.features)))
    te_accuracySVM=te_predictedSVM.filter(lambda p:p[0]==p[1]).count()/testCount
    te_accuracyLR=te_predictedLR.filter(lambda p:p[0]==p[1]).count()/testCount
    print("SVM testing accuracy="+str(round(te_accuracySVM*100,2))+"%")#SVM测试集准确度
    print("LR testing accuracy="+str(round(te_accuracyLR*100,2))+"%")#LR测试集准确度

    #进行预测
    schema_predict=StructType(
        [
            StructField('user_id',DoubleType()),
            StructField('seller_id',DoubleType()),
            StructField('prob',DoubleType()),
            StructField('age_range',DoubleType()),
            StructField('gender',DoubleType()),
            StructField('total_count',DoubleType()),
            StructField('total_unique_item',DoubleType()),
            StructField('unique_item',DoubleType()),
            StructField('total_unique_cat',DoubleType()),
            StructField('unique_cat',DoubleType()),
            StructField('total_browse_days',DoubleType()),
            StructField('browse_days',DoubleType()),
            StructField('one_clicks',DoubleType()),
            StructField('shopping_carts',DoubleType()),
            StructField('purchase_times',DoubleType()),
            StructField('favourite_times',DoubleType())
        ]
    )
    result=spark.read.csv(r"C:\\Users\\953031197\\Desktop\\data_format1\\test_format1.csv", encoding='gbk', header=True, inferSchema=True).withColumnRenamed("merchant_id", "seller_id")#使列名一致
    result=result.drop('prob')
    predict_data=spark.read.format('csv').option('sep',',').option('header','true').load('C:\\Users\\953031197\\Desktop\\processed_test_data.csv',schema=schema_predict)
    predict_data.show(5)
    predict_data=predict_data.fillna(0, subset=['shopping_carts','prob', 'favourite_times', 'one_clicks','browse_days','purchase_times']) #填充null值
    predict_data.show(5)
    predict_data = predict_data.filter(predict_data.gender. isNotNull())
    predict_data =predict_data.filter(predict_data.age_range. isNotNull())
    predict_data.show(5)
    predictData=predict_data.rdd
    predictData=predictData.map(lambda line:(line[0],line[1], LabeledPoint(line[2],[line[3:]])))
    predictData.cache()
    LR.clearThreshold()#可能性预测
    #pr_predictedSVM=predictData.map(lambda p:(p[0],p[1],SVM.predict(p[2].features)))
    pr_predictedLR=predictData.map(lambda p:(p[0],p[1],LR.predict(p[2].features)))
    #dfSVM=pr_predictedSVM.toDF().withColumnRenamed("_1", "user_id").withColumnRenamed("_2", "seller_id").withColumnRenamed("_3", "SVM") 
    dfLR=pr_predictedLR.toDF().withColumnRenamed("_1", "user_id").withColumnRenamed("_2", "seller_id").withColumnRenamed("_3", "prob") 
    #result=result.join(dfSVM,["user_id","seller_id"],'left')
    result=result.join(dfLR,["user_id","seller_id"],'left')
    result.toPandas().to_csv('result.csv')