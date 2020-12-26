from __future__ import print_function, division
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
spark = SparkSession\
        .builder\
        .appName("TableProcess")\
        .getOrCreate()
train_data =spark.read.csv(r"C:\\Users\\953031197\\Desktop\\data_format1\\train_format1.csv", encoding='gbk', header=True, inferSchema=True).withColumnRenamed("merchant_id", "seller_id")#使列名一致
log_data=spark.read.csv(r"C:\\Users\\953031197\\Desktop\\data_format1\\user_log_format1.csv", encoding='gbk', header=True, inferSchema=True)
info_data=spark.read.csv(r"C:\\Users\\953031197\\Desktop\\data_format1\\user_info_format1.csv", encoding='gbk', header=True, inferSchema=True)
train_data=train_data.join(info_data,"user_id")

total_count = log_data.groupBy("user_id","seller_id").agg({"action_type": "count"}).withColumnRenamed("count(action_type)", "total_count")#在该商家的总log数
train_data=train_data.join(total_count ,["user_id","seller_id"])

total_unique_item = log_data.dropDuplicates(subset=[c for c in log_data.columns if c in ["user_id","seller_id","item_id"]]).groupBy("user_id").agg({"item_id": "count"}).withColumnRenamed("count(item_id)", "total_unique_item")#浏览的总商品数
train_data=train_data.join(total_unique_item,["user_id"])

unique_item = log_data.dropDuplicates(subset=[c for c in log_data.columns if c in ["user_id","seller_id","item_id"]]).groupBy("user_id","seller_id").agg({"item_id": "count"}).withColumnRenamed("count(item_id)", "unique_item")#浏览的该商家商品数
train_data=train_data.join(unique_item,["user_id","seller_id"])

total_unique_cat = log_data.dropDuplicates(subset=[c for c in log_data.columns if c in ["user_id","seller_id","cat_id"]]).groupBy("user_id").agg({"cat_id": "count"}).withColumnRenamed("count(cat_id)", "total_unique_cat")#浏览的总商品种类数
train_data=train_data.join(total_unique_cat,["user_id"])

unique_cat = log_data.dropDuplicates(subset=[c for c in log_data.columns if c in ["user_id","seller_id","cat_id"]]).groupBy("user_id","seller_id").agg({"cat_id": "count"}).withColumnRenamed("count(cat_id)", "unique_cat")#浏览的该商家商品种类数
train_data=train_data.join(unique_cat,["user_id","seller_id"])

total_browse_days = log_data.dropDuplicates(subset=[c for c in log_data.columns if c in ["user_id","seller_id","item_id"]]).groupBy("user_id").agg({"time_stamp": "count"}).withColumnRenamed("count(time_stamp)", "total_browse_days")#浏览的总天数
train_data=train_data.join(total_browse_days,["user_id"])

browse_days = log_data.dropDuplicates(subset=[c for c in log_data.columns if c in ["user_id","seller_id","item_id"]]).groupBy("user_id","seller_id").agg({"time_stamp": "count"}).withColumnRenamed("count(time_stamp)", "browse_days")#浏览该商家的天数
train_data=train_data.join(browse_days,["user_id","seller_id"])

one_clicks = log_data.filter(log_data.action_type==0).groupBy("user_id","seller_id").agg({"action_type": "count"}).withColumnRenamed("count(action_type)", "one_clicks")#在该商家单击的次数
train_data=train_data.join(one_clicks,["user_id","seller_id"])

shopping_carts = log_data.filter(log_data.action_type==1).groupBy("user_id","seller_id").agg({"action_type": "count"}).withColumnRenamed("count(action_type)", "shopping_carts")#在该商家添加购物车的次数
train_data=train_data.join(shopping_carts,["user_id","seller_id"])

purchase_times = log_data.filter(log_data.action_type==2).groupBy("user_id","seller_id").agg({"action_type": "count"}).withColumnRenamed("count(action_type)", "purchase_times")#在该商家购买的次数
train_data=train_data.join(purchase_times,["user_id","seller_id"])

favourite_times = log_data.filter(log_data.action_type==3).groupBy("user_id","seller_id").agg({"action_type": "count"}).withColumnRenamed("count(action_type)", "favourite_times")#在该商家添加收藏夹的次数
train_data=train_data.join(favourite_times,["user_id","seller_id"])

train_data.show()
train_data.toPandas().to_csv('processed_train_data.csv')