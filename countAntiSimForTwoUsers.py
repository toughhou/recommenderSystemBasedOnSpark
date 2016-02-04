#coding=utf-8
"""计算两个用户之间的相异度"""
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,DataFrame

conf = SparkConf().setAppName('rawDataToOutlineData').setMaster('spark://HP-Pavilion:7077')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

dfUser = sqlContext.read.parquet('user_base')
dfUser.show()
print(dfUser.dtypes)
print(dfUser.schema)
sqlContext.registerDataFrameAsTable(dfUser,'user_base')
dfUserGrouped = dfUser.groupBy(dfUser.age).count()
listOfRows = dfUserGrouped.orderBy('age').collect()
print(listOfRows)
num = len(listOfRows)
l = []
for index in range(num):
    temp = {}
    if(index == 0):
        temp[listOfRows[index]['age']] = 1
    else:
        M = l[index-1][listOfRows[index-1]['age']]+listOfRows[index-1]['count']
        temp[listOfRows[index]['age']] = M
    l.append(temp)
print(l)
print(M)
for i in range(num):
    l[i][listOfRows[i]['age']] = float(l[i][listOfRows[i]['age']]-1)/(M-1)
    #print(l[i][listOfRows[i]['age']])
print(l)
sc.stop()
