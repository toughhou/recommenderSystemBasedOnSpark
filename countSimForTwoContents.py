# coding=utf-8
"""基于内容的推荐，离线计算两个内容的相似性，存为 parquet形式"""
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,Row
import math

def countIntersectionForTwoSets(list1,list2):
    """计算两个集合的交集的模"""
    count = 0
    for i in range(len(list1)):
        m = list1[i]
        for j in range(len(list2)):
            if(list2[j] == m):
                count = count + 1
                break
    return count

def countSimBetweenTwoContent(list1,list2):
    """计算两个content的相似度"""
    s1 = len(list1)
    s2 = len(list2)
    m = math.sqrt(s1*s2)
    return countIntersectionForTwoSets(list1, list2)/m
    

conf = SparkConf().setAppName('rawDataToOutlineData').setMaster('spark://HP-Pavilion:7077')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

dfMovies = sqlContext.read.parquet('movie_base')
dfMovies.show()
"""计算两个rdd的笛卡尔积"""
rddMovieCartesianed = dfMovies.rdd.cartesian(dfMovies.rdd)
rddMovieIdAndGenre = rddMovieCartesianed.map(lambda line:Row(movie1=line[0]['movieId'],\
                                                             movie2=line[1]['movieId'],\
                                                             sim=countSimBetweenTwoContent(\
                                                             line[0]['genres'].encode('utf-8').split('|'),\
                                                             line[1]['genres'].encode('utf-8').split('|'))))
dfFinal = sqlContext.createDataFrame(rddMovieIdAndGenre)
dfFinal.show()
dfFinal.write.parquet('simContent')
                                             
sc.stop()