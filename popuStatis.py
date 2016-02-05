# coding=utf-8
# 基于人口统计的推荐引擎的实现
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,Row
from operator import add

def countScore(userId,movieId):
    """求userId对movieId的预测评分"""
    """给定UseId，查询相似用户"""
    dfAntiSim = sqlContext.read.parquet('antiSimPopu')
    dfSimUsers = dfAntiSim.where(dfAntiSim.user1 == userId).where(dfAntiSim.antiSim < antiSimThreshold).select('*')
    dfSimUsers.printSchema()
    """给定movieId，查询看过该电影的用户"""
    dfRatings = sqlContext.read.parquet('rating_base')
    dfRatingsCorrespondingMovieId = dfRatings.where(dfRatings.movieId == movieId).select('*')
    dfRatingsCorrespondingMovieId.printSchema()
    """两表join查找"""
    dfResult = dfSimUsers.join(dfRatingsCorrespondingMovieId,dfRatingsCorrespondingMovieId.userId==dfSimUsers.user2,'inner').select('*')
    dfResult.show()
    dfResult.printSchema()
    dfFinal = dfResult.orderBy('antiSim').limit(userSimNum).select('*')
    dfFinal.show()
    listOfRows = dfFinal.collect()    
    simAll = 0.0
    for index in range(len(listOfRows)):
        temp = listOfRows[index]
        if(temp['user2']!=userId):
            simAll = simAll+1-temp['antiSim']
    score = 0.0
    for index in range(len(listOfRows)):
        temp = listOfRows[index]
        if(temp['user2']!=userId):
            score = score+((1-temp['antiSim'])/simAll)*temp['rating']
    return score

def findTopNToRecommend(userId):
    """根据userId进行推荐TopN"""
    """查找相似用户"""
    dfAntiSim = sqlContext.read.parquet('antiSimPopu')
    dfSimUsers = dfAntiSim.where(dfAntiSim.user1 == userId).where(dfAntiSim.antiSim < antiSimThreshold).orderBy('antiSim').select('*')
    dfSimUsers.printSchema()
    """相似用户的所有rating"""
    dfRatings = sqlContext.read.parquet('rating_base')
    dfRatingsCorrespondingUserId = dfRatings.join(dfSimUsers,dfRatings.userId == dfSimUsers.user2,'inner').select('*')
    dfRatingsCorrespondingUserId.printSchema()
    temp = dfRatingsCorrespondingUserId.orderBy('antiSim').groupBy('user2').count().collect()
    count = 0
    print(temp)
    for index in range(len(temp)):
        if(index == userSimNum):
            break
        count = count+temp[index]['count']
    dfRatingsCorrespondingUserIdSorted = dfRatingsCorrespondingUserId.orderBy('antiSim').limit(count).select('*')
    rddScore = dfRatingsCorrespondingUserId.map(lambda row:Row(movie=row['movieId'],score1=(1-row['antiSim'])*(row['rating']-2.5)))
    rddReduced = rddScore.reduceByKey(add)
    rddReducedRow = rddReduced.map(lambda line:Row(movie=line[0],scoreFinal=line[1]))
    dfFinal = sqlContext.createDataFrame(rddReducedRow).orderBy('scoreFinal',ascending=0).limit(recommendListNum).select('*')
    return dfFinal    
    
    
    
"""用户距离阈值，高于该值的用户将被过滤掉"""
antiSimThreshold = 0.4
"""相似用户的个数"""
userSimNum = 10
"""推荐结果列表项的个数"""
recommendListNum = 30

conf = SparkConf().setAppName('popuStatisRecommender').setMaster('spark://HP-Pavilion:7077')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
"""分数评估推荐"""
print(countScore(1,1))
"""topN推荐"""
findTopNToRecommend(1).show(n=31)

sc.stop()