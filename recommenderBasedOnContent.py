#coding=utf-8
"""基于内容的推荐引擎的实现"""
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,Row
from operator import add

def countScore(userId,movieId):
    """评分预测的推荐方案，给定userId，movieId，求出预测分数"""
    """选取用户已经看过的电影"""
    dfRatings = sqlContext.read.parquet('rating_base')
    dfRatingsCorrespondingUserId = dfRatings.where(dfRatings.userId == userId).select('*')
    """选取相似电影，给定movieId"""
    dfSimMovies = sqlContext.read.parquet('simContent')
    dfSimMovies.show()
    dfSimMoviesCorrespondingMovieId = dfSimMovies.where(dfSimMovies.movie1 == movieId).select('*')
    dfMovieWatchedSim = dfRatingsCorrespondingUserId.join(dfSimMoviesCorrespondingMovieId,\
                                                          dfSimMoviesCorrespondingMovieId.movie2 == dfRatingsCorrespondingUserId.movieId,\
                                                          'inner').where(dfSimMoviesCorrespondingMovieId.sim > simThreshold).select('movie2','sim','rating')
    dfMovieWatchedSim.show()
    dfFinal = dfMovieWatchedSim.orderBy('sim',ascending=0).limit(movieSimNum).select('*')
    dfFinal.show()
    listOfRows = dfFinal.collect()    
    """计算预测分数"""
    simAll = 0.0
    for index in range(len(listOfRows)):
        temp = listOfRows[index]
        if(temp['movie2']!=movieId):
            simAll = simAll+temp['sim']
    score = 0.0
    for index in range(len(listOfRows)):
        temp = listOfRows[index]
        if(temp['movie2']!=movieId):
            score = score+(temp['sim']/simAll)*temp['rating']
    return score

def findTopNToRecommend(userId):
    """根据userId进行推荐TopN"""
    dfRatings = sqlContext.read.parquet('rating_base')
    """用户最近观看的电影"""
    dfRatingsJustNow = dfRatings.orderBy('timestamp',ascending=0).where(dfRatings.userId == userId).limit(moviesWatchedJustNowNum).select('movieId','rating')
    dfRatingsJustNow.show()
    """满足阈值的电影"""
    dfSimMovies = sqlContext.read.parquet('simContent')
    dfSimMoviesSelected = dfSimMovies.where(dfSimMovies.sim > simThreshold).select('*')
    """相似于用户观看过的电影的电影"""
    dfMoviesSimToMoviesWatched = dfRatingsJustNow.join(dfSimMoviesSelected,dfRatingsJustNow.movieId == dfSimMoviesSelected.movie1,'inner')\
        .select('movieId','movie2','sim','rating')
    dfScored = dfMoviesSimToMoviesWatched.withColumn('score1',dfMoviesSimToMoviesWatched.sim*(dfMoviesSimToMoviesWatched.rating-2.5))\
        .select('movie2','score1')
    dfScored.show()
    rddScored = dfScored.rdd
    rddFinal = rddScored.reduceByKey(add).map(lambda line:Row(movie2=line[0],scoreFinal=line[1]))
    dfFinal = sqlContext.createDataFrame(rddFinal).orderBy('scoreFinal',ascending=0)\
        .limit(recommendListNum).select('*')
    return dfFinal

"""配置项"""    
"""电影相似阈值，低于该阈值，电影将被过滤掉"""
simThreshold = 0.8
"""相似电影的个数"""
movieSimNum = 10
"""目标用户最近看过的电影的个数"""
moviesWatchedJustNowNum = 3
"""推荐结果列表项的个数"""
recommendListNum = 30

conf = SparkConf().setAppName('recommenderBasedOnContent').setMaster('spark://HP-Pavilion:7077')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

print(countScore(1,1))
findTopNToRecommend(1).show()

sc.stop()

