#coding=utf-8
"""构造评分矩阵"""
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,Row

def launch(num,line):
    result = [line[0]]
    """得到评分矩阵中的一行"""
    li = line[1]
    liNum = len(li)
    j = 0
    for i in range(1,num+1):
        if(li[j][0] != i):
            result.append(0.0)
        else:
            result.append(li[j][1])
            if(j != liNum-1):
                j = j + 1
    return result
conf = SparkConf().setAppName('createRatingMetrix').setMaster('spark://HP-Pavilion:7077')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

dfRating = sqlContext.read.parquet('rating_base').select('movieId','userId','rating')
"""
一定是 lambda line:(line[0],(line[1],line[2]))
不能是 lambda line:(line[0],line[1],line[2])
否为会报 too much value 的错误，默认情况下，第一个元素，即 line[0]会被视为key，而后一个则被视为value，
所以key后面只能有一个整体的value
"""
rddRating = dfRating.rdd.map(lambda line:(line[0],(line[1],line[2])))
rddRatingGroupedByMovieId = rddRating.groupByKey()
"""
groupByKey()之后产生的是一个 ResultIterable对象
from pyspark.resultiterable import ResultIterable
通过查看源码可以知道，data为其的一个public成员，可以通过
ResultIterable.data得到groupByKey后的数据
"""
rddLaunched = rddRatingGroupedByMovieId.map(lambda line:(line[0],line[1].data))
usersNum = sqlContext.read.parquet('user_base').count()
print(rddLaunched.first())
rddFinal = rddLaunched.map(lambda line :launch(usersNum,line))
print(rddFinal.first())
dfFinal = sqlContext.createDataFrame(rddFinal.map(lambda line:Row(row=line)))
dfFinal.show()
dfFinal.write.parquet('ratingMatrix')

sc.stop()