#coding=utf-8
"""将原始数据转换为parquet列式文件存储"""
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,Row

conf = SparkConf().setAppName('rawDataToOutlineData').setMaster('spark://HP-Pavilion:7077')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

moviesRawData = sc.textFile('data/movies.dat')
temp1 = moviesRawData.map(lambda line:line.split('::'))
temp2 = temp1.map(lambda line:Row(movieId=int(line[0]),title=line[1],genres=line[2]))
dfMovies = sqlContext.createDataFrame(temp2)
dfMovies.show()
dfMovies.write.parquet('movie_base')

ratingsRawData = sc.textFile('data/ratings.dat')
temp3 = ratingsRawData.map(lambda line:line.split('::'))
temp4 = temp3.map(lambda line:Row(userId=int(line[0]),movieId=int(line[1]),rating=float(line[2]),timestamp=int(line[3])))
dfRatings = sqlContext.createDataFrame(temp4)
dfRatings.show()
dfRatings.write.parquet('rating_base')

usersRawData = sc.textFile('data/users.dat')
temp5 = usersRawData.map(lambda line:line.split('::'))
temp6 = temp5.map(lambda line:Row(userId=int(line[0]),gender=line[1],age=int(line[2]),occupation=int(line[3]),zipCode=line[4]))
dfUsers = sqlContext.createDataFrame(temp6)
dfUsers.show()
dfUsers.write.parquet('user_base')

sc.stop()
