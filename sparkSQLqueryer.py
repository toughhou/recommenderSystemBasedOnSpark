#coding=utf-8
"""从parquet列式存储文件中进行查询"""
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,DataFrame

conf = SparkConf().setAppName('rawDataToOutlineData').setMaster('spark://HP-Pavilion:7077')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

dfMovies = sqlContext.read.parquet('movie_base')
dfMovies.show()
print(dfMovies.dtypes)
print(dfMovies.schema)
sqlContext.registerDataFrameAsTable(dfMovies,'movie_base')
dfMovieResult = sqlContext.sql('select * from movie_base where movieId=1193')
dfMovieResult.show()

dfRating = sqlContext.read.parquet('rating_base')
dfRating.show()
print(dfRating.dtypes)
print(dfRating.schema)
sqlContext.registerDataFrameAsTable(dfRating,'rating_base')
dfRatingResult = sqlContext.sql('select * from rating_base')
dfMovieResult.show()

dfUser = sqlContext.read.parquet('user_base')
dfUser.show()
print(dfUser.dtypes)
print(dfUser.schema)
sqlContext.registerDataFrameAsTable(dfUser,'user_base')
dfUserResult = sqlContext.sql('select * from user_base')
dfUserResult.show()

sc.stop()
