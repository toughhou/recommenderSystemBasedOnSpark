#coding=utf-8
"""计算两个用户之间的相异度"""
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,DataFrame,Row

def findAgeCoded(age,listCoded,listOfRows):
    """查找age的编码结果"""
    for index in range(len(listCoded)):
        temp = listOfRows[index]['age']
        if(temp == age):
            return listCoded[index][temp]
def antiSimOfGender(gender1,gender2):
    """计算性别的相异值"""
    if(gender1 == gender2):
        return 0
    else:
        return 1
def antiSimOfOccupation(o1,o2):
    """计算occupation的相异值"""
    if(o1 == o2):
        return 0
    else:
        return 1
def antiSimOfZipCode(z1,z2):
    """计算zipCode的相异值"""
    if(z1 == z2):
        return 0
    else:
        return 1
def countAntiSimBetweenTwoUsers(row1,row2,listCoded,listOfRows):
    """计算两个user的相异值"""
    age1 = findAgeCoded(row1['age'], listCoded, listOfRows)
    age2 = findAgeCoded(row2['age'], listCoded, listOfRows)
    resultAge = abs(age2-age1)
    resultGender = antiSimOfGender(row1['gender'],row2['gender'])
    resultOccupation = antiSimOfOccupation(row1['occupation'],row2['occupation'])
    resultZipCode = antiSimOfZipCode(row1['zipCode'],row2['zipCode'])
    return (resultGender+resultOccupation+resultZipCode+resultAge)/4
    
conf = SparkConf().setAppName('rawDataToOutlineData').setMaster('spark://HP-Pavilion:7077')
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

dfUser = sqlContext.read.parquet('user_base')
dfUser.show()
print(dfUser.dtypes)
print(dfUser.schema)
sqlContext.registerDataFrameAsTable(dfUser,'user_base')
"""对序数性属性age进行编码"""
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
"""计算两个user的相异值，并持久化存储"""
rddUser = dfUser.rdd
rddUserCartesianed = rddUser.cartesian(rddUser)
print(rddUserCartesianed.first())
rddUserAntiSim = rddUserCartesianed.map(lambda line:Row(user1=line[0]['userId'],\
                                                        user2=line[1]['userId'],\
                                                        antiSim=countAntiSimBetweenTwoUsers(line[0],\
                                                                                            line[1],\
                                                                                            l,listOfRows)))
dfUserAntiSim = sqlContext.createDataFrame(rddUserAntiSim)
dfUserAntiSim.show()
dfUserAntiSim.write.parquet('antiSimPopu')
dfUserAntiSim.printSchema()
sc.stop()
