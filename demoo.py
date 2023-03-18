from pyspark.sql import SparkSession
import pyspark.pandas as ps
from pyspark.sql import SparkSession

#sc = SparkContext(master="local", appName="spark_demo")
#spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
psdf = ps.DataFrame(
    {'a': [1, 2, 3, 4, 5, 6],
     'b': [100, 200, 300, 400, 500, 600],
     'c': ["one", "two", "three", "four", "five", "six"]},
    index=[10, 20, 30, 40, 50, 60])

print(psdf)

psdf1 = ps.DataFrame(
    {'a': [10, 20, 30, 40, 50, 60],
     'b': [1000, 2000, 3000, 4000, 5000, 6000],
     'c': ["one100", "two200", "three300", "four400", "five500", "six600"]},
    index=[1, 2, 3, 4, 5, 6])

print(psdf1)
result = ps.concat([psdf,psdf1],axis=1)
print(result)


#df2=df.select(concat(df.firstname,df.middlename,df.lastname)
              #.alias("FullName"),"dob","gender","salary")
#df2.show(truncate=False)

#data = [('Vasanth','1991-04-01','M',3000), ('Vanadhi','2000-05-19','M',4000), ('Pavithraa','1978-09-05','M',4000),('Mani','1967-12-01','F',4000),('Jeeva','1980-02-17','F',-1)]
#columns = ["Name","dob","gender","salary"]
#df3 = spark.createDataFrame(data= data, schema = columns)
#df4 = df2.concat(df3)
#df4= df2.select(df2.gender).concat(df3.select(df3.gender))
#df4.show()
#df5 = df2.union(df3)
#df5.show()

#df2 = df2.withColumn("id",monotonically_increasing_id())
#df3 = df3.withColumn( "id", monotonically_increasing_id())
#horiztnlcombined_data = df2.join(df3,df3.id == df2.id, how='inner')
#horiztnlcombined_data.show()