from pyspark.sql import SparkSession
from pyspark.sql.functions import concat,col
from pyspark import SparkContext
from pyspark.sql.functions import monotonically_increasing_id

sc = SparkContext(master="local", appName="spark_demo")
spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]
columns = ["firstname","middlename","lastname","dob","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df2=df.select(concat(df.firstname,df.middlename,df.lastname)
              .alias("FullName"),"dob","gender","salary")
df2.show(truncate=False)

data = [('Vasanth','1991-04-01','M',3000), ('Vanadhi','2000-05-19','M',4000), ('Pavithraa','1978-09-05','M',4000),('Mani','1967-12-01','F',4000),('Jeeva','1980-02-17','F',-1)]
columns = ["Name","dob","gender","salary"]
df3 = spark.createDataFrame(data= data, schema = columns)
#df4 = df2.concat(df3)
#df4= df2.select(df2.gender).concat(df3.select(df3.gender))
#df4.show()
df5 = df2.union(df3)
df5.show()

df2 = df2.withColumn("id",monotonically_increasing_id())
df3 = df3.withColumn( "id", monotonically_increasing_id())
horiztnlcombined_data = df2.join(df3,df3.id == df2.id, how='inner')
horiztnlcombined_data.show()