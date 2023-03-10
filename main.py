from pyspark import SparkConf,SparkContext
import pyspark
from pyspark.sql import SparkSession

sc= SparkContext(master="local",appName="spark_demo")
spark = SparkSession.builder().appName("SparkByExamples.com").getOrCreate()
df = spark.read.csv("students_total.csv")
print(df)