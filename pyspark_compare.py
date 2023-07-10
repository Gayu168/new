import pyspark
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import concat,col
from pyspark.sql.functions import *
from pyspark.sql.types import StructType



def mask(source_col, target_col):
    """  Getting  column name of source and target from user and concatenate with target """

    s_eq = source[source_col].compare(target[target_col]).rename(f's_{source_col}')
    t_eq = target[target_col].compare(source[source_col]).rename(f't_{source_col}')
    df = ps.concat([s_eq, t_eq], axis=1)
    print(df)
    return df


def color_mismatch(val):
    """ Highlighting the mismatched data of source and target with red color"""

    color = 'background-color:red' if val == False else ''
    return color


def concat_col(source_col, target_col):
    """ Columns are arranged together in a single Dataframe"""

    s_col = source[source_col].rename(f's_{source_col}')
    # s_col = ps.series
    # type(s_col)
    t_col = target[target_col].rename(f't_{source_col}')
    # t_col = ps.series
    df = ps.concat([s_col, t_col], axis=1)
    print(df)
    return df


if __name__ == '__main__':
  sc = SparkContext(master="local", appName="spark_demo")
  spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
  df = spark.read.option("header", True).csv(r"C:\Users\Obuli\Downloads\students_mark1_colchanges.csv")
  df1 = spark.read.csv(r"C:\Users\Obuli\Downloads\students_mark1_colnamechanges.csv")
  df.printSchema()
  df3 = spark.createDataFrame([], StructType([]))
  df3.printSchema()
#  b_df = spark.DataFrame()
 # original_df = spark.DataFrame()
  for _ in range(2):
      ###len(tuple(zip(source,target)))):
        source_col = input('Choose the source col:')
        target_col = input('Choose the target col:')
        concat_col_= concat_col(source_col,target_col)
        original_df = ps.concat([original_df,concat_col_],axis = 1)
        mask_= mask(source_col,target_col)
        print(mask)
        b_df = ps.concat([b_df,mask_],axis = 1)
        #print(b_df)
        #colur_=original_df.style.apply(lambda x:bool_df.applymap(color_mismatch),axis = None).to_excel("Output.xlsx",index=False)

  b_df1 = b_df[(np.bool(b_df) == False).any(axis=0)]
  print(b_df1)
  updated_df = original_df.iloc[b_df1.index]
  print(b_df1)
  print(updated_df)
  try:
       updated_df.style.apply(lambda x: b_df1.applymap(color_mismatch), axis=None).to_excel("excelfile.xlsx",index=False)

  except PermissionError:
        print('File permission denied')

