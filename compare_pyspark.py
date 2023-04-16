from __future__ import annotations
import mysql.connector
import pandas as pd
#import pyarrow as pq
pd.options.display.max_columns = None
pd.options.display.max_rows = None
pd.options.display.width = 1000
#from Profiling import json_profiling
#from pandas_testing import fun_check,date_conversion,data_rounding,arithmethic_functions
from pyspark import SparkConf,SparkContext
import pyspark
from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import *


def compare_fun(source,target):

    """Compare the source and target dataframe from user"""

    def mask(source_col,target_col):

         """  Getting  column name of source and target from user and concatenate with target """

         s_col = source.select(col(source_col)).toPandas() #.rename(f's_{source_col}')
         t_col = target.select(col(target_col)).toPandas() #.rename(f't_{source_col}')
         s_eq = s_col.eq(t_col)
         t_eq = t_col.eq(s_col)
         #s_eq=source[source_col].eq(target[target_col]).rename(f's_{source_col}')
         #t_eq = target[target_col].eq(source[source_col]).rename(f't_{source_col}')
         df =pd.concat([s_eq,t_eq],axis = 1)
         return df

    def color_mismatch(val):

        """ Highlighting the mismatched data of source and target with red color"""

        color  ='background-color:red' if val == False else''
        return color


    def concat_col(source_col,target_col):

        """ Columns are arranged together in a single Dataframe"""


        s_col = source.select(col(source_col)).toPandas()
        t_col = target.select(col(target_col)).toPandas()
        df = pd.concat([s_col, t_col], axis=1)
        return df


    bool_df = pd.DataFrame()
    original_df = pd.DataFrame()
    for _ in range(3):                ###len(tuple(zip(source,target)))):
        source_col = input('Choose the source col:')
        target_col = input('Choose the target col:')
        concat_col_= concat_col(source_col,target_col)
        original_df = pd.concat([original_df,concat_col_],axis = 1)
        mask_= mask(source_col,target_col)
        bool_df = pd.concat([bool_df,mask_],axis = 1)
        #colur_=original_df.style.apply(lambda x:bool_df.applymap(color_mismatch),axis = None).to_excel("Output.xlsx",index=False)

    bool_df1 = bool_df[(bool_df == False).any(axis=1)]
    updated_df = original_df.iloc[bool_df1.index, :]
    print(bool_df1)
    print(updated_df)
  #  try:
    updated_df.style.apply(lambda x: bool_df1.applymap(color_mismatch), axis=None).to_excel("excelfile.xlsx",index=False)

   # except PermissionError:
        #print('File permission denied')

if __name__ == '__main__':

   sc = SparkContext(master="local", appName="spark_demo")
   spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
   source = spark.read.csv(r'C:\Users\Obuli\Downloads\students_mark1_colchanges.csv',header=True)
   target = spark.read.csv(r'C:\Users\Obuli\Downloads\students_mark_changes.csv',header=True)
   result = compare_fun(source,target)