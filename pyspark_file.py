from __future__ import annotations
import mysql.connector
import pandas as pd
#import pyarrow as pq
pd.options.display.max_columns = None
pd.options.display.max_rows = None
pd.options.display.width = 1000
from Profiling import json_profiling
from pandas_testing import fun_check,date_conversion,data_rounding,arithmethic_functions
from pyspark import SparkConf,SparkContext
import pyspark
from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import *



def credentials():

    """ Getting the Credentials from user for connecting the databases"""

   # print("enter the user:")
    #user = input()
    user ="root"
   # print("enter the localhost:")
    #localhost = input()
    localhost = "localhost"
    #print("enter the password:")
    #password = input()
    password = "Gayu@1998"
    print("enter the database:")
    database = input()
    return user, localhost, password, database


def sorting(sr_df):

    """ Gathering the column details from user for sorting dataframe."""

    print("Enter the column name for sorting:")
    col_list = []
    for i in range(3):
        col_name = input()
        col_list.append(col_name)
    sorted_df = sr_df.sort_values(by = col_list,ascending = True)
    return sorted_df


def source_connect(src_db):

    """ Getting query from user for connecting source connection"""

    cur = src_db.cursor()
    print("enter the sql query :")
    query = input()
    data = cur.execute(query)
    num_fields = len(cur.description)
    field_names = [i[0] for i in iter(cur.description)]
    df = pd.DataFrame(cur.fetchall(), columns=field_names);
    return field_names,df



def get_database_schema(src_db):

        """ Query the INFORMATION_SCHEMA for the list of tables in each database """

        cur = src_db.cursor()
        print("enter the sql query for dataschema:")
        query = input()
        dataschema = cur.execute(query)
        schema =  cur.fetchall()
        return dict((column, datatype) for column, datatype in schema)


def compare_fun(source,target):

    """Compare the source and target dataframe from user"""

    def mask(source_col,target_col):

         """  Getting  column name of source and target from user and concatenate with target """

         s_eq=source[source_col].eq(target[target_col]).rename(f's_{source_col}')
         t_eq = target[target_col].eq(source[source_col]).rename(f't_{source_col}')
         df =pd.concat([s_eq,t_eq],axis = 1)
         return df

    def color_mismatch(val):

        """ Highlighting the mismatched data of source and target with red color"""

        color  ='background-color:red' if val == False else''
        return color


    def concat_col(source_col,target_col):

        """ Columns are arranged together in a single Dataframe"""

        s_col = source.withColumn(source[source_col],col(f's_{source_col}'))
        t_col= target.withColumn(target[target_col],col(f't_{source_col}'))
        df = pd.concat([s_col,t_col],axis = 1)
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
    try:
       updated_df.style.apply(lambda x: bool_df1.applymap(color_mismatch), axis=None).to_excel("excelfile.xlsx",index=False)

    except PermissionError:
        print('File permission denied')

def get_schema_flatfile(src):

       """It Gives the column name and datatype of a file"""
       schema_file = src.dtypes
       return schema_file


def get_connection(engine_: str) -> dict:

    if int(engine_) == 1:

        """ Connecting the mysql database with help of credentials"""
        user,localhost,password,database= credentials()
        db_con1 = mysql.connector.connect(host=localhost, user=user, password= password, database=database)
        return db_con1
    if int(engine_) == 2:

        """ Connecting the postgresql database with help of credentials"""
       # user, localhost, password, database = credentials()
     #   psql = PostgresSQLConnection(user=user, host= localhost, password=password, database_=database)
       # psql_db = psql.get_database_schema();

class FileConnection:

    """ This Class contains comparison of flatfile . First check the type format of file
    after execute schema comparison of flat file """

    def read_csv(self, file_path: str) -> pd.DataFrame:
        try:

            df = spark.read.csv(file_path, header=True)
            return df

        except Exception as e:

            raise ValueError(f"Error reading CSV file: {e}")

    def read_excel(self, file_path: str) -> pd.DataFrame:
        try:
            return pd.read_excel(file_path,header=0,index_col=None)
        except Exception as e:
            raise ValueError(f"Error reading Excel file: {e}")

    def read_text(self, file_path: str) -> pd.DataFrame:
        try:
            col=[]
            delimeter = input('Enter the delimeter of file:')
            col_txt = input(' column is present or not:')
            if col_txt == True:
                source = pd.read_table(file_path,  delimiter=delimeter)
                return source
            else:
                source = pd.read_table(file_path, delimiter=delimeter)
                for i in range(source.shape[1]):
                    col_name = 'column'+str(i+1)
                    print(col_name)
                    col.append(col_name)
                source = pd.read_csv(file_path, sep=delimeter,  header=None, names=col)
                return source


        except Exception as e:
            raise ValueError(f"Error reading text file: {e}")

    def read_parquet(self,file_path:str) -> pd.DataFrame:
        try:
            return pq.read_table(file_path)
        except Exception as e:
            raise ValueError(f"Error reading parquet file: {e}")

    def read_json(self,file_path:str) -> pd.DataFrame:
        try:
            return pq.read_json(file_path)
        except Exception as e:
            raise ValueError(f"Error reading json file: {e}")

    def create_dataframe(self, file_path1, file_type1):
        formats = {'csv': self.read_csv, 'excel': self.read_excel,'text':self.read_text,'parquet':self.read_parquet, 'json':self.read_json}
        try:
            if file_type1 not in formats:
                raise ValueError(f"Unsupported file type: {file_type1}")
            file1 = formats[file_type1](file_path1)

            return file1
        except Exception as e:
            raise ValueError(f"Error comparing files: {e}")

def testing(source,target):
    result = pd.testing.assert_frame_equal(source,target)
    return result

def col_check(source,target):
    df1 = source
    df2 = target
    return df1,df2

def get_type_file(type: str) -> dict:

     """ This Function is choosing the type of file(Database,Flatfile)
        1 is for flat file
        2 is for Database connection."""

     if int(type) == 1:
         path1 = input("Enter the source: ")
         file_type = input("Enter the type: ")
         file_con = FileConnection()
         source_ = file_con.create_dataframe(file_path1=path1, file_type1=file_type)
        # source_profiling = json_profiling(source_)
        # print(source_profiling)
         source_schema = get_schema_flatfile(source_)
         print("Source Database schema", source_schema)
         return source_


     if int(type) == 2:
         db_engine = input('Choose the source engine: 1-mysql 2-postgressql: ')
         db_type = get_connection(db_engine)
         db_schema_src = get_database_schema(db_type)
         print("Source Database schema", db_schema_src)
         columnnames_sr, source = source_connect(db_type)
         sort_sr = sorting(source)
         source_profiling = json_profiling(source)
         print(source_profiling)
         return sort_sr

if __name__ == '__main__':
    sc = SparkContext(master="local", appName="spark_demo")
    spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()
    src_type = input("Enter the type of file: 1-Flatfile  2-Database :")
    st = time.time()
    source = get_type_file(src_type)
    et = time.time()
    processtime = st-et
    final = processtime/60
    print(final,"Minutes")
    #print('Execution time:', time.strftime("%H:%M:%S", time.gmtime(processtime)))
    #col_changed_sr = fun_check(source)
    #print(col_changed_sr)
    #date_check = date_conversion(source)
    #print(date_check)
    #rounding_op = data_rounding(source)
    #print(rounding_op)
    #obj = arithmethic_functions(source)
    #obj.Add_fun(source)
    #obj.Sub_fun(source)
   # obj.Mul_fun(source)
    #obj.Div_fun(source)
    #obj.cuberoot_fun(source)
    #obj.sqrt_fun(source)
    #obj.log_fun(source)
   # obj.exponetial_fun(source)
    tar_type = input("Enter the type of file: 1-Flatfile  2-Database :")
    target = get_type_file(tar_type)

    cmp_re = compare_fun(source,target)
    #ans = testing(source, target)
    #print(ans)

    #date_check = date_conversion(source)
    #print(date_check)



