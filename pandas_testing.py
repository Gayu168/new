import pandas as pd
import numpy as np

def fun_check(source):

  N = input("how many column you want to change datatype :")
  convert_dict={}
  for i in range(int(N)):
    col_change = input("Enter a columnname for conversion:")
    new_data_type = input("Enter a new datatype:")
    convert_dict[col_change] = new_data_type
  df = source.astype(convert_dict)
  return df.dtypes


def date_conversion(source):
   N = input("how many column you want to change dateformat for a column :")
   for i in range(int(N)):
      col_name = input("Enter a column name for  datetime conversion:")
      new_format = input("Enter a new datatype:")
      source[col_name] = pd.to_datetime(source[col_name]).dt.strftime(new_format)
   return source

def data_rounding(source):
    N = input("how many column you want to rounding for a column :")
    for i in range(int(N)):
        col_name = input("Enter a column name for  rounding:")
        rounding_term  =int(input("Enter a rounding number :"))
        source[col_name] = source[col_name].round(rounding_term)
    return source


class arithmethic_functions ():
     def __init__(self, source):
         self.source = source


     def Add_fun(self,source):
         col_name = input("Enter a column for increment:")
         adding_term = int(input("Enter a value for add: "))
         source[col_name] = source[col_name] + adding_term
         print(source)
         return source

     def Sub_fun(self,source):
         col_name = input("Enter a column for Decrement:")
         deleting_term = int(input("Enter a value for delete: "))
         source[col_name] = source[col_name] - deleting_term
         print(source)
         return source

     def Mul_fun(self, source):
         col_name = input("Enter a column for multiplication:")
         multiplicate_term = int(input("Enter a value for multiplicate: "))
         source[col_name] = source[col_name] * multiplicate_term
         print(source)
         return source

     def Div_fun(self, source):
         col_name = input("Enter a column for Division:")
         dividing_term = int(input("Enter a value for divider: "))
         source[col_name] = source[col_name]/dividing_term
         print(source)
         return source

     def sqrt_fun(self,source):
         col_name = input("Enter a column for Sqrt:")
         source[col_name] =np.sqrt(source[col_name])
         print(source)
         return source

     def cuberoot_fun(self, source):
         col_name = input("Enter a column for Cube root:")
         source[col_name] = np.cbrt(source[col_name])
         print(source)
         return source


     def log_fun(self,source):
         col_name = input("Enter a column for logrithm:")
         source[col_name] = np.log(source[col_name])
         print(source)
         return source

     def exponetial_fun(self,source):
         col_name = input("Enter a column for exponetial:")
         source[col_name] = np.exp(source[col_name])
         print(source)
         return source

     def Add_Col_fun(self,source):
        col_name1 = input("Enter the first column name :")
        col_name2 = input("Enter the second column name:")
        source['Added_column'] = source[col_name1]+source[col_name2]
        print(source)
        return source

     def Sub_Col_fun(self,source):
        col_name1 = input("Enter the first column name :")
        col_name2 = input("Enter the second column name:")
        source['subtracted_column'] = source[col_name1]-source[col_name2]
        print(source)
        return source

     def Mul_Col_fun(self,source):
        col_name1 = input("Enter the first column name :")
        col_name2 = input("Enter the second column name:")
        source['multiplied_column'] = source[col_name1]* source[col_name2]
        print(source)
        return source

     def Div_Col_fun(self,source):
        col_name1 = input("Enter the first column name :")
        col_name2 = input("Enter the second column name:")
        source['Divison_column'] = source[col_name1]/ source[col_name2]
        print(source)
        return source

class swap():
   def __init__(self, source):
        self.source = source


   def swap_rows(self,source):
      # index = input("enter the index column:")
      # df = source.set_index(index)
       index1 = int(input("enter the row index want to be change:"))
       index2 = int(input("enter the row index have to replace in given position:"))
       source.iloc[index1], source.iloc[index2] = source.iloc[index2].copy(), source.iloc[index1].copy()
       print(source)
       return source



class string_functions():
    def __init__(self, source):
        self.source = source

    def upper_fun(self,source):
        col_name = input("Enter a column for converting upper case:")
        source[col_name] = np.char.upper(source[col_name])
        print(source)
        return source

    def lower_fun(self,source):
        col_name = input("Enter a column for converting lower case:")
        source[col_name] = np.char.lower(source[col_name])
        print(source)
        return source

    def split_fun(self, source):
        col_name = input("Enter a column for converting lower case:")
        split_key = input("Enter a split key for converting:")
        source[col_name] = np.char.split(col_name, sep = split_key)
        print(source)
        return source

    def Concat_fun(self,source):
        col_name1 = input("Enter the first column:")
        col_name2 = input("Enter the second column:")
        source['Concat_column'] = source[col_name1]+" "+source[col_name2]
        print(source)
        return source


class currency_conversion():
    def __init__(self, source):
        self.source = source


    def USD_TO_INR(self,source):
        col_name = input("Enter the column for converting USD to INR:")
        source['Indian_rupee'] = source[col_name]*81.88
        print(source)
        return source

    def INR_TO_USD(self,source):
        col_name = input("Enter the column for converting INR to USD:")
        source['USD'] = source[col_name]/81.88
        print(source)
        return source

    def RAND_TO_INR(self,source):
        col_name = input("Enter the column for converting Rand to INR:")
        source['Rand_value'] = source[col_name] / 4.48
        print(source)
        return source

    def INR_TO_RAND(self,source):
        col_name = input("Enter the column for converting Rand to INR:")
        source['INR'] = source[col_name] * 4.48
        print(source)
        return source