import pandas as pd
import numpy as np
from typing import Union, List
from dataclasses import dataclass


@dataclass
class Sorting:
    cols: Union[List[str], str]

    def __init__(self, cols):
        self.cols = cols
        self.split()

    def split(self):
        keys = self.cols.split(',')
        self.cols = [col.strip() for col in keys]
        return self


class DataType:
    def __init__(self, dataframe: pd.DataFrame, col: str, type_: str):
        self.Dataframe = dataframe
        self.col = col
        self.type_ = type_

    def convert(self):
        self.Dataframe.astype({f"{self.col}:{self.type_}"})


class Arithmetic:

    def __init__(self, dataframe: pd.DataFrame, col: str, val: int):
        self.Dataframe = dataframe
        self.col = col
        self.val = val

    def add(self):
        self.Dataframe[self.col] = self.Dataframe[self.col] + self.val
        print(self.Dataframe)
        return self.Dataframe

    def sub(self):
        self.Dataframe[self.col] = self.Dataframe[self.col] - self.val
        print(self.Dataframe)
        return self.Dataframe

    def mul(self):
        self.Dataframe[self.col] = self.Dataframe[self.col] * self.val
        print(self.Dataframe)
        return self.Dataframe

    def div(self):
        self.Dataframe[self.col] = self.Dataframe[self.col] / self.val
        print(self.Dataframe)
        return self.Dataframe

    def sqrt(self):
        self.Dataframe[self.col] = np.sqrt(self.Dataframe[self.col])
        print(self.Dataframe)
        return self.Dataframe

    def cbrt(self):
        self.Dataframe[self.col] = np.cbrt(self.Dataframe[self.col])
        print(self.Dataframe)
        return self.Dataframe

    def log(self):
        self.Dataframe[self.col] = np.log(self.Dataframe[self.col])
        print(self.Dataframe)
        return self.Dataframe

    def exp(self):
        self.Dataframe[self.col] = np.exp(self.Dataframe[self.col])
        print(self.Dataframe)
        return self.Dataframe

    def rounding(self):
        self.Dataframe[self.col] = self.Dataframe[self.col].round(self.val)
        return self.Dataframe


class multiple_col:
    def __init__(self, dataframe: pd.DataFrame, col1: str, col2 :str):
        self.Dataframe = dataframe
        self.col1 = col1
        self.col2 = col2

    def Add_Col_fun(self, source):
        col_name1 = input("Enter the first column name :")
        col_name2 = input("Enter the second column name:")
        source['Added_column'] = source[col_name1] + source[col_name2]
        print(source)
        return source

    def Sub_Col_fun(self, source):
        col_name1 = input("Enter the first column name :")
        col_name2 = input("Enter the second column name:")
        source['subtracted_column'] = source[col_name1] - source[col_name2]
        print(source)
        return source

    def Mul_Col_fun(self, source):
        col_name1 = input("Enter the first column name :")
        col_name2 = input("Enter the second column name:")
        source['multiplied_column'] = source[col_name1] * source[col_name2]
        print(source)
        return source

    def Div_Col_fun(self, source):
        col_name1 = input("Enter the first column name :")
        col_name2 = input("Enter the second column name:")
        source['Divison_column'] = source[col_name1] / source[col_name2]
        print(source)
        return source


@dataclass
class String:

    def __init__(self, dataframe: pd.DataFrame, col: str):
        self.Dataframe = dataframe
        self.col = col

    def upper(self):
        self.Dataframe[self.col] = np.char.upper(self.Dataframe[self.col])
        print(self.Dataframe)
        return self.Dataframe

    def lower(self):
        self.Dataframe[self.col] = np.char.lower(self.Dataframe[self.col])
        print(self.Dataframe)
        return self.Dataframe



class Datetime:
    def __init__(self, dataframe, col, type_: str = None):
        self.Dataframe = dataframe
        self.col = col
        self.type_ = type_

    def conversion(self):
        self.Dataframe[self.col] = pd.to_datetime(self.Dataframe[self.col]).dt.strftime(self.type_)
        return self.Dataframe

class Currency_conversion:
    def __init__(self, dataframe, col:str):
        self.Dataframe[self.col]= dataframe
        self.col = col

    def USD_TO_INR(self):
        self.Dataframe['Indian_rupee'] = self.Dataframe[self.col]* 81.88
        return self.Dataframe

    def INR_TO_USD(self,source):
        self.Dataframe['USD'] = self.Dataframe[self.col] / 81.88
        return self.Dataframe

    def RAND_TO_INR(self,source):
         self.Dataframe['INR'] = self.Dataframe[self.col] / 4.48
         return self.Dataframe

    def INR_TO_RAND(self,source):
        self.Dataframe['Rand'] = self.Dataframe[self.col] / 81.88
        return self.Dataframe

class Filter(DataType, Datetime, Arithmetic, String):
    def __init__(self, dataframe: pd.DataFrame, col: str, op, val: int = None, type_: str = None):
        self.op = op
        self.type_ = type_
        String.__init__(self, dataframe=dataframe, col=col)
        Arithmetic.__init__(self, dataframe=dataframe, col=col, val=val)
        Datetime.__init__(self, dataframe=dataframe, col=col, type_=type_)
        DataType.__init__(self, dataframe=dataframe, col=col, type_=type_)
        Currency_conversion.__init__(self, dataframe=dataframe, col=col)
        multiple_col.init(self,dataframe = dataframe, col1 = col1, col2 =col2)

    def operation(self):
        print("op", self.op)
        return self.op(self)
