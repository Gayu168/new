import pandas as pd
import numpy as np
from typing import Union, List
from dataclasses import dataclass
import pyarrow as pa
import pyarrow.compute as pc


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

        self.Dataframe[self.col] = self.Dataframe[self.col].astype(self.type_)
        print(self.Dataframe)
        return self.Dataframe



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


class Multiple_col:
    def __init__(self, dataframe: pd.DataFrame, col: str, col1 :str):
        self.Dataframe = dataframe
        self.col = col
        self.col1 = col1

    def add_col_fun(self):
        self.Dataframe['Added_column'] = self.Dataframe[self.col]+self.Dataframe[self.col1]
        print(self.Dataframe)
        return self.Dataframe

    def sub_col_fun(self):
        self.Dataframe['Added_column'] = self.Dataframe[self.col] - self.Dataframe[self.col1]
        print(self.Dataframe)
        return self.Dataframe

    def mul_col_fun(self):
        self.Dataframe['Added_column'] = self.Dataframe[self.col] * self.Dataframe[self.col1]
        print(self.Dataframe)
        return self.Dataframe

    def div_col_fun(self):
        self.Dataframe['Added_column'] = self.Dataframe[self.col] / self.Dataframe[self.col1]
        print(self.Dataframe)
        return self.Dataframe


@dataclass
class String:

    def __init__(self, dataframe: pd.DataFrame, col: str,col1:str = None):
        self.Dataframe = dataframe
        self.col = col
        self.col1 = col1

    def upper(self):
        #self.Dataframe[self.col] = pc.compute.ascii_upper(self.Dataframe[self.col])
        #self.Dataframe[self.col] = pc.utf8_upper(self.Dataframe[self.col])
        self.Dataframe[self.col] = self.Dataframe[self.col].str.upper().astype(str)
        print(self.Dataframe)
        return self.Dataframe

    def lower(self):
        self.Dataframe[self.col] = self.Dataframe[self.col].str.lower().astype(str)
        print(self.Dataframe)
        return self.Dataframe

  
    def concat(self):
        self.Dataframe[self.col] = self.Dataframe[self.col]+"" +self.Dataframe[self.col1]
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
        self.Dataframe = dataframe
        self.col = col

    def usd_to_inr(self):
        self.Dataframe['Indian_rupee'] = self.Dataframe[self.col]* 81.88
        return self.Dataframe

    def inr_to_usd(self):
        self.Dataframe['USD'] = self.Dataframe[self.col] / 81.88
        return self.Dataframe

    def rand_to_inr(self):
         self.Dataframe['INR'] = self.Dataframe[self.col] / 4.48
         return self.Dataframe

    def inr_to_rand(self):
        self.Dataframe['Rand'] = self.Dataframe[self.col] * 4.48
        return self.Dataframe



class Swap():
    def __init__(self,dataframe ):       #,index1: int = None,index2: int = None
        self.Dataframe = dataframe
      #  self.index1 = index1
       # self.index2 = index2

    def swap_rows(self):
        index1 = int(input("enter the row index want to be change:"))
        index2 = int(input("enter the row index have to replace in given position:"))
        self.Dataframe.iloc[index1], self.Dataframe.iloc[index2] =self.Dataframe.iloc[index2].copy(), self.Dataframe.iloc[index1].copy()
        print(self.Dataframe)
        #return source


class Filter(Multiple_col,DataType, Datetime, Arithmetic, String):
    def __init__(self, dataframe: pd.DataFrame, col: str, op, val: int = None, type_: str = None, col1 : str = None,index1 : int = None ,index2 : int = None ):
        self.op = op
        self.type_ = type_
        String.__init__(self, dataframe=dataframe, col=col,col1 = col1)
        Arithmetic.__init__(self, dataframe=dataframe, col=col, val=val)
        Datetime.__init__(self, dataframe=dataframe, col=col, type_=type_)
        DataType.__init__(self, dataframe=dataframe, col=col, type_=type_)
        Currency_conversion.__init__(self, dataframe=dataframe, col=col)
        Multiple_col.__init__(self,dataframe = dataframe, col = col, col1 =col1)
        Swap.__init__(self,dataframe=dataframe)

    def operation(self):
        print("op", self.op)
        return self.op(self)
