from __future__ import annotations
from typing import Union, List
from dataclasses import dataclass
import pandas as pd
from file_connection import FileConnection
from db_connection import DatabaseConnection
from filtering import Sorting, Filter
from profiilng import json_profiling
from config import Database_url


@dataclass
class FileType:
    FLAT_FILE = 'flat_file'
    DATABASE = 'database'


@dataclass
class Dataprocessor:
    type_: str
    Store: pd.DataFrame = None
    Connect = None

    def processor(self) -> Union[Dataprocessor, str]:
        """
        arg: type_:
        """
        type_check = {FileType.FLAT_FILE: self.file_processor, FileType.DATABASE: self.database_processor}
        checker = type_check.get(self.type_)
        if checker is not None:
            self.Store = checker()
            return self
        return "invalid file type"

    @staticmethod
    def file_processor():
        path = input("Enter the source: ")
        file_type = input("Enter the type_: ")
        file_con = FileConnection(file_path=path, file_type=file_type)
        return file_con.create_dataframe()

    @staticmethod
    def database_processor():
        connect = DatabaseConnection(database_url=Database_url)
        return connect.get_data_from_database()

    def sorting(self, col_name: Union[List[str], str]) -> pd.DataFrame:
        """ Gathering the column details from user for sorting dataframe."""
        sort_cols = Sorting(cols=col_name)
        self.Store = self.Store.sort_values(by=sort_cols.cols, ascending=True)
        return self.Store

    def filter(self, col: str, op, val: int = None, type_: str = None) -> pd.DataFrame:
        fltr = Filter(dataframe=self.Store, col=col, val=val, type_=type_, op=op, col1=col1)
        return fltr.operation()

    def get_schema(self):
        return self.Store.dtypes

    def profile(self):
        return json_profiling(dataframe=self.Store)