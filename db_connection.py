from typing import Union
import pandas as pd
import sqlalchemy.engine.base
from sqlalchemy import create_engine, text


class DatabaseConnection:

    def __init__(self, database_url: str = None):
        self.cnx = self.get_connection(url=database_url)

    @staticmethod
    def get_connection(url: str) -> Union[sqlalchemy.engine.base.Connection, str]:
        """
        Establish the connection
        """
        try:
            engine = create_engine(url)
            return engine.connect()
        except Exception as e:
            return f"Error {e}"

    def get_data_from_database(self) -> Union[pd.DataFrame, str]:
        """
        Query from user for source database
        """
        try:
            query = input("enter the sql query :").strip()
            df = pd.read_sql_query(sql=text(query), con=self.cnx)
            return df
        except Exception as e:
            return f"Error:{e}"

    def get_database_schema(self) -> Union[pd.DataFrame, str]:
        """
        Query the INFORMATION_SCHEMA for the list of tables in each database
        arg: cnx -> Database connection
        return: dictionary of column and data types of the schema
        """
        try:
            query = input("enter the sql query for dataschema: ").strip()
            schema = pd.read_sql_query(sql=text(query), con=self.cnx)
            # return dict((column, datatype) for column, datatype in zip(schema.iloc[0], schema.iloc[1]))
            return pd.read_sql_query(sql=text(query), con=self.cnx)
        except Exception as e:
            return f"Error {e}"