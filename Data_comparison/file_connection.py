from dataclasses import dataclass
import pandas as pd


@dataclass
class FileConnection:
    """ This Class contains comparison of flat_file . First check the type format of file
    after execute schema comparison of flat file """
    file_type: str
    file_path: str

    @staticmethod
    def read_csv(file_path: str) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path, encoding='ISO-8859-1', infer_datetime_format=True)
        except Exception as e:
            raise ValueError(f"Error reading CSV file: {e}")

    @staticmethod
    def read_excel(file_path: str) -> pd.DataFrame:
        try:
            return pd.read_excel(file_path)
        except Exception as e:
            raise ValueError(f"Error reading Excel file: {e}")

    @staticmethod
    def read_text(file_path: str) -> pd.DataFrame:
        try:
            sep = input('Enter the delimiter')
            return pd.read_csv(file_path, encoding='ISO-8859-1', sep=sep)
        except Exception as e:
            raise ValueError(f"Error reading Text file: {e}")

    @staticmethod
    def read_json(file_path: str) -> pd.DataFrame:
        try:
            return pd.read_json(file_path, encoding='ISO-8859-1')
        except Exception as e:
            raise ValueError(f"Error reading Text file: {e}")

    @staticmethod
    def read_pickle(file_path: str) -> pd.DataFrame:
        try:
            return pd.read_pickle(file_path)
        except Exception as e:
            raise ValueError(f"Error reading Text file: {e}")

    @staticmethod
    def read_parquet(file_path: str) -> pd.DataFrame:
        try:
            return pd.read_parquet(file_path)
        except Exception as e:
            raise ValueError(f"Error reading Text file: {e}")

    def create_dataframe(self):
        formats = {
                    'csv': (FileConnection.read_csv, {}),
                    'excel': (FileConnection.read_excel, {}),
                    'json': (FileConnection.read_json, {}),
                    'text': (FileConnection.read_text, {}),
                    'pickle': (FileConnection.read_pickle, {}),
                    'parquet': (FileConnection.read_parquet, {})
                   }
        try:
            if self.file_type not in formats:
                raise ValueError(f"Unsupported file type: {self.file_type}")
            file1 = formats[self.file_type][0](self.file_path)
            return file1
        except Exception as e:
            raise ValueError(f"Error comparing files: {e}")
