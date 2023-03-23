import pandas as pd
import json
import numpy as np


class NumpyEncoder(json.JSONEncoder):
    """ Custom encoder for numpy data types """

    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):

            return int(obj)

        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)

        elif isinstance(obj, (np.complex_, np.complex64, np.complex128)):
            return {'real': obj.real, 'imag': obj.imag}

        elif isinstance(obj, np.ndarray):
            return obj.tolist()

        elif isinstance(obj, np.bool_):
            return bool(obj)

        elif isinstance(obj, np.void):
            return None
        elif isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, object):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


class Profiler:

    @staticmethod
    def column_statistics(df: pd.DataFrame) -> dict:
        """ stats analysis mean,std,max,min,max,count """

        # print("Statistics analysis ")
        stat = {"Column_statistics": {}}
        for n_col in df.select_dtypes(include=['number']).columns:
            # print(n_col)
            stat['Column_statistics'][n_col] = {
                f"Mean of Column Values:{df[n_col].mean()}",
                f"Standard Deviation of Values:{df[n_col].std()}",
                f"Median of Values:{df[n_col].median()}",
                f"Maximum of Values:{df[n_col].max()}",
                f"Minimum of Values:{df[n_col].min()}",
                f"Count of Non-null Values:{df[n_col].count()}"
            }
        return stat

    @staticmethod
    def columns_analysis(df):
        """ column analysis unique,nan,memory_usage,freq,null """
        analysis = {'Column_Analysis': {}}
        for col in df.columns:
            col_data = df[col]
            analysis['Column_Analysis'][col] = {
                "data_type": str(col_data.dtype),
                "unique_values": col_data.nunique(),
                "is_unique": col_data.is_unique,
                "is_sorted": col_data.is_monotonic_increasing,
                "nan_values": col_data.isnull().sum(),
                "not_null_values": col_data.notnull().sum(),
                "most_frequent_value": col_data.value_counts().idxmax(),
                "memory_usage": col_data.memory_usage(deep=False),
            }
            if np.issubdtype(col_data.dtype, np.number):
                analysis['Column_Analysis'][col]["negative_values"] = (col_data < 0).sum()
                analysis['Column_Analysis'][col]["zeros"] = (col_data == 0).sum()
                analysis['Column_Analysis'][col]["positive_values"] = (col_data > 0).sum()
        return analysis

    @staticmethod
    def table_profiling(df: pd.DataFrame) -> dict:
        """ overview tabs n_rows,n_cols,numeric,category,duplicate """

        table_info = {'OVERVIEW': {f"TOTAL NUMBER OF ROWS:{df.shape[0]}",
                                   f"TOTAL NUMBER OF COLUMNS:{df.shape[1]}",
                                   f"NUMBER OF Numeric TYPE:{len(df.select_dtypes(include=['number']).columns.tolist())}",
                                   f"NUMBER OF Categorical TYPE:{len(df.select_dtypes(include=['object']).columns.tolist())}",
                                   f"TOTAL NUMBER OF DUPLICATES:{df.duplicated(keep=False).sum()}",
                                   f"MEMORY_USAGE:{sum(df.memory_usage(index=False, deep=False))} B",
                                   }
                      }

        return table_info

    @staticmethod
    def json_formatter(data: dict) -> str:
        json_obj = json.dumps(data, indent=4, cls=NumpyEncoder)
        return json_obj


def json_profiling(dataframe: pd.DataFrame) -> str:
    # print("profiling frame", dataframe)
    profile = Profiler()
    m1 = profile.table_profiling(df=dataframe) | profile.column_statistics(df=dataframe)
    m2 = m1 | profile.columns_analysis(df=dataframe)
    return profile.json_formatter(m2)