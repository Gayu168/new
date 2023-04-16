from __future__ import annotations
import pandas as pd
from dataclasses import dataclass


@dataclass
class Compare:

    """
    Compare the source and target dataframe from user
    """


    def concat_col(self, source_col_: str, target_col_: str) -> pd.DataFrame:
        """
        Columns are arranged together in a single Dataframe
        """

        s_col = self.src[source_col_].rename(f's_{source_col_}')
        t_col = self.tar[target_col_].rename(f't_{target_col_}')
        return pd.concat([s_col, t_col], axis=1)

    def mask(self, source_col: str, target_col: str) -> pd.DataFrame:
        """
        Getting  column name of source and target from user and concatenate with target
        """

        s_eq = self.src[source_col].eq(self.tar[target_col]).rename(f's_{source_col}')
        t_eq = self.tar[target_col].eq(self.src[source_col]).rename(f't_{target_col}')
        return pd.concat([s_eq, t_eq], axis=1)

    @staticmethod
    def color_mismatch(val: bool) -> str:
        """ Highlighting the mismatched data of source and target with red color"""

        return 'background-color: red' if val is False else ''

    def output(self):
        try:
            self.original_frame.style.apply(lambda x: self.bool_frame.applymap(Compare.color_mismatch), axis=None).\
                to_excel("output.xlsx", index=False)
        except PermissionError:
            print('File permission denied')

    def default(self):
        src_cols = self.src.columns
        tar_cols = self.tar.columns
        concat_cols = (self.concat_col(src_col, tar_col) for src_col, tar_col in zip(src_cols, tar_cols))
        original_df = pd.concat(concat_cols, axis=1)
        mask_ = (self.mask(src_col, tar_col) for src_col, tar_col in zip(src_cols, tar_cols))
        bool_df = pd.concat(mask_, axis=1)
        self.bool_frame = bool_df[(bool_df == False).any(axis=1)]
        self.original_frame = original_df.iloc[self.bool_frame.index, :]
        print(self.original_frame)
        return self.output()
