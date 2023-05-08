from processor import Dataprocessor
from data_comapre import Compare
import numpy as np
from filtering import Arithmetic, Datetime, DataType, String #,Swap     #,Multiple_col,Currency_conversion
def main():
    src_type = input("Enter the type_ of file: Flat_file or Database :")
    source = Dataprocessor(type_=src_type).processor()
    #print(source.Store)
    #sorting = source.sorting(col_name=input("Enter the column names: "))
    #print(sorting)
    filter = source.filter(col=input("col: ").strip(),op = Datetime.conversion,type_= '%Y/%m/%d')
    print(filter)
    profiling = source.profile()
    print(profiling)
    schema = source.get_schema()
    print(schema)
    tar_type = input("Enter the type_ of file: 1-Flat_file  2-Database :")
    target = Dataprocessor(type_=tar_type).processor()
    cmp = Compare(source.Store, target.Store).default()


if __name__ == '__main__':
    main()