import pandas as pd
import numpy as np
import pyarrow as pa
#create dict

d = {'a':1, 'b':2, 'c':3, 'd':5.9,'e':15}
ser = pd.Series(d)
print(ser)

print(ser.array)
print("f value:",ser.get('f'))

print("d value:",ser.get('d'))
s2 = ser.rename("different")
print(s2.name)
print(s2)
df2 = pd.DataFrame({"A": 1.0,
                    "B": pd.Timestamp("20130102"),
                    "C": pd.Series(1, index=list(range(4)), dtype="float32"),
                    "D": np.array([3] * 4, dtype="int32"),
                    "E": pd.Categorical(["test", "train", "test", "train"]),
                    "F": "foo",})
#print(df2)
#print(df2['A'])
#print(df2.loc[:, ["A", "B"]])
#print(df2.loc["2013-01-02", ["B"]])

data = list("abc")
ser_sd = pd.Series(data, dtype="string[pyarrow]")
ser_ad = pd.Series(data, dtype=pd.ArrowDtype(pa.string()))

ser_sd.str.contains("a")
ser_ad.str.contains("a")
