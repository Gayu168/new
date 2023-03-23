import polars as pl
df = pl.read_csv( r"C:\Users\Obuli\Downloads\students_mark_changes.csv")
print(df)
df1 = pl.DataFrame({"foo": [1, 2, 3],"bar": [6.0, 7.0, 8.0],"ham": ["a", "b", "c"],})
df2 = pl.DataFrame({"foo": [3, 2, 1],"bar": [8.0, 7.0, 6.0],"ham": ["c", "b", "a"],})
result = df1.frame_equal(df1)
print(result)
res=df1.frame_equal(df2)
print(res)
