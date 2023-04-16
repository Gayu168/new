import pandas as pd
import numpy as np
import cProfile
pd.options.display.max_columns = None
pd.options.display.max_rows = None
pd.options.display.width = 1000

if __name__ == '__main__':
  source = pd.read_csv(r"C:\Users\Obuli\Downloads\SampleSuperstore1.csv")
  target = pd.read_csv(r"C:\Users\Obuli\Downloads\SampleSuperstore_changes.csv")
 # source = pd.read_table(r"C:\Users\Obuli\Downloads\1crore.txt", delimiter='|')
  #target = pd.read_table(r"C:\Users\Obuli\Downloads\1crore1.txt", delimiter='|')
#print(source)
#print(target)
  s_eq = pd.DataFrame()
  t_eq = pd.DataFrame()
  target_df = pd.DataFrame()
  source_df =  pd.DataFrame()
  source_index = list(range(len(source)))
  target_index = list(range(len(target)))

  for i in range(2):             #len(tuple(zip(source,target)))):
    source_col = input('Choose the source col:')
    target_col = input('Choose the target col:')
    s_col = pd.Series(source[source_col].eq(target[target_col]))
    t_col = pd.Series(target[target_col].eq(source[source_col]))

    try:
     # source_index = s_col[(s_col == False)]
      #print(source_index)
      #q2.append(source_index.index)
      #print(q2)
      bool_df1 = s_col[(s_col == False)]
      source_index.extend(list(bool_df1.index))
      #print(source_index)
      #source_index.append(list(bool_df1.index))
      bool_df2 = t_col[(t_col == False)]
      target_index.extend(list(bool_df2.index))
      #print(target_index)

    except:
       continue
  #print(stack)
  index_value = list(set(source_index))
  s_eq = source.iloc[index_value]
  #print(s_eq)
  index_value = list(set(source_index))
  t_eq = source.iloc[index_value]
  columns = list(s_eq)
  columns1 = list(t_eq)

  for i,j in (zip(columns,columns1)):
     target_df = pd.concat([s_eq[i], t_eq[j]], axis=1)
     source_df = pd.concat([source_df, target_df], axis=1)


  print(source_df)
  print(source_df.shape[0])
  print(source_df.shape[1])




