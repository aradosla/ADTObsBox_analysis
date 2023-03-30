import pandas as pd


df_fills = pd.read_parquet('fills.parquet', engine='pyarrow')
#df_fills = pd.read_parquet("fills.parquet")
if df_fills['HX:FILLN'].iloc[0] == None:
  df_fills['HX:FILLN'].iloc[0] = df_fills['HX:FILLN'].dropna().iloc[0]
df_fills['HX:FILLN'] = df_fills['HX:FILLN'].ffill(axis=0)
print(df_fills['HX:FILLN'].dropna().unique())

bunches_max = []
for i in df_fills['HX:FILLN'].dropna().unique():
  bunches  = df_fills[df_fills['HX:FILLN']==i]['LHC.BQM.B1:NO_BUNCHES'].dropna().unique()
  #bunches_max.append(bunches.max())
  print(i, bunches)
  print(i, bunches.max())
#print(bunches_max)

