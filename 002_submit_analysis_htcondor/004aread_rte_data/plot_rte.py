#!/usr/bin/env python
# coding: utf-8

# In[60]:


#! cp RTE_Frequence_2022_11.txt /Users/skostogl/Documents/workspace/ADTobsbox/nxcals_martin


# In[22]:


import pandas as pd
import numpy as np
file_path = 'RTE_Frequence_2022_11.txt'
df = pd.read_csv(file_path, encoding='ISO-8859-1', skiprows=[0], delimiter=';', parse_dates=[0], decimal=',')
df = df.drop([len(df)-1])
df['DATE'] = df.apply(lambda x: pd.to_datetime(str(x['DATE']), format='%d/%m/%Y %H:%M:%S').tz_localize('CET'), axis=1)
df['FREQUENCE_RETENUE(EN Hz)']=df['FREQUENCE_RETENUE(EN Hz)'].astype(float)


# In[18]:


t1 = pd.Timestamp("2022-11-23 13:10:04.433965+00:00", tz='UTC')
t2 = pd.Timestamp("2022-11-23 13:49:59.655837+00:00", tz='UTC')

df_int = df[(df['DATE']>=t1) & (df['DATE']<=t2)]


# In[19]:


df_int


# In[31]:


import matplotlib.pyplot as plt
fig = plt.figure(figsize=(10,8))
myf = np.arange(8000, 8250, 50)
k = myf/50.

for i in range(len(myf)):
    plt.plot( (df_int['FREQUENCE_RETENUE(EN Hz)']-50.0)*k[i] + myf[i], df_int['DATE'])


# In[ ]:




