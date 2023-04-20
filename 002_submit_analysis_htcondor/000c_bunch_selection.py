import h5py
import datetime
import os
import tree_maker
import yaml
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from useful_tools.useful_functions import *

path = 'Fill8469_3'
dirs = next(os.walk(path))[1]
dirs = [dir for dir in dirs if dir.isdigit()]
dirs.sort()
last_dir = dirs[-1]
config_file_path = os.path.join(path, last_dir, 'config.yaml')
print(config_file_path)
#print(last_dir)
tot_num = 2

#with open('Fill8469_3/037/config.yaml','r') as fid:
with open(config_file_path,'r') as fid:
    config=yaml.safe_load(fid)

filenames_to_consider = config['adt_file']
filenames_to_consider = [filenames_to_consider[0]]

adt = ADT(filenames_to_consider)
myDict = adt.importEmptyDF
#quit()

repeat_fft = 3
frev = 11245.5

which_bunch_b1 = 'all'
which_bunch_b2 = 'all'

for beamplane in myDict.keys():
    print(beamplane, beamplane[0:2])
    myDF = myDict[beamplane].copy()

    #try:
    if True:
        myDF['Data'] = myDF['Path'].apply(adt.loadData)

        for counter in range(len(myDF)):
            data = myDF.iloc[counter]['Data']
           
            if beamplane[0:2] == 'B1':
                if which_bunch_b1 =='all':
                   idx = np.where(abs(data[0,:])>0.0)[0]
                else:
                    idx = which_bunch_b1

            elif beamplane[0:2] == 'B2':
                if which_bunch_b2 =='all':
                   idx = np.where(abs(data[0,:])>0.0)[0]
                else:
                    idx = which_bunch_b2
            filling_scheme = np.zeros(3564)
            filling_scheme[idx] = 1
           # pd.DataFrame({'Filling scheme':filling_scheme}).to_parquet('filling.parquet')
            print('bunches = ',np.where(filling_scheme>0)[0])
            print('Bunches or where filling scheme > 0: ', len(np.where(filling_scheme>0)[0]))
            print(filling_scheme)
            print(f"Bunch slots {idx}")

            
bunches = np.where(filling_scheme>0)[0]


number = [''] * tot_num
closest = [''] * tot_num

for i in range(tot_num):
    number[i] = (i + 1) * (3564 - bunches[0])/tot_num # closest[i] = min(bunches, key=lambda x: abs(x - number[i]))
    closest[i] = min(bunches, key=lambda x: abs(x - number[i]))
'''
#closest = []
used_values = set()

for i in range(tot_num):
    # find the closest value
    closest_i = min(bunches, key=lambda x: abs(x - number[i]))
    
    # check if closest[i] is equal to closest[i-1]
    if i > 0 and closest_i == closest[i-1]:
        # search for the next closest value that is not already used
        while closest_i in used_values:
            closest_i = min(bunches, key=lambda x: abs(x - number[i]))
        
    closest.append(closest_i)
    used_values.add(closest_i)


'''   
used_values = closest[:]
print(used_values)
if closest[i] == closest[i-1]:
    for val in bunches:
        if val not in used_values:
            closest[i] = val
            used_values.append(val)
            break
print(used_values) 
    #if j in closest == j+1:
     #   closest[j] = bunch
print(closest)
closest_unique = np.unique(closest)
#print(number)
print(used_values)
print(closest_unique)
df = pd.DataFrame(closest_unique, columns=['Equally spaces'])
plt.plot(df['Equally spaces'])
plt.show()
df.to_parquet('bunches_used.parquet')
#np.savetxt('buches_used.txt', closest_unique) 
###### Select automatically the first non zero value and do the bunch calculation ##############
