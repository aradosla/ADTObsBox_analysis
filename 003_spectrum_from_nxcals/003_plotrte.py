import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import matplotlib.dates as mdates

import numpy as np
import pandas as pd
import matplotlib.cm as cm
import matplotlib.pyplot as plt
from scipy.interpolate import griddata
from scipy.ndimage.filters import gaussian_filter
from matplotlib.ticker import (MultipleLocator, FormatStrFormatter,
                               AutoMinorLocator)
import matplotlib.patches as mpatches
params = {'xtick.labelsize': 20,
'ytick.labelsize': 20,
'font.size': 20,
'figure.autolayout': True,
'figure.figsize': (12, 9),
'axes.titlesize' : 23,
'axes.labelsize' : 23,
'lines.linewidth' : 1,
'lines.markersize' : 6,
'legend.fontsize': 18,
'mathtext.fontset': 'stix',
'font.family': 'STIXGeneral'}
#plt.rcParams['figure.dpi'] = 300
plt.rcParams.update(params)
#plt.rcParams.update(plt.rcParamsDefault)
n = 200

fill_nb = 8496
path = "from_nxcals_spectrum"
path_save = "/eos/user/a/aradosla/SWAN_projects/Analysis/ADTObsBox_analysis/results" 
bunch_nb = 'all'
file_path = '/afs/cern.ch/work/a/aradosla/private/ADTObsBox_analysis/read_rte_data/RTE_Frequence_2022_11.txt'
df_rte = pd.read_csv(file_path, encoding='ISO-8859-1', skiprows=[0], delimiter=';', parse_dates=[0], decimal=',')
df_rte = df_rte.drop([len(df_rte)-1])
df_rte['DATE'] = df_rte.apply(lambda x: pd.to_datetime(str(x['DATE']), format='%d/%m/%Y %H:%M:%S').tz_localize('CET'), axis=1)
df_rte['FREQUENCE_RETENUE(EN Hz)']=df_rte['FREQUENCE_RETENUE(EN Hz)'].astype(float)
print("Done reading rte")



for filename in [f"FFT_8496_B1H_2022-11-27 20:01:12.404461_2022-11-28 05:01:16.187474.parquet"]:#,
                 #f"FFT_Fill{fill_nb}_Q10_B1V.parquet",
                # f"FFT_Fill{fill_nb}_Q10_B2H.parquet",
                # f"FFT_Fill{fill_nb}_Q10_B2V.parquet"]:

  #filename = "FFT_Fill7966_Q10_B2V.parquet"
  #pu = filename.split("_")[-2]
  fill = filename.split("_")[0][4:]
  print('fill = ', fill)
  beamplane = filename.split("_")[1][3:]
  
  df = pd.read_parquet(f"{path}/{filename}")
  #df.set_index("time", inplace=True)
  save_to  = f"{path_save}/plots_{fill}_{beamplane}_{bunch_nb}_zoom_rte"
  from pathlib import Path
  Path(save_to).mkdir(parents=True, exist_ok=True)
  
  #my_df = pd.read_parquet(f'/eos/project/l/lhc-lumimod/LuminosityFollowUp/2022/HX:FILLN={fill}')
  my_df = pd.read_parquet(f'/afs/cern.ch/work/a/aradosla/private/ADTObsBox_analysis/003_spectrum_from_nxcals/fills.parquet')
  np.unique(my_df["HX:BMODE"].dropna())
  #my_df = my_df[my_df["HX:FILLN"] == '8469']["HX:BMODE"].dropna()
  


  #my_df = pd.read_parquet("fills.parquet")
  if my_df['HX:FILLN'].iloc[0] == None:
    my_df['HX:FILLN'].iloc[0] = my_df['HX:FILLN'].dropna().iloc[0]
  my_df['HX:FILLN'] = my_df['HX:FILLN'].ffill(axis=0)



  dff = df.dropna()
  dff['fourier'] = dff['fourier'].dropna()
  print('dff[fourier]', dff['fourier'])
  print(dff)
  myt1 = dff.index[0]
  myt2 = dff.index[-1]
  
  if True:
 
  
      #for counter, ii in enumerate(range(0, 5000,200)):
      ii=7600.0 
      x_lims = mdates.date2num(dff.index.values)  
      #x_lims2 = mdates.date2num(dfmains2.index.values)  
      
      frev=11245.5
      #freqs = np.linspace(0, frev*2., len(dff["fourier"].iloc[0]))
      ############
      freqs = np.linspace(0, frev*2., len(dff["fourier"].iloc[0]))
      ############
      #freqs = dff.freqs.values[0]
      ################
      fourier_abs = np.array(dff.fourier.to_list())
      #############
      #fourier_abs = np.array(dff.fourier_abs.to_list())
      
           
      
      myfilter = (freqs>7580) & (freqs<7620) 
      #myfilter = (freqs>2980) & (freqs<3020)
      fig, ax = plt.subplots()
  
      
      
      plt.imshow(np.array(np.log10(fourier_abs)[:, myfilter]), aspect='auto', cmap='jet', extent=[freqs[myfilter][0], freqs[myfilter][-1], x_lims[0], x_lims[-1]])
      ##plt.imshow(np.array(np.log10(fourier_abs)[:, myfilter]), aspect='auto', cmap='jet')
      
      
      plt.pcolormesh(freqs[myfilter], dff.index.values, np.array(np.log10(fourier_abs)[:, myfilter]), cmap='jet', shading='auto')
      
      #plt.plot(dfmains2.f.values, dfmains2.index.values, c='k', lw=5)
      
      #for i in range(50, 12000, 50):
      #    kk = i/50.
      #    plt.plot(i+(dfmains2.f.values-50.)*kk, dfmains2.index.values, c='b', lw=2)
      print(ax.get_xlim())
      #plt.plot([50. for i in dfmains2["f"]], dfmains2.index.values, lw=2, c='k')
      
      plt.xlim(freqs[myfilter][0], freqs[myfilter][-1])
      plt.ylim(dff.index.values[0], dff.index.values[-1])
      #plt.ylim(dff.index.values[0], dff.index.values[-1])
      plt.colorbar()
      ax.yaxis_date()
      date_format = mdates.DateFormatter('%H:%M:%S')
      ax.yaxis.set_major_formatter(date_format)
      plt.title(f'Fill {fill}, {beamplane}')
      # This simply sets the x-axis data to diagonal so it fits better.
      fig.autofmt_xdate()
      plt.xlabel('f (Hz)')
      #plt.ylabel("UTC time 02/07/2022")
      #plt.ylabel("UTC time 06/07/2022")
      plt.ylabel("UTC time")
      
      
      my_df = my_df[my_df["HX:FILLN"] == '8469']
      
      myf = np.array([7600.0])#np.arange(ii-10,ii+n+10, 50)
      k = myf/50.

      
      #first_HX_BMODE = my_df['HX:BMODE'].unique()[0]
      t1 = myt1#pd.Timestamp(my_df[my_df['HX:BMODE'] == first_HX_BMODE].iloc[0].name, tz='UTC')
      #last_HX_BMODE = my_df['HX:BMODE'].unique()[-1]
      t2 = myt2#pd.Timestamp(my_df[my_df['HX:BMODE'] == last_HX_BMODE].iloc[0].name,tz ='UTC')


      df_int = df_rte[(df_rte['DATE']>=t1) & (df_rte['DATE']<=t2)]


      
      for mode in ["RAMP", "FLATTOP", "ADJUST"]:#, "SQUEEZE", "STABLE"]:
          tt=pd.Timestamp(my_df[my_df["HX:BMODE"] == mode].iloc[0].name)
          plt.axhline(tt, c='k', lw=2)
          print('tt', tt)
          plt.text(ii, tt, mode, c='k', fontsize=18)
  
      

      for i in range(len(myf)):
          plt.plot( (df_int['FREQUENCE_RETENUE(EN Hz)']-50.0)*k[i] + myf[i], df_int['DATE'])

      
      fig.tight_layout()
      
      fig.savefig(f"{save_to}/testnew.png")
      #fig.savefig(f"plots_MD_latest/plot_{counter}_{ii}Hz_{ii+200}Hz.png")
      
      plt.close("all")
  
