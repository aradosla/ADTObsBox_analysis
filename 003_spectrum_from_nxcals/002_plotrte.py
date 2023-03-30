import os
import getpass
import logging
import sys
import numpy as np
import datetime


import matplotlib.dates as mdates

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
'figure.figsize': (12, 11),
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

import nx2pd as nx
from nxcals.spark_session_builder import get_or_create, Flavor

#with open("config.yaml", "r") as f:
#      config = yaml.safe_load(f)

'''


logging.basicConfig(stream=sys.stdout, level=logging.INFO)

os.environ['PYSPARK_PYTHON'] = "./environment/bin/python"
username = getpass.getuser()
print(f'Assuming that your kerberos keytab is in the home folder, ' 
      f'its name is "{getpass.getuser()}.keytab" '
      f'and that your kerberos login is "{username}".')

logging.info('Executing the kinit')
os.system(f'kinit -f -r 5d -kt {os.path.expanduser("~")}/{getpass.getuser()}.keytab {getpass.getuser()}');

# %%
import json
import nx2pd as nx 
import pandas as pd

from nxcals.spark_session_builder import get_or_create, Flavor
fills = ["8496"]#df_fills['HX:FILLN'].unique()
save_to = 'from_nxcals_spectrum'


logging.info('Creating the spark instance')
#spark = get_or_create(flavor=Flavor.LOCAL)
spark = get_or_create(flavor=Flavor.LOCAL,                                         
conf={'spark.driver.maxResultSize': '8g',                                          
    'spark.executor.memory':'8g',                                                  
    'spark.driver.memory': '16g',                                                  
    'spark.executor.instances': '20',                                              
    'spark.executor.cores': '2',                                                   
    })                                                                             

#spark = get_or_create(flavor=Flavor.YARN_SMALL, master='yarn')
sk  = nx.SparkIt(spark)
logging.info('Spark instance created.')


df_fills = pd.read_parquet("fills.parquet")
if df_fills['HX:FILLN'].iloc[0] == None:
  df_fills['HX:FILLN'].iloc[0] = df_fills['HX:FILLN'].dropna().iloc[0]
df_fills['HX:FILLN'] = df_fills['HX:FILLN'].ffill(axis=0)

print(df_fills['HX:FILLN'].unique())

fills = ["8496"]#df_fills['HX:FILLN'].unique()
save_to = 'from_nxcals_spectrum'

try:
    os.makedirs(save_to)
except FileExistsError:
    pass

for current_fill in fills:
  for beamplane in ["B1H", "B1V", "B2H", "B2V"]:
  #for beamplane in ["B1H"]:
      try:
        print(current_fill, beamplane)
        try:
          df_current = df_fills[df_fills['HX:FILLN'] == current_fill]
          print(df_current)  
          print(df_current['HX:BMODE'].dropna(), df_current['LHC.BQM.B1:NO_BUNCHES'].dropna().max())
          t0 = pd.Timestamp(df_current[df_current['HX:BMODE'] == 'INJPHYS'].index[0])
          try:
            t1 = pd.Timestamp(df_current[df_current['HX:BMODE'] == 'BEAMDUMP'].index[0])
          except Exception as e:
            t1 = pd.Timestamp(df_current[df_current['HX:BMODE'] ==df_current['HX:BMODE'].dropna().iloc[-1]].index[-1])
          if current_fill=='8211':
            t1 = pd.Timestamp(df_current[df_current['HX:BMODE'] == 'BEAMDUMP'].index[-1])

          # split into 5h intervals if needed
          print(t0, t1)
          #t0 = pd.Timestamp('2022-10-23 00:00:13', tz='CET')
          #t1 = pd.Timestamp('2022-12-02 16:00', tz='CET')
  
          #t0 = pd.Timestamp("2022-10-23 09:01:07", tz='CET')
          #t1 = pd.Timestamp("2022-11-27 22:31:44", tz='CET')
          print(t0,t1)

          interval = datetime.timedelta(minutes=60*1)
          periods = []
          period_start = t0
          while period_start < t1:
              period_end = min(period_start + interval, t1)
              periods.append((period_start, period_end))
              period_start = period_end

        except Exception as e:
          print('Error! ', beamplane, current_fill, e)
          continue
        data_list = [f"ObsBox2Spectrum.LHC.ADT.REAL.{beamplane}.Q7:acquisitionCorrected:acquisition"]
        appended_df = []
        for i in range(0, len(periods)):
          print(f"hereeeee {i}")
          print(data_list,periods[i][0],periods[i][1])
          df = sk.nxcals_df(data_list,periods[i][0],periods[i][1],pandas_processing=[nx.pandas_get,nx.pandas_pivot,])
          appended_df.append(df)
        try:
          df = pd.concat(appended_df, axis=0)
          tfirst = pd.Timestamp(df.iloc[0].name)
          tlast = pd.Timestamp(df.iloc[-1].name)
        except Exception as e:
            print('Empty df! ', e)
            continue
        print(tfirst, tlast)
        df.to_parquet(f"{save_to}/FFT_{current_fill}_{beamplane}_{tfirst}_{tlast}.parquet")
      except Exception as e:
          print("Error! ", beamplane, current_fill, e)

'''
import pytz

file_path = '/afs/cern.ch/work/a/aradosla/private/ADTObsBox_analysis/read_rte_data/RTE_Frequence_2022_11.txt'
df_rte = pd.read_csv(file_path, encoding='ISO-8859-1', skiprows=[0], delimiter=';', parse_dates=[0], decimal=',')
df_rte = df_rte.drop([len(df_rte)-1])
df_rte['DATE'] = df_rte.apply(lambda x: pd.to_datetime(str(x['DATE']), format='%d/%m/%Y %H:%M:%S').tz_localize('CET'), axis=1)
df_rte['FREQUENCE_RETENUE(EN Hz)']=df_rte['FREQUENCE_RETENUE(EN Hz)'].astype(float)
print("Done reading rte")
print(df_rte['DATE'].index)
fills = ['8496']

print('Finished with the first part -> now plotting')
for fill_nb in fills:

    for beam_plane in ['B1H', 'B2H', 'B1V', 'B2V']:

      #try:
        #beam_plane = 'B2H'

        #file = glob.glob(f"{path}/*{fill_nb}*{beam_plane}*")[0]
        file = f"from_nxcals_spectrum/FFT_8496_B1H_2022-11-27 20:01:12.404461_2022-11-28 05:01:16.187474.parquet"
        print(file)
        df = pd.read_parquet(file)
        df[df.columns[0]] = df[df.columns[0]].apply(lambda x: x['elements'])
        df.index = [pd.Timestamp(df.index[i], tz=pytz.UTC) for i in range(len(df))]        

        print(df)
        myt1 = df.index[0]
        myt2 = df.index[-1]
        print('t1', myt1)
        print('t2', myt2)
        #quit()


        df_fills = pd.read_parquet("fills.parquet")
        if df_fills['HX:FILLN'].iloc[0] == None:
          df_fills['HX:FILLN'].iloc[0] = df_fills['HX:FILLN'].dropna().iloc[0]
        df_fills['HX:FILLN'] = df_fills['HX:FILLN'].ffill(axis=0)
        df_fills = df_fills[df_fills['HX:FILLN'] == fill_nb]
        df_fills.index = [pd.Timestamp(df_fills.index[i]) for i in range(len(df_fills))]
        print(df_fills)

        df_bunches = df_fills[f'LHC.BQM.B{beam_plane[1:2]}:NO_BUNCHES'].dropna().astype(int)
        bunches = df_bunches.max()

        
        save_to  = f"/eos/user/a/aradosla/SWAN_projects/Analysis/ADTObsBox_analysis/results_nxcals_one"
        from pathlib import Path
        Path(save_to).mkdir(parents=True, exist_ok=True)

        save_to  = f"{save_to}/plots_{fill_nb}/{beam_plane}"
        from pathlib import Path
        Path(save_to).mkdir(parents=True, exist_ok=True)


        for counter, ii in enumerate(range(0, 5000,200)):#(range(0, 5000,200)):

            x_lims = mdates.date2num(df.index.values)
            print(ii)
            frev=11245.5
            freqs = np.linspace(0, frev, len(df[df.columns[0]].iloc[0]))
            fourier_abs = np.array(df[df.columns[0]].to_list())

            ################################################
            myfilter = (freqs>ii-10) & (freqs<ii+210)
            fig, ax = plt.subplots()
            plt.pcolormesh(freqs[myfilter], df.index.values, np.array(np.log10(fourier_abs)[:, myfilter]), cmap='jet', shading='auto')
            plt.xlim(freqs[myfilter][0], freqs[myfilter][-1])
            plt.ylim(df.index.values[0], df.index.values[-1])
            #plt.colorbar()
            ax.yaxis_date()
            date_format = mdates.DateFormatter('%H:%M:%S')
            ax.yaxis.set_major_formatter(date_format)
            plt.title(f'Fill {fill_nb}, {beam_plane}, {bunches} bunches')
            fig.autofmt_xdate()
            plt.xlabel('f (Hz)')
            plt.ylabel(f"UTC time {df.index[0].day}/{df.index[0].month}/{df.index[0].year}")
            plot_beam_mode = True
            if plot_beam_mode:
                for mode in ["PRERAMP", "RAMP", "FLATTOP", "ADJUST", "STABLE"]:#, "SQUEEZE", "STABLE"]:
                    try:
                        tt=pd.Timestamp(df_fills[df_fills["HX:BMODE"] == mode].iloc[0].name)
                        print(tt)
                        plt.axhline(tt, c='k', lw=2)
                        plt.text(ii, tt, mode, c='k', fontsize=18)
                    except:
                        print('prob', mode)

            fig.tight_layout()
            fig.savefig(f"{save_to}/plot_{counter}_{ii}Hz_{ii+200}Hz.png")
            plt.close("all")

            #############################################
            myfilter = (freqs<11245.5-(ii-10)) & (freqs>11245.5-(ii+210))
            fig, ax = plt.subplots()
            plt.pcolormesh(freqs[myfilter], df.index.values, np.array(np.log10(fourier_abs)[:, myfilter]), cmap='jet', shading='auto')
            plt.xlim(freqs[myfilter][0], freqs[myfilter][-1])
            plt.ylim(df.index.values[0], df.index.values[-1])
            #plt.colorbar()
            ax.yaxis_date()
            date_format = mdates.DateFormatter('%H:%M:%S')
            ax.yaxis.set_major_formatter(date_format)
            plt.title(f'Fill {fill_nb}, {beam_plane}, {bunches} bunches')
            fig.autofmt_xdate()
            plt.xlabel('f (Hz)')
            plt.ylabel(f"UTC time {df.index[0].day}/{df.index[0].month}/{df.index[0].year}")
             

            myf = np.array([7600.0])#np.arange(ii-10,ii+n+10, 50)
            k = myf/50.


            #first_HX_BMODE = my_df['HX:BMODE'].unique()[0]
            t1 = myt1#pd.Timestamp(my_df[my_df['HX:BMODE'] == first_HX_BMODE].iloc[0].name, tz='UTC')
            #last_HX_BMODE = my_df['HX:BMODE'].unique()[-1]
            t2 = myt2#pd.Timestamp(my_df[my_df['HX:BMODE'] == last_HX_BMODE].iloc[0].name,tz ='UTC')
            df_int = df_rte[(df_rte['DATE']>=t1) & (df_rte['DATE']<=t2)]

             
            if plot_beam_mode:
                for mode in ["PRERAMP", "RAMP", "FLATTOP", "ADJUST", "STABLE"]:#, "SQUEEZE", "STABLE"]:
                    try:
                        tt=pd.Timestamp(df_fills[df_fills["HX:BMODE"] == mode].iloc[0].name)
                        print(tt)
                        plt.axhline(tt, c='k', lw=2)
                        plt.text(11245.5-(ii+210), tt, mode, c='k', fontsize=18)
                    except:
                        print('prob', mode)
            
            for i in range(len(myf)):
                plt.plot( (df_int['FREQUENCE_RETENUE(EN Hz)']-50.0)*k[i] + myf[i], df_int['DATE'])
   

            fig.tight_layout()
            fig.savefig(f"{save_to}/plot_{counter}_{11245.5-(ii+210)}Hz_{11245.5-(ii-10)}Hz.png")
            plt.close("all")
      #except:
      #      print('prob')

