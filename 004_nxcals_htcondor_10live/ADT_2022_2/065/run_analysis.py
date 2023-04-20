import os
import json
import getpass
import logging
import sys
import numpy as np
import datetime
import yaml
import tree_maker
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import matplotlib.dates as mdates
import glob
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


with open("config.yaml", "r") as f:
      config = yaml.safe_load(f)

# Plot spectra

fills = config["fill_nb"]
path = '/eos/user/a/aradosla/SWAN_projects/Analysis/ADTObsBox_analysis/results_nxcals/run_2023'


for fill_nb in fills:

    if fill_nb == '8182':
        plot_beam_mode = False
    else:
        plot_beam_mode = True
    
    
    for beam_plane in ['B1H', 'B2H', 'B1V', 'B2V']:
    
      if True:
        #beam_plane = 'B2H'

        file = glob.glob(f"{path}/plots_{fill_nb}/{beam_plane}/*parquet")[0]
        print(file)
        df = pd.read_parquet(file)
        df[df.columns[0]] = df[df.columns[0]].apply(lambda x: x['elements'])
        df.index = [pd.Timestamp(df.index[i]) for i in range(len(df))]
        print(df)


        df_fills = pd.read_parquet(config["fills_path"])
        if df_fills['HX:FILLN'].iloc[0] == None:
          df_fills['HX:FILLN'].iloc[0] = df_fills['HX:FILLN'].dropna().iloc[0]
        df_fills['HX:FILLN'] = df_fills['HX:FILLN'].ffill(axis=0)
        df_fills = df_fills[df_fills['HX:FILLN'] == fill_nb]
        df_fills.index = [pd.Timestamp(df_fills.index[i]) for i in range(len(df_fills))]
        print(df_fills)

        df_bunches = df_fills[f'LHC.BQM.B{beam_plane[1:2]}:NO_BUNCHES'].dropna().astype(int)
        bunches = df_bunches.max()

        myeos=config["save_to"]
        save_to  = f"{myeos}"
        from pathlib import Path
        Path(save_to).mkdir(parents=True, exist_ok=True)

        save_to  = f"{myeos}/{beam_plane}"
        from pathlib import Path
        Path(save_to).mkdir(parents=True, exist_ok=True)    


        #for counter, ii in enumerate(range(0, 25,1)):#(range(0, 5000,200)):
        if True:
            x_lims = mdates.date2num(df.index.values)  

            frev=11245.5
            freqs = np.linspace(0, frev, len(df[df.columns[0]].iloc[0]))
            fourier_abs = np.array(df[df.columns[0]].to_list())
            line =10
            ################################################
            myfilter = (freqs>line -10) & (freqs<line+15) 
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

            if plot_beam_mode:
                for mode in ["PRERAMP", "RAMP", "FLATTOP", "ADJUST", "STABLE"]:#, "SQUEEZE", "STABLE"]:
                    try:
                        tt=pd.Timestamp(df_fills[df_fills["HX:BMODE"] == mode].iloc[0].name)
                        print(tt)
                        plt.axhline(tt, c='k', lw=2)
                        plt.text(0, tt, mode, c='k', fontsize=18)
                    except:
                        print('prob', mode)

            fig.tight_layout()
            fig.savefig(f"{save_to}/plot_{fill_nb}_0-25Hz.png")
            plt.close("all")

tree_maker.tag_json.tag_it(config['log_file'], 'completed')

