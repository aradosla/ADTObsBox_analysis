# %%
import pandas as pd
import tree_maker
from tree_maker import NodeJob
from tree_maker import initialize
import time
import os
from pathlib import Path
import itertools
import numpy as np
import yaml
from user_defined_functions import generate_run_sh
from user_defined_functions import generate_run_sh_htc
from pathlib import Path

config=yaml.safe_load(open('config.yaml'))



bunches_used = pd.read_parquet("bunches_used.parquet", engine = "pyarrow")
bunches_used = bunches_used['Equally spaces'].to_list()
print(bunches_used)
#bunch_nb_b1 = bunches_used
bunch_nb_b1 = [12, 1971]
#quit()
#bunch_nb_b1 =['all']
#bunch_tot_num = len(bunch_nb_b1)

#bunch_nb_b1 = [1184, 2368, 3353] #['all']
#bunch_nb_b1 = [712, 1421, 2131, 2842, 3353]
#bunch_nb_b1 = [355, 712, 1066, 1421, 1776, 2131, 2486, 2842, 3197, 3353]
#bunch_nb_b1 = [12, 178, 355, 533, 712, 888, 1066, 1242, 1421, 1596, 1776, 1954, 2131, 2309, 2486, 2787, 3019, 3197, 3353]
bunch_nb_b2 = ['all']
bunch_tot_num = len(bunch_nb_b1)



path = 'ADTObsBox_data'
fillnb = 8469
study_name = f"Fill{fillnb}_{bunch_tot_num}"
#study_name = f"Fill{fillnb}_all"

save_to =  f"/eos/user/a/aradosla/FFTs/{study_name}" #"/eos/user/s/skostogl/analysis_test"
#save_to = f"/afs/cern.ch/work/a/aradosla/private/{study_name}"
Path(save_to).mkdir(parents=True, exist_ok=True)

mypython = "/afs/cern.ch/work/a/aradosla/private/miniforge3/bin/activate"

eos_path = "/eos/project/l/lhc-lumimod/MD7003/ADTObsBox/data_Fill8469"

filenames = pd.read_parquet(f"filenames_Fill{fillnb}.parquet").filenames.values
filenames = np.array([f"{eos_path}/" + i.split("/")[-1] for i in filenames])

n = 2
filenames_in_chunks = [filenames[i * n:(i + 1) * n] for i in range((len(filenames) + n - 1) // n )]

make_path = 'ADTObsBox_data'

#mysymbolic_link = f"sshfs aradosla@lxplus.cern.ch:/eos/project/l/lhc-lumimod/MD7003/ADTObsBox/data_Fill8469 {make_path} -o IdentityFile=/afs/cern.ch/user/a/aradosla/.ssh/id_rsa"
#mysymbolic_link = f"ln -s /eos/project/l/lhc-lumimod/MD7003/ADTObsBox/data_Fill8469/* {make_path}"
#mysymbolic_link = f"sshfs aradosla@lxplus.cern.ch:/eos/user/a/aradosla/ADTObsBox_data_2022 {path} -o IdentityFile=/afs/cern.ch/user/a/aradosla/.ssh/id_rsa"
#mysymbolic_link = f"ln -s /eos/user/a/aradosla/ADTObsBox_data_2022/* {path}"

#print(filenames_in_chunks)
#quit()

children={}
for child in range(len(filenames_in_chunks[:])):
    children[f"{study_name}/{child:03}"] = {
                                    'adt_file':filenames_in_chunks[child].tolist(),
                                    'save_to': f"{save_to}/results_fft_{study_name}_{child:03}.parquet",
                                    'bunch_nb_b1':bunch_nb_b1,
                                    'bunch_nb_b2':bunch_nb_b2,
                                    'log_file': f"{os.getcwd()}/{study_name}/{child:03}/tree_maker.log"
                                    }

config['root']['children'] = children
config['root']['setup_env_script'] = mypython
#config['root']['symbolic_link'] = mysymbolic_link
config['root']['make_path'] = make_path

# Create tree object
start_time = time.time()
root = initialize(config)
print('Done with the tree creation.')
print("--- %s seconds ---" % (time.time() - start_time))

# From python objects we move the nodes to the file-system.
start_time = time.time()
#root.make_folders(generate_run_sh)
root.make_folders(generate_run_sh_htc)
print('The tree folders are ready.')
print("--- %s seconds ---" % (time.time() - start_time))

import shutil
shutil.move("tree_maker.json", f"tree_maker_{study_name}.json")
shutil.move("tree_maker.log", f"tree_maker_{study_name}.log")
