#!/bin/bash
source /afs/cern.ch/work/a/aradosla/private/miniforge3/bin/activate
cp -rf /afs/cern.ch/user/a/aradosla/ADTObsBox_analysis/002_submit_analysis_htcondor/Fill8469/007/* .
ls
pwd
mkdir ADTObsBox_data
sshfs aradosla@lxplus.cern.ch:/eos/user/s/skostogl/data_Fill8469 ADTObsBox_data -o IdentityFile=/afs/cern.ch/user/a/aradosla/.ssh/id_rsa
python run_analysis.py > output_ht.txt 2> error_ht.txt
cp -rf log* /afs/cern.ch/user/a/aradosla/ADTObsBox_analysis/002_submit_analysis_htcondor/Fill8469/007
