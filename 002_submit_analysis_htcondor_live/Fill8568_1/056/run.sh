#!/bin/bash
source /afs/cern.ch/work/a/aradosla/private/miniforge3/bin/activate
cp -rf /afs/cern.ch/work/a/aradosla/private/ADTObsBox_analysis/002_submit_analysis_htcondor_live/Fill8568_1/056/* .
ls
pwd
mkdir ADTObsBox_data
sshfs aradosla@cs-ccr-dev1.cern.ch:/nfs/cfc-sr4-adtobs2buf/obsbox/slow ADTObsBox_data -o IdentityFile=/afs/cern.ch/user/a/aradosla/.ssh/id_rsa
python run_analysis.py > output_ht.txt 2> error_ht.txt
cp -rf *ht.txt log* /afs/cern.ch/work/a/aradosla/private/ADTObsBox_analysis/002_submit_analysis_htcondor_live/Fill8568_1/056
