#!/bin/bash
source /afs/cern.ch/work/a/aradosla/private/miniforge3/bin/activate
cp -rf /afs/cern.ch/work/a/aradosla/private/ADTObsBox_analysis/002_submit_analysis_htcondor/Fill8469_2/025/* .
ls
pwd
mkdir ADTObsBox_data
python run_analysis.py > output_ht.txt 2> error_ht.txt
cp -rf *ht.txt log* /afs/cern.ch/work/a/aradosla/private/ADTObsBox_analysis/002_submit_analysis_htcondor/Fill8469_2/025
