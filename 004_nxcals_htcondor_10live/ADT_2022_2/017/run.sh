#!/bin/bash
source /afs/cern.ch/work/a/aradosla/private/miniforge3/bin/activate
cp -rf /afs/cern.ch/work/a/aradosla/private/ADTObsBox_analysis/004_nxcals_htcondor_10live/ADT_2022_2/017/* .
ls
pwd
python run_analysis.py > output_ht.txt 2> error_ht.txt
cp -rf output* log* /afs/cern.ch/work/a/aradosla/private/ADTObsBox_analysis/004_nxcals_htcondor_10live/ADT_2022_2/017
