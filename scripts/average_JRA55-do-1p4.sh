#!/bin/bash

#PBS -P y99
#PBS -N JRA55-do-1p4
#PBS -l ncpus=48
#PBS -l mem=190GB
#PBS -l jobfs=4GB
#PBS -l walltime=12:00:00
#PBS -l storage=gdata/xp65+gdata/p73+gdata/cj50+gdata/ik11+gdata/qv56
#PBS -l wd
#PBS -o output/PBS/
#PBS -j oe

echo "Going into TMIP notebooks directory"
cd ~/Projects/TMIP/notebooks

echo "Loading conda/analysis3 module"
module purge
module use /g/data/xp65/public/modules
module load conda/analysis3

echo "Running transport-state script"
python3 scripts/average_JRA55-do-1p4.py \
&> output/average_JRA55-do-1p4.$PBS_JOBID.out


