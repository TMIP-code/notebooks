#!/bin/bash

#PBS -P y99
#PBS -N MOC_OM2-025
#PBS -l ncpus=48
#PBS -l mem=190GB
#PBS -l jobfs=4GB
#PBS -l walltime=01:00:00
#PBS -l storage=gdata/xv83+gdata/oi10+gdata/dk92+gdata/hh5+gdata/rr3+gdata/al33+gdata/fs38+gdata/xp65+gdata/p73+gdata/cj50+gdata/ik11
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
python3 scripts/MOC_ACCESS-OM2_025deg_jra55_iaf_omip2_cycle6.py \
&> output/MOC_ACCESS-OM2_025deg_jra55_iaf_omip2_cycle6.$PBS_JOBID.out

