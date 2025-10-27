#!/bin/bash

#PBS -P y99
#PBS -N OM2-025_prepro
#PBS -l ncpus=48
#PBS -l mem=190GB
#PBS -l jobfs=4GB
#PBS -l walltime=12:00:00
#PBS -l storage=gdata/xv83+gdata/oi10+gdata/dk92+gdata/hh5+gdata/rr3+gdata/al33+gdata/fs38+gdata/xp65+gdata/p73+gdata/cj50+gdata/ik11
#PBS -l wd
#PBS -o output/PBS/
#PBS -j oe

echo "Going into TMIP notebooks directory"
cd ~/Projects/TMIP/notebooks

echo "Loading conda/analysis3 module"
module use /g/data/xp65/public/modules
module load conda/analysis3
conda activate conda/analysis3
conda info

echo "Loading python3/3.12.1"
module load python3/3.12.1

echo "Running transport-state script"
python scripts/average_ACCESS-OM2_025deg_jra55_iaf_omip2_cycle6.py \
&> output/average_ACCESS-OM2_025deg_jra55_iaf_omip2_cycle6.$PBS_JOBID.out


