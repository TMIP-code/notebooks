#!/bin/bash

#PBS -P xv83
#PBS -N cycloAAavg
#PBS -q express
#PBS -l ncpus=48
#PBS -l mem=190GB
#PBS -l jobfs=4GB
#PBS -l walltime=1:00:00
#PBS -l storage=gdata/xv83+gdata/dk92+gdata/hh5+gdata/xp65+gdata/p73
#PBS -l wd
#PBS -o output/PBS/
#PBS -j oe

echo "Going into TMIP notebooks directory"
cd ~/Projects/TMIP/notebooks

echo "Loading conda/analysis3-24.04 module"
module use /g/data/hh5/public/modules
module load conda/analysis3-24.04
conda activate conda/analysis3-24.04
conda info

echo "Loading python3/3.12.1"
module load python3/3.12.1


echo "Running transport-state script"
python scripts/cyclo_average_AA_variables.py &> output/cyclo_average_AA_variables.$PBS_JOBID.out

