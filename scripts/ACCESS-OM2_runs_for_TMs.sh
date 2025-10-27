#!/bin/bash

#PBS -P y99
#PBS -q express
#PBS -N OM2_runs_for_TMs
#PBS -l ncpus=4
#PBS -l mem=19GB
#PBS -l jobfs=4GB
#PBS -l walltime=00:05:00
#PBS -l storage=gdata/xp65
#PBS -l wd
#PBS -o output/PBS/
#PBS -j oe


echo "Loading conda/analysis3 module"
module use /g/data/xp65/public/modules
module load conda/analysis3-25.07
conda activate conda/analysis3-25.07
conda info

echo "Loading python3/3.12.1"
module load python3/3.12.1

echo "Running catalog filtering script"
python scripts/ACCESS-OM2_runs_for_TMs.py &> output/ACCESS-OM2_runs_for_TMs.$PBS_JOBID.out


