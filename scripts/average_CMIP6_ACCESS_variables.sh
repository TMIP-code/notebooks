#!/bin/bash

#PBS -P xv83
#PBS -N CMIP6_ACCESS_preprocessing
#PBS -l ncpus=28
#PBS -l mem=180GB
#PBS -l jobfs=4GB
#PBS -l walltime=12:00:00
#PBS -l storage=gdata/xv83+gdata/oi10+gdata/dk92+gdata/hh5+gdata/rr3+gdata/al33+gdata/fs38+gdata/xp65+gdata/p73
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

# CHANGE HERE the model, experiment, ensemble, etc.
model=ACCESS-ESM1-5
# model=ACCESS-CM2
experiment=historical
ensemble=r1i1p1f1 # <- note that this is not used in the script
year_start=1850
num_years=10

echo "Running transport-state script"
python scripts/average_CMIP6_ACCESS_variables.py $model $experiment $ensemble $year_start $num_years \
&> output/$model.$experiment.allensembles.$year_start.$num_years.$PBS_JOBID.out


