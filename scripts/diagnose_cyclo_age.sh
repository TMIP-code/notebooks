#!/bin/bash

#PBS -P xv83
#PBS -N diagnose_age
#PBS -l ncpus=48
#PBS -l mem=180GB
#PBS -l jobfs=4GB
#PBS -l walltime=2:00:00
#PBS -l storage=gdata/xv83+scratch/xv83+gdata/dk92+gdata/hh5+gdata/xp65+gdata/p73+scratch/p66
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
year_start=1990
num_years=10
lumpby=month
# lumpby=season


echo "Running transport-state script"
python scripts/diagnose_cyclo_age.py $model $experiment $year_start $num_years $lumpby \
&> output/cyclo.diagnose_cyclo_age.$PBS_JOBID.$model.$experiment.$year_start.$num_years.out


