#!/bin/bash

#PBS -P xv83
#PBS -N CMIP5_ACCESS_preprocessing
#PBS -l ncpus=28
#PBS -l mem=180GB
#PBS -l jobfs=4GB
#PBS -l walltime=3:00:00
#PBS -l storage=gdata/xv83+gdata/dk92+gdata/rr3+gdata/hh5+gdata/xp65
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

# CHANGE HERE the model, experiment, ensemble, etc.
model=ACCESS1-3
experiment=historical
ensemble=r1i1p1 # <- note that this is not used in the script
year_start=1990
num_years=10
# lumpby=month
lumpby=season

echo "Running transport-state script"
python3 scripts/cyclo_average_CMIP5_ACCESS_variables.py $model $experiment $ensemble $year_start $num_years $lumpby \
&> output/cyclo.$lumpby.$experiment.$model.allensembles.$year_start.$num_years.$PBS_JOBID.out


