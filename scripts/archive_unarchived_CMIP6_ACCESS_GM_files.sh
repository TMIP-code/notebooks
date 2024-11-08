#!/bin/bash

#PBS -P xv83
#PBS -N archive_GM
#PBS -l ncpus=28
#PBS -l mem=180GB
#PBS -l jobfs=4GB
#PBS -l walltime=1:00:00
#PBS -l storage=gdata/xv83+gdata/dk92+gdata/hh5+gdata/xp65+gdata/p73+scratch/p66
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
# year_start=1850
year_start=1890
# year_end=2015
year_end=1900
# members=("HI-05","HI-06")
members=("HI-07")


echo "Running transport-state script"
python scripts/archive_unarchived_CMIP6_ACCESS_GM_files.py $model $experiment $members $year_start $year_end \
&> output/$PBS_JOBID.$model.monthly.datafromTilo.out


