#!/bin/bash

#PBS -P xv83
#PBS -N archive_GM_check
#PBS -l ncpus=28
#PBS -l mem=180GB
#PBS -l jobfs=4GB
#PBS -l walltime=12:00:00
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
year_start=1850
# year_start=1990
year_end=2015
# year_end=2000
members=("HI-05","HI-07","HI-08","HI-09","HI-10","HI-11","HI-12")
# members=("HI-07")


echo "Running transport-state script"
python scripts/check_archive_unarchived_CMIP6_ACCESS_GM_files.py $model $experiment $members $year_start $year_end \
&> output/$PBS_JOBID.$model.monthly.checkarchiveddatavsTilos.out


