#!/bin/bash

#PBS -P xv83
#PBS -N archive_GM_check
#PBS -l ncpus=48
#PBS -l mem=180GB
#PBS -l jobfs=4GB
#PBS -l walltime=24:00:00
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

# CHANGE HERE the model, experiment, members, decades.
model=ACCESS-ESM1-5
# model=ACCESS-CM2

# experiment=historical
# # members=("HI-05","HI-06","HI-07","HI-08","HI-09","HI-10","HI-11","HI-12","HI-13","HI-14")
# # members=("HI-15","HI-16","HI-17","HI-18","HI-19","HI-20","HI-21","HI-22","HI-23","HI-24")
# # members=("HI-25","HI-26","HI-27","HI-28","HI-29","HI-30","HI-31","HI-32","HI-33","HI-34")
# members=("HI-35","HI-36","HI-37","HI-38","HI-39","HI-40","HI-41","HI-42","HI-43","HI-44")
# decade_start=1850
# decade_end=2020
experiment=ssp370
# members=("SSP-370-05","SSP-370-06","SSP-370-07","SSP-370-08","SSP-370-09","SSP-370-10","SSP-370-11","SSP-370-12","SSP-370-13","SSP-370-14")
# members=("SSP-370-15","SSP-370-16","SSP-370-17","SSP-370-18","SSP-370-19","SSP-370-20","SSP-370-21","SSP-370-22","SSP-370-23","SSP-370-24")
# members=("SSP-370-25","SSP-370-26","SSP-370-27","SSP-370-28","SSP-370-29","SSP-370-30","SSP-370-31","SSP-370-32","SSP-370-33","SSP-370-34")
members=("SSP-370-35","SSP-370-36","SSP-370-37","SSP-370-38","SSP-370-39","SSP-370-40","SSP-370-41","SSP-370-42","SSP-370-43","SSP-370-44")
decade_start=2010
decade_end=2100


echo "Running transport-state script"
python scripts/check_archive_unarchived_CMIP6_ACCESS_GM_files.py $model $experiment $members $decade_start $decade_end \
&> output/check_archive_unarchived_CMIP6_ACCESS_GM_files.$PBS_JOBID.out


