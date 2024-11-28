#!/bin/bash

#PBS -P xv83
#PBS -N archive_GM
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

experiment=historical
members=("HI-33","HI-34","HI-35","HI-36")
decade_start=1850
decade_end=2020
# experiment=ssp370
# members=("SSP-370-39","SSP-370-40","SSP-370-41","SSP-370-42","SSP-370-43","SSP-370-44")
# decade_start=2010
# decade_end=2100


echo "Running transport-state script"
python scripts/archive_unarchived_CMIP6_ACCESS_GM_files.py $model $experiment $members $decade_start $decade_end \
&> output/$PBS_JOBID.$model.monthly.datafromTilo.out


