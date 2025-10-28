#!/bin/bash

#PBS -P y99
#PBS -N OM2_raw_preprocessing
#PBS -l ncpus=48
#PBS -l mem=190GB
#PBS -l jobfs=4GB
#PBS -l walltime=12:00:00
#PBS -l storage=gdata/xv83+gdata/oi10+gdata/dk92+gdata/hh5+gdata/rr3+gdata/al33+gdata/fs38+gdata/xp65+gdata/p73+gdata/cj50
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
# model=ACCESS-ESM1-5
# model=ACCESS-CM2
# model=ACCESS-OM2
# model=ACCESS-OM2-025
# model=ACCESS-OM2-01
# subcatalog=01deg_jra55v13_ryf9091_qian_wthmp
# year_start=2150 # for this experiment this is the start of the last decade
# num_years=10

echo "Running transport-state script"
python3 scripts/average_ACCESS-OM2_01deg_jra55v13_ryf9091_qian.py \
&> output/average_ACCESS-OM2_01deg_jra55v13_ryf9091_qian.$PBS_JOBID.out


