#!/bin/bash

#PBS -P xv83
#PBS -N CMIP6_ACCESS_preprocessing
#PBS -l ncpus=28
#PBS -l mem=180GB
#PBS -l jobfs=4GB
#PBS -l walltime=12:00:00
#PBS -l storage=gdata/xv83+gdata/dk92+gdata/fs38+gdata/hh5
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
experiment=piControl
ensemble=r1i1p1f1 # <- note that this is not used in the script
# year_start=1990
num_years=30

echo "Running transport-state script"
python scripts/build_average_CMIP6_ACCESS_piControl_transport_state_on_Gadi.py $model $experiment $ensemble $num_years \
&> output/$model.$experiment.allensembles.last$num_years.$PBS_JOBID.out


