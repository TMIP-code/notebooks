#!/bin/bash

#PBS -N ESM15transport
#PBS -l ncpus=28
#PBS -l mem=180GB
#PBS -l jobfs=4GB
#PBS -l walltime=1:00:00
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

# CHANGE HERE: comment/uncomment triplets of (experiment, first_year, last_year)
# Note that historical and RCPs should end year 2006 and 2100, respectively
# and that RCPs should start 2006 (zero-based indexing in python)
ensemble=r1i1p1f1

echo "Running transport-state script"
python scripts/build_average_ACCESS-ESM1.5_transport_state_on_Gadi.py $ensemble \
&> output/$ensemble.$PBS_JOBID.out


