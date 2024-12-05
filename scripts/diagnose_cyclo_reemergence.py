

# import sys to access script arguments (experiment, ensemble, first_year, last_year)
import sys

# For interactive use only
model = "ACCESS-ESM1-5"
experiment = "historical"
year_start = 1990
num_years = 10
lumpby = "month"


# Model etc. defined from script input
model = sys.argv[1]
print("Model: ", model, " (type: ", type(model), ")")
experiment = sys.argv[2]
print("Experiment: ", experiment, " (type: ", type(experiment), ")")
year_start = int(sys.argv[3])
num_years = int(sys.argv[4])
print("Time window: ", year_start, " to ", year_start + num_years - 1)
lumpby = sys.argv[5] # "month" or "season"
print("Lumping by", lumpby)

seasons = ("DJF", "MAM", "JJA", "SON")
months = range(1, 13) # Ugh! Zero-based indexing!

# 1. Load packages

# Import os for makedirs/isfile/environ
import os
os.environ["PYTHONWARNINGS"] = "ignore"

# Load dask
from dask.distributed import Client

# import glob for searching directories
from glob import glob

# Load xarray for N-dimensional arrays
import xarray as xr

# Load Pandas to make axis of members
import pandas as pd

# Load traceback to print exceptions
import traceback

# Import numpy
import numpy as np

# # Load xmip for preprocessing (trying to get consistent metadata for making matrices down the road)
# from xmip.preprocessing import combined_preprocessing


# 2. Define some functions
# (to avoid too much boilerplate code)
print("Defining functions")

def time_window_strings(year_start, num_years):
    """
    return strings for start_time and end_time
    """
    # start_time is first second of year_start
    start_time = f'{year_start}'
    # end_time is last second of last_year
    end_time = f'{year_start + num_years - 1}'
    # Return the weighted average
    return start_time, end_time

def CMIP6_member(member):
    return f'r{member}i1p1f1'

# Members to loop through


# Create directory on scratch to save the data
scratchdatadir = '/scratch/xv83/TMIP/data'

# Depends on time window
start_time, end_time = time_window_strings(year_start, num_years)
start_time_str = f'Jan{start_time}'
end_time_str = f'Dec{end_time}'



def inputdirfun(member):
    return f'{scratchdatadir}/{model}/{experiment}/{CMIP6_member(member)}/{start_time_str}-{end_time_str}/cyclo{lumpby}'

def inputfilepathfun(member):
    return f'{inputdirfun(member)}/reemergence_time.nc'

def isvalidmember(member):
    return os.path.isfile(inputfilepathfun(member))

members = [m for m in range(1, 41) if isvalidmember(m)]

# # For debugging only
# members = members[0:2]

paths = [inputfilepathfun(m) for m in members]
members_axis = pd.Index(members, name="member")


# function to open all files and combine them
# TODO Figure out how to do this... !@#$%^&*xarray*&^%$#@!
def open_my_dataset(paths):
    ds = xr.open_mfdataset(
        paths,
        chunks={'Ti':-1, 'lev':-1}, # TODO these dim names likely won't work for my Gammas
        concat_dim=[members_axis], # TODO these dim names likely won't work for my Gammas
        compat='override',
        preprocess=None,
        engine='netcdf4',
        # data_vars='minimal', # <- cannot have this option otherwise only one member is loaded it seems
        coords='minimal',
        combine='nested',
        parallel=True,
        join='outer',
        attrs_file=None,
        combine_attrs='override',
    )
    return ds




outputdir = f'{scratchdatadir}/{model}/{experiment}/all_members/{start_time_str}-{end_time_str}/cyclo{lumpby}'
print("Creating directory: ", outputdir)
os.makedirs(outputdir, exist_ok=True)
print("  to be saved in: ", outputdir)

print("Starting client")

# This `if` statement is required in scripts (not required in Jupyter)
if __name__ == '__main__':
    client = Client(n_workers=40, threads_per_worker=1)

    adjointage_ds = open_my_dataset(paths)
    print("\nadjointage_ds: ", adjointage_ds)
    adjointage = adjointage_ds.adjointage
    print("\nadjointage: ", adjointage)

    adjointage_timemean = adjointage.mean(dim = ["Ti"])
    print("\nadjointage_timemean: ", adjointage_timemean)
    adjointage_timemean.to_dataset(name = 'adjointage_timemean').to_netcdf(f'{outputdir}/adjointage_timemean.nc', compute = True)

    adjointage_ensemblemean = adjointage_timemean.mean(dim = ["member"])
    print("\nadjointage_ensemblemean: ", adjointage_ensemblemean)
    adjointage_ensemblemean.to_dataset(name = 'adjointage_ensemblemean').to_netcdf(f'{outputdir}/adjointage_ensemblemean.nc', compute = True)

    adjointage_ensemblestd = adjointage_timemean.std(dim = ["member"])
    print("\nadjointage_ensemblestd: ", adjointage_ensemblestd)
    adjointage_ensemblestd.to_dataset(name = 'adjointage_ensemblestd').to_netcdf(f'{outputdir}/adjointage_ensemblestd.nc', compute = True)

    adjointage_ensemblemax = adjointage_timemean.max(dim = ["member"])
    print("\nadjointage_ensemblemax: ", adjointage_ensemblemax)
    adjointage_ensemblemax.to_dataset(name = 'adjointage_ensemblemax').to_netcdf(f'{outputdir}/adjointage_ensemblemax.nc', compute = True)

    adjointage_ensemblemin = adjointage_timemean.min(dim = ["member"])
    print("\nadjointage_ensemblemin: ", adjointage_ensemblemin)
    adjointage_ensemblemin.to_dataset(name = 'adjointage_ensemblemin').to_netcdf(f'{outputdir}/adjointage_ensemblemin.nc', compute = True)



    client.close()





