
# import sys to access script arguments (experiment, ensemble, first_year, last_year)
import sys

# Ignore warnings
from os import environ
environ["PYTHONWARNINGS"] = "ignore"
PROJECT = environ["PROJECT"]

# Import makedirs to create directories where I write new files
from os import makedirs, listdir

# Load dask
from dask.distributed import Client

# Load intake and cosima cookbook
import intake

# Load xarray for N-dimensional arrays
import xarray as xr

# Load datetime to deal with time formats
import datetime

# Load traceback to print exceptions
import traceback

# Load pandas for data manipulation
import pandas as pd

# 2. Define some functions
# (to avoid too much boilerplate code)
print("Defining functions")

def select_data(cat, xarray_open_kwargs, **kwargs):
    selectedcat = cat.search(**kwargs)
    print("\nselectedcat: ", selectedcat)
    xarray_combine_by_coords_kwargs=dict(
        compat="override",
        data_vars="minimal",
        coords="minimal"
    )
    datadask = selectedcat.to_dask(
        xarray_open_kwargs=xarray_open_kwargs,
        xarray_combine_by_coords_kwargs=xarray_combine_by_coords_kwargs,
        parallel=True,
    )
    return datadask

# directory to save the data to (as NetCDF)
outputdir = f'/scratch/{PROJECT}/TMIP/data/JRA55-do-1p4'
print("Creating directory: ", outputdir)
makedirs(outputdir, exist_ok=True)

inputdir = '/g/data/qv56/replicas/input4MIPs/CMIP6/OMIP/MRI/MRI-JRA55-do-1-4-0/atmos/3hrPt'
uasinputdir = f'{inputdir}/uas/gr/v20190429'
vasinputdir = f'{inputdir}/vas/gr/v20190429'
years = range(1958, 2018 + 1)


# keyword arguments to open_mfdataset
kwargs = dict(
    # chunks={'time':-1, 'st_ocean':-1},
    concat_dim="time",
    compat='override',
    preprocess=None,
    engine='netcdf4',
    data_vars='minimal',
    coords='minimal',
    combine='nested',
    parallel=True,
    join='outer',
    attrs_file=None,
    combine_attrs='override',
)

# 4. Load data, preprocess it, and save it to NetCDF

print("Starting client")

# This `if` statement is required in scripts (not required in Jupyter)
if __name__ == '__main__':
    client = Client(n_workers=48, threads_per_worker=1) #, memory_limit='16GB') # Note: with 1thread/worker cannot plot thetao. Maybe I need to understand why?

    for year in years
        print(f'Processing year: {year}')
        timestr = f'{year:04d}01010000-{year:04d}12312100'

        uaspath = f'{uasinputdir}/uas_input4MIPs_atmosphericState_OMIP_MRI-JRA55-do-1-4-0_gr_{timestr}.nc'
        vaspath = f'{vasinputdir}/vas_input4MIPs_atmosphericState_OMIP_MRI-JRA55-do-1-4-0_gr_{timestr}.nc'

    # monthly means of uas² + vas²
        try:
            print("Loading uas data")
            uas = xr.open_dataset(uaspath, **kwargs)
            print("\nuas: ", uas)
            print("Loading vas data")
            vas = xr.open_dataset(vaspath, **kwargs)
            print("\nvas: ", vas)
            print("Performing u² = uas² + vas² (lazily?)")
            u2 = uas.uas**2 + vas.vas**2 # should work (uas + vas colocated)
            print("\nu2: ", u2)
            print("Averaging u2 monthly")
            u2_month = u2.resample(time="1M").mean(keep_attrs=True)
            print("\nu2_month: ", u2_month)
            print("Saving u2 to: ", f'{outputdir}/u2_{timestr}.nc')
            u2_month.to_netcdf(f'{outputdir}/u2_{timestr}.nc', compute=True)
        except Exception:
            print(f'Error processing uas and vas data')
            print(traceback.format_exc())

    client.close()




