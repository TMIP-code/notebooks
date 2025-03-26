# qsub -I -q express -P xv83 -l ncpus=48 -l mem=190GB -l jobfs=4GB -l walltime=0:20:00 -l storage=gdata/xv83+gdata/dk92+gdata/hh5+gdata/xp65+gdata/p73

# import sys to access script arguments (experiment, ensemble, first_year, last_year)
import sys

# These are fixed (correspond to my AA simulation)
model='ACCESS-ESM1-5'
experiment='historical'
year_start=1850
num_years=10
lumpby="month"

# 1. Load packages

# Import os for makedirs/isfile/environ
import os
os.environ["PYTHONWARNINGS"] = "ignore"

# Load dask
from dask.distributed import Client

# Load xarray for N-dimensional arrays
import xarray as xr

# Load datetime to deal with time formats
import datetime

# Load traceback to print exceptions
import traceback

# Import numpy
import numpy as np

# Load pandas for data manipulation
import pandas as pd

# 2. Define some functions
# (to avoid too much boilerplate code)
print("Defining functions")

def open_my_dataset(paths):
    ds = xr.open_mfdataset(
        paths,
        chunks={'time':-1, 'st_ocean':-1},
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
    return ds

def season_climatology(ds):
    # Make a DataArray with the number of days in each month, size = len(time)
    month_length = ds.time.dt.days_in_month
    # Calculate the weights by grouping by 'time.season'
    weights = month_length.groupby("time.season") / month_length.groupby("time.season").sum()
    # Test that the sum of the weights for each season is 1.0
    np.testing.assert_allclose(weights.groupby("time.season").sum().values, np.ones(4))
    # Calculate the weighted average
    return (ds * weights).groupby("time.season").sum(dim="time")

def month_climatology(ds):
    # Make a DataArray with the number of days in each month, size = len(time)
    month_length = ds.time.dt.days_in_month
    # Calculate the weights by grouping by 'time.season'
    weights = month_length.groupby("time.month") / month_length.groupby("time.month").sum()
    # Test that the sum of the weights for each month is 1.0
    np.testing.assert_allclose(weights.groupby("time.month").sum().values, np.ones(12))
    # Calculate the weighted average
    ds_out = (ds * weights).groupby("time.month").sum(dim="time")
    # Keep track of mean number of days per month
    mean_days_in_month = month_length.groupby("time.month").mean()
    # And assign it to new coordinate
    ds_out = ds_out.assign_coords(mean_days_in_month=('month', mean_days_in_month.data))
    return ds_out

def climatology(ds, lumpby):
    if lumpby == "month":
        return month_climatology(ds)
    elif lumpby == "season":
        return season_climatology(ds)
    else:
        raise ValueError(f"lumpby has to be month or season")

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

# Create directory on scratch to save the data
scratchdatadir = '/scratch/xv83/TMIP/data'
AAdatadir = '/scratch/xv83/bp3051/access-esm/archive/andersonacceleration_test-n10-5415f621/'
gdatadatadir = '/g/data/xv83/TMIP/data'

# Depends on time window
start_time, end_time = time_window_strings(year_start, num_years)
start_time_str = f'Jan{start_time}'
end_time_str = f'Dec{end_time}'

# Simulation years from AA output
simyears = range(10) # AA cycles of 10 years

# 4. Load data, preprocess it, and save it to NetCDF

print("Starting client")

# This `if` statement is required in scripts (not required in Jupyter)
if __name__ == '__main__':
    client = Client(n_workers=44, threads_per_worker=1)#, memory_limit='16GB') # Note: with 1thread/worker cannot plot thetao. Maybe I need to understand why?

    # print ensemble/member
    print(f"\nProcessing AA output")

    # directory to save the data to (as NetCDF)
    outputdir = f'{scratchdatadir}/{model}/{experiment}/AA/{start_time_str}-{end_time_str}/cyclo{lumpby}'
    print("Creating directory: ", outputdir)
    os.makedirs(outputdir, exist_ok=True)
    print("  averaging data from: ", AAdatadir)
    print("  to be saved in: ", outputdir)

    # # umo / tx_trans
    # paths = [f'{AAdatadir}/output00{simyear}/ocean/ocean-3d-tx_trans-1monthly-mean-ym_185{simyear}_01.nc' for simyear in simyears]
    # try:
    #     print("    Loading tx_trans")
    #     tx_trans_ds = open_my_dataset(paths)
    #     print("      selecting time window")
    #     tx_trans_ds_sel = tx_trans_ds.sel(time=slice(start_time, end_time))
    #     print(f"Averaging tx_trans over each {lumpby}")
    #     tx_trans = climatology(tx_trans_ds_sel["tx_trans"], lumpby)
    #     print("\ntx_trans: ", tx_trans)
    #     print("Saving tx_trans to: ", f'{outputdir}/umo.nc')
    #     tx_trans.to_dataset(name='umo').to_netcdf(f'{outputdir}/umo.nc', compute=True)
    # except Exception:
    #     print(f'Error processing AA tx_trans')
    #     print(traceback.format_exc())
    # # vmo / ty_trans
    # paths = [f'{AAdatadir}/output00{simyear}/ocean/ocean-3d-ty_trans-1monthly-mean-ym_185{simyear}_01.nc' for simyear in simyears]
    # try:
    #     print("    Loading ty_trans")
    #     ty_trans_ds = open_my_dataset(paths)
    #     print("      selecting time window")
    #     ty_trans_ds_sel = ty_trans_ds.sel(time=slice(start_time, end_time))
    #     print(f"Averaging ty_trans over each {lumpby}")
    #     ty_trans = climatology(ty_trans_ds_sel["ty_trans"], lumpby)
    #     print("\nty_trans: ", ty_trans)
    #     print("Saving ty_trans to: ", f'{outputdir}/vmo.nc')
    #     ty_trans.to_dataset(name='vmo').to_netcdf(f'{outputdir}/vmo.nc', compute=True)
    # except Exception:
    #     print(f'Error processing AA ty_trans')
    #     print(traceback.format_exc())

    # # tx_trans_gm
    # paths = [f'{AAdatadir}/output00{simyear}/ocean/ocean-3d-tx_trans_gm-1monthly-mean-ym_185{simyear}_01.nc' for simyear in simyears]
    # try:
    #     print("    Loading tx_trans_gm")
    #     tx_trans_gm_ds = open_my_dataset(paths)
    #     print("      selecting time window")
    #     tx_trans_gm_ds_sel = tx_trans_gm_ds.sel(time=slice(start_time, end_time))
    #     print(f"Averaging tx_trans_gm over each {lumpby}")
    #     tx_trans_gm = climatology(tx_trans_gm_ds_sel["tx_trans_gm"], lumpby)
    #     print("\ntx_trans_gm: ", tx_trans_gm)
    #     print("Saving tx_trans_gm to: ", f'{outputdir}/tx_trans_gm.nc')
    #     tx_trans_gm.to_dataset(name='tx_trans_gm').to_netcdf(f'{outputdir}/tx_trans_gm.nc', compute=True)
    # except Exception:
    #     print(f'Error processing AA tx_trans_gm')
    #     print(traceback.format_exc())
    # # ty_trans_gm
    # paths = [f'{AAdatadir}/output00{simyear}/ocean/ocean-3d-ty_trans_gm-1monthly-mean-ym_185{simyear}_01.nc' for simyear in simyears]
    # try:
    #     print("    Loading ty_trans_gm")
    #     ty_trans_gm_ds = open_my_dataset(paths)
    #     print("      selecting time window")
    #     ty_trans_gm_ds_sel = ty_trans_gm_ds.sel(time=slice(start_time, end_time))
    #     print(f"Averaging ty_trans_gm over each {lumpby}")
    #     ty_trans_gm = climatology(ty_trans_gm_ds_sel["ty_trans_gm"], lumpby)
    #     print("\nty_trans_gm: ", ty_trans_gm)
    #     print("Saving ty_trans_gm to: ", f'{outputdir}/ty_trans_gm.nc')
    #     ty_trans_gm.to_dataset(name='ty_trans_gm').to_netcdf(f'{outputdir}/ty_trans_gm.nc', compute=True)
    # except Exception:
    #     print(f'Error processing AA ty_trans_gm')
    #     print(traceback.format_exc())

    # # tx_trans_submeso
    # paths = [f'{AAdatadir}/output00{simyear}/ocean/ocean-3d-tx_trans_submeso-1monthly-mean-ym_185{simyear}_01.nc' for simyear in simyears]
    # try:
    #     print("    Loading tx_trans_submeso")
    #     tx_trans_submeso_ds = open_my_dataset(paths)
    #     print("      selecting time window")
    #     tx_trans_submeso_ds_sel = tx_trans_submeso_ds.sel(time=slice(start_time, end_time))
    #     print(f"Averaging tx_trans_submeso over each {lumpby}")
    #     tx_trans_submeso = climatology(tx_trans_submeso_ds_sel["tx_trans_submeso"], lumpby)
    #     print("\ntx_trans_submeso: ", tx_trans_submeso)
    #     print("Saving tx_trans_submeso to: ", f'{outputdir}/tx_trans_submeso.nc')
    #     tx_trans_submeso.to_dataset(name='tx_trans_submeso').to_netcdf(f'{outputdir}/tx_trans_submeso.nc', compute=True)
    # except Exception:
    #     print(f'Error processing AA tx_trans_submeso')
    #     print(traceback.format_exc())
    # # ty_trans_submeso
    # paths = [f'{AAdatadir}/output00{simyear}/ocean/ocean-3d-ty_trans_submeso-1monthly-mean-ym_185{simyear}_01.nc' for simyear in simyears]
    # try:
    #     print("    Loading ty_trans_submeso")
    #     ty_trans_submeso_ds = open_my_dataset(paths)
    #     print("      selecting time window")
    #     ty_trans_submeso_ds_sel = ty_trans_submeso_ds.sel(time=slice(start_time, end_time))
    #     print(f"Averaging ty_trans_submeso over each {lumpby}")
    #     ty_trans_submeso = climatology(ty_trans_submeso_ds_sel["ty_trans_submeso"], lumpby)
    #     print("\nty_trans_submeso: ", ty_trans_submeso)
    #     print("Saving ty_trans_submeso to: ", f'{outputdir}/ty_trans_submeso.nc')
    #     ty_trans_submeso.to_dataset(name='ty_trans_submeso').to_netcdf(f'{outputdir}/ty_trans_submeso.nc', compute=True)
    # except Exception:
    #     print(f'Error processing AA ty_trans_submeso')
    #     print(traceback.format_exc())

    # mlotst dataset
    paths = [f'{AAdatadir}/output00{simyear}/ocean/ocean-2d-mld-1monthly-mean-ym_185{simyear}_01.nc' for simyear in simyears]
    try:
        print("Loading mlotst data")
        mlotst_ds = open_my_dataset(paths)
        print("\nmlotst_ds: ", mlotst_ds)
        print("Slicing mlotst for the time period")
        mlotst_ds_sel = mlotst_ds.sel(time=slice(start_time, end_time))
        print(f"Averaging mlotst over each {lumpby}")
        mlotst = climatology(mlotst_ds_sel["mld"], lumpby)
        print("\nmlotst: ", mlotst)
        print("Saving mlotst to: ", f'{outputdir}/mlotst.nc')
        mlotst.to_dataset(name='mlotst').to_netcdf(f'{outputdir}/mlotst.nc', compute=True)
    except Exception:
        print(f'Error processing {model} {member} mlotst')
        print(traceback.format_exc())

    client.close()




