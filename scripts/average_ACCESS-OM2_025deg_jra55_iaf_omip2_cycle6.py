
# import sys to access script arguments (experiment, ensemble, first_year, last_year)
import sys

# interactive use only
model = "ACCESS-OM2-025"
subcatalog = "025deg_jra55_iaf_omip2_cycle6"
year_start = 1960
num_years = 20

# 1. Load packages

# Ignore warnings
from os import environ
environ["PYTHONWARNINGS"] = "ignore"
PROJECT = environ["PROJECT"]

# Import makedirs to create directories where I write new files
from os import makedirs

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

def time_window_strings(year_start, num_years):
    """
    return strings for start_time and end_time
    """
    # start_time is first second of year_start
    start_time = f'{year_start:04d}'
    # end_time is last second of last_year
    end_time = f'{year_start + num_years - 1:04d}'
    # Return the weighted average
    return start_time, end_time

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



# 3. Load catalog

catalogs = intake.cat.access_nri
print(catalogs)
print(catalogs.keys())
cat = catalogs[subcatalog]
print(cat)

# Only keep the required data
searched_cat = cat.search(variable = ["tx_trans", "ty_trans", "mld", "area_t", "dht"])
print(searched_cat)


# Create directory on scratch to save the data
datadir = f'/scratch/{PROJECT}/TMIP/data'
start_time, end_time = time_window_strings(year_start, num_years)
start_time_str = f'Jan{start_time}'
end_time_str = f'Dec{end_time}'



# 4. Load data, preprocess it, and save it to NetCDF

print("Starting client")

# This `if` statement is required in scripts (not required in Jupyter)
if __name__ == '__main__':
    client = Client(n_workers=48, threads_per_worker=1) #, memory_limit='16GB') # Note: with 1thread/worker cannot plot thetao. Maybe I need to understand why?


    # directory to save the data to (as NetCDF)
    outputdir = f'{datadir}/{model}/{subcatalog}/{start_time_str}-{end_time_str}'
    print("Creating directory: ", outputdir)
    makedirs(outputdir, exist_ok=True)

    # area_t
    try:
        print("Loading area_t data")
        area_t_datadask = select_data(searched_cat,
            dict(
                chunks={'xt_ocean':360, 'yt_ocean':300}
            ),
            variable = "area_t",
            frequency = "fx",
        )
        print("\narea_t_datadask: ", area_t_datadask)
        area_t = area_t_datadask["area_t"]
        print("\narea_t: ", area_t)
        print("Saving area_t to: ", f'{outputdir}/area_t.nc')
        area_t.to_netcdf(f'{outputdir}/area_t.nc', compute=True)
    except Exception:
        print(f'Error processing {model} area_t')
        print(traceback.format_exc())


    # tx_trans
    try:
        print("Loading tx_trans data")
        tx_trans_datadask = select_data(searched_cat,
            dict(
                chunks={'time': -1, 'xt_ocean':180, 'yt_ocean':150, 'lev':25}
            ),
            variable = "tx_trans",
            frequency = "1mon",
        )
        print("\ntx_trans_datadask: ", tx_trans_datadask)
        print("Slicing tx_trans for the time period")
        tx_trans_datadask_sel = tx_trans_datadask.sel(time=slice(start_time, end_time))
        print("Averaging tx_trans")
        tx_trans = tx_trans_datadask_sel["tx_trans"].weighted(tx_trans_datadask_sel.time.dt.days_in_month).mean(dim="time")
        print("\ntx_trans: ", tx_trans)
        print("Saving tx_trans to: ", f'{outputdir}/tx_trans.nc')
        tx_trans.to_netcdf(f'{outputdir}/tx_trans.nc', compute=True)
    except Exception:
        print(f'Error processing {model} tx_trans')
        print(traceback.format_exc())

    # ty_trans
    try:
        print("Loading ty_trans data")
        ty_trans_datadask = select_data(searched_cat,
            dict(
                chunks={'time': -1, 'xt_ocean':180, 'yt_ocean':150, 'lev':25}
            ),
            variable = "ty_trans",
            frequency = "1mon",
        )
        print("\nty_trans_datadask: ", ty_trans_datadask)
        print("Slicing ty_trans for the time period")
        ty_trans_datadask_sel = ty_trans_datadask.sel(time=slice(start_time, end_time))
        print("Averaging ty_trans")
        ty_trans = ty_trans_datadask_sel["ty_trans"].weighted(ty_trans_datadask_sel.time.dt.days_in_month).mean(dim="time")
        print("\nty_trans: ", ty_trans)
        print("Saving ty_trans to: ", f'{outputdir}/ty_trans.nc')
        ty_trans.to_netcdf(f'{outputdir}/ty_trans.nc', compute=True)
    except Exception:
        print(f'Error processing {model} ty_trans')
        print(traceback.format_exc())


    # mld dataset
    try:
        print("Loading mld data")
        mld_datadask = select_data(searched_cat,
            dict(
                chunks={'time': -1, 'xt_ocean':360, 'yt_ocean':300}
            ),
            variable = "mld",
            frequency = "1mon",
        )
        print("\nmld_datadask: ", mld_datadask)
        print("Slicing mld for the time period")
        mld_datadask_sel = mld_datadask.sel(time=slice(start_time, end_time))
        print("Averaging mld (mean of the yearly maximum of monthly data)")
        mld_yearlymax = mld_datadask_sel.groupby("time.year").max(dim="time")
        print("\nmld_yearlymax: ", mld_yearlymax)
        mld = mld_yearlymax.mean(dim="year")
        print("\nmld: ", mld)
        print("Saving mld to: ", f'{outputdir}/mld.nc')
        mld.to_netcdf(f'{outputdir}/mld.nc', compute=True)
        mld_max = mld_datadask_sel.max(dim="time")
        print("\nmld_max: ", mld_max)
        print("Saving mld_max to: ", f'{outputdir}/mld_max.nc')
        mld_max.to_netcdf(f'{outputdir}/mld_max.nc', compute=True)
    except Exception:
        print(f'Error processing {model} mld')
        print(traceback.format_exc())

    # dht
    try:
        print("Loading dht data")
        dht_datadask = select_data(searched_cat,
            dict(
                chunks={'time': -1, 'xt_ocean':180, 'yt_ocean':150, 'lev':25}
            ),
            variable = "dht",
            frequency = "1mon",
        )
        print("\ndht_datadask: ", dht_datadask)
        print("Slicing dht for the time period")
        dht_datadask_sel = dht_datadask.sel(time=slice(start_time, end_time))
        print("Averaging dht")
        dht = dht_datadask_sel["dht"].weighted(dht_datadask_sel.time.dt.days_in_month).mean(dim="time")
        print("\ndht: ", dht)
        print("Saving dht to: ", f'{outputdir}/dht.nc')
        dht.to_netcdf(f'{outputdir}/dht.nc', compute=True)
    except Exception:
        print(f'Error processing {model} dht')
        print(traceback.format_exc())



    client.close()




