
# import sys to access script arguments (experiment, ensemble, first_year, last_year)
import sys

# interactive use only
model = "ACCESS-OM2-025"
subcatalog = "025deg_jra55_iaf_omip2_cycle6"

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

# Import numpy
import numpy as np

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

def yearlymeans(ds):
    # Make a DataArray with the number of days in each month, size = len(time)
    month_length = ds.time.dt.days_in_month
    # Calculate the weights by grouping by 'time.year'
    weights = month_length.groupby("time.year") / month_length.groupby("time.year").sum()
    # Test that the sum of the weights for each year is 1.0
    np.testing.assert_allclose(weights.groupby("time.year").sum().values, 1.0)
    # Calculate the weighted average
    return (ds * weights).groupby("time.year").sum(dim="time")

# 3. Load catalog

catalogs = intake.cat.access_nri
print(catalogs)
print(catalogs.keys())
cat = catalogs[subcatalog]
print(cat)

# Only keep the required data
searched_cat = cat.search(variable = ["ty_trans_rho", "ty_trans_rho_gm"])
print(searched_cat)


# Create directory on scratch to save the data
datadir = f'/scratch/{PROJECT}/TMIP/data'




# 4. Load data, preprocess it, and save it to NetCDF

print("Starting client")

# This `if` statement is required in scripts (not required in Jupyter)
if __name__ == '__main__':
    client = Client(n_workers=48, threads_per_worker=1) #, memory_limit='16GB') # Note: with 1thread/worker cannot plot thetao. Maybe I need to understand why?


    # directory to save the data to (as NetCDF)
    outputdir = f'{datadir}/{model}/{subcatalog}'
    print("Creating directory: ", outputdir)
    makedirs(outputdir, exist_ok=True)


    # ty_trans_rho
    try:
        print("Loading ty_trans_rho data")
        ty_trans_rho_datadask = select_data(searched_cat,
            dict(
                # float ty_trans_rho(time, potrho, grid_yu_ocean, grid_xt_ocean) ;
                # ty_trans_rho:_ChunkSizes = 1, 40, 108, 120 ;
                chunks={'time':-1, 'potrho':40, 'grid_xt_ocean':120, 'grid_yu_ocean':108}
            ),
            variable = "ty_trans_rho",
            frequency = "1mon",
        )
        print("\nty_trans_rho_datadask: ", ty_trans_rho_datadask)
        print("Sum longitudinally and cumsum vertically")
        psi = ty_trans_rho_datadask.sum("grid_xt_ocean")
        psi = psi.cumsum('potrho') - psi.sum('potrho')
        print("\npsi: ", psi)
        print("Saving psi to: ", f'{outputdir}/psi.nc')
        psi.to_netcdf(f'{outputdir}/psi.nc', compute=True)
    except Exception:
        print(f'Error processing {model} ty_trans_rho')
        print(traceback.format_exc())


    # ty_trans_rho_gm_gm
    try:
        print("Loading ty_trans_rho_gm data")
        ty_trans_rho_gm_datadask = select_data(searched_cat,
            dict(
                # float ty_trans_rho(time, potrho, grid_yu_ocean, grid_xt_ocean) ;
                # ty_trans_rho:_ChunkSizes = 1, 40, 108, 120 ;
                chunks={'time':-1, 'potrho':40, 'grid_xt_ocean':120, 'grid_yu_ocean':108}
            ),
            variable = "ty_trans_rho_gm",
            frequency = "1mon",
        )
        print("\nty_trans_rho_gm_datadask: ", ty_trans_rho_gm_datadask)
        print("Sum longitudinally")
        psi_gm = ty_trans_rho_gm_datadask.sum("grid_xt_ocean")
        print("\npsi_gm: ", psi_gm)
        print("Saving psi_gm to: ", f'{outputdir}/psi_gm.nc')
        psi_gm.to_netcdf(f'{outputdir}/psi_gm.nc', compute=True)
    except Exception:
        print(f'Error processing {model} ty_trans_rho_gm')
        print(traceback.format_exc())

    try:
        print("Calculating total overturning streamfunction")
        psi_tot = (psi.ty_trans_rho + psi_gm.ty_trans_rho_gm).to_dataset(name='psi_tot')
        print("\npsi_tot: ", psi_tot)
        psi_tot.to_netcdf(f'{outputdir}/psi_tot.nc', compute=True)
    except Exception:
        print(f'Error processing {model} psi_tot')
        print(traceback.format_exc())

    try:
        print("Yearly means")
        psi_tot_year = yearlymeans(psi_tot)
        print("\npsi_tot_year: ", psi_tot_year)
        psi_tot_year.to_netcdf(f'{outputdir}/psi_tot_year.nc', compute=True)
    except Exception:
        print(f'Error processing {model} psi_tot_year')
        print(traceback.format_exc())

    try:
        print("Averaging total overturning streamfunction")
        psi_tot_avg = psi_tot.weighted(psi_tot.time.dt.days_in_month).mean(dim="time")
        print("\npsi_tot_avg: ", psi_tot_avg)
        psi_tot_avg.to_netcdf(f'{outputdir}/psi_tot_avg.nc', compute=True)
    except Exception:
        print(f'Error processing {model} psi_tot_avg')
        print(traceback.format_exc())

    try:
        print("Loading psi_tot into memory for rolling averages")
        psi_tot = psi_tot.load()
    except Exception:
        print(f'Error loading {model} psi_tot into memory')
        print(traceback.format_exc())

    try:
        print("Calculating rolling yearly average of total overturning streamfunction")
        window_size = 12  # 12 months for yearly rolling average
        psi_tot_rolling = psi_tot.rolling(time=window_size).construct("window")
        print("\npsi_tot_rolling: ", psi_tot_rolling)
        month_weights = psi_tot.time.dt.days_in_month
        print("\nmonth_weights: ", month_weights)
        month_weights_rolling = month_weights.rolling(time=window_size).construct("window")
        print("\nmonth_weights_rolling: ", month_weights_rolling)
        psi_tot_rolling_weighted = psi_tot_rolling.weighted(month_weights_rolling.fillna(0))
        print("\npsi_tot_rolling_weighted: ", psi_tot_rolling_weighted)
        psi_tot_rollingyear = psi_tot_rolling_weighted.mean("window", skipna=False)
        print("\npsi_tot_rollingyear: ", psi_tot_rollingyear)
        psi_tot_rollingyear.to_netcdf(f'{outputdir}/psi_tot_rollingyear.nc', compute=True)
    except Exception:
        print(f'Error processing {model} psi_tot_rollingyear')
        print(traceback.format_exc())

    try:
        print("Calculating rolling decadal average of total overturning streamfunction")
        window_size = 120  # 120 months for 10-year rolling average
        psi_tot_rolling = psi_tot.rolling(time=window_size).construct("window")
        print("\npsi_tot_rolling: ", psi_tot_rolling)
        month_weights = psi_tot.time.dt.days_in_month
        print("\nmonth_weights: ", month_weights)
        month_weights_rolling = month_weights.rolling(time=window_size).construct("window")
        print("\nmonth_weights_rolling: ", month_weights_rolling)
        psi_tot_rolling_weighted = psi_tot_rolling.weighted(month_weights_rolling.fillna(0))
        print("\npsi_tot_rolling_weighted: ", psi_tot_rolling_weighted)
        psi_tot_rollingdecade = psi_tot_rolling_weighted.mean("window", skipna=False)
        print("\npsi_tot_rollingdecade: ", psi_tot_rollingdecade)
        psi_tot_rollingdecade.to_netcdf(f'{outputdir}/psi_tot_rollingdecade.nc', compute=True)
    except Exception:
        print(f'Error processing {model} psi_tot_rollingdecade')
        print(traceback.format_exc())


    client.close()




