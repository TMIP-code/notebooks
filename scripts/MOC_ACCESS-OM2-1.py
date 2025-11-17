
# import sys to access script arguments (experiment, ensemble, first_year, last_year)
import sys

# interactive use only
model = "ACCESS-OM2-1"
subcatalog = sys.argv[1]

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
                chunks={'time':-1, 'potrho':27, 'grid_xt_ocean':120, 'grid_yu_ocean':100}
            ),
            variable = "ty_trans_rho",
            frequency = "1mon",
        )
        print("\nty_trans_rho_datadask: ", ty_trans_rho_datadask)
        print("Sum longitudinally and cumsum vertically")
        psi = ty_trans_rho_datadask.sum("grid_xt_ocean")
        psi = psi.cumulative('potrho').sum() - psi.sum('potrho')
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
                chunks={'time':-1, 'potrho':27, 'grid_xt_ocean':120, 'grid_yu_ocean':100}
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

    client.close()




