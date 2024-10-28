

# import sys to access script arguments (experiment, ensemble, first_year, last_year)
import sys

# interactive use only
model="ACCESS-ESM1-5"
experiment="historical"


# Model etc. defined from script input
model = sys.argv[1]
print("Model: ", model, " (type: ", type(model), ")")
experiment = sys.argv[2]
print("Experiment: ", experiment, " (type: ", type(experiment), ")")
year_start = 1850
year_end = 2015

# 1. Load packages

# import numpy
import numpy as np

# Ignore warnings
from os import environ
environ["PYTHONWARNINGS"] = "ignore"

# Import makedirs to create directories where I write new files
from os import makedirs

# Load dask
from dask.distributed import Client

# import glob for searching directories
from glob import glob

# Load xarray for N-dimensional arrays
import xarray as xr

# Load traceback to print exceptions
import traceback

# Load xmip for preprocessing (trying to get consistent metadata for making matrices down the road)
from xmip.preprocessing import combined_preprocessing


# Wrap it into a simple function
def annual_mean(varname, ds):
    # Make a DataArray with the number of days in each month, size = len(time)
    month_length = ds.time.dt.days_in_month

    # Calculate the weights by grouping by 'time.year'
    weights = month_length.groupby("time.year") / month_length.groupby("time.year").sum()

    # Test that the sum of the weights for each year is 1.0
    # np.testing.assert_allclose(weights.groupby("time.year").sum().values, np.ones(1))

    # Calculate the weighted average
    return (ds[varname] * weights).groupby("time.year").sum(dim="time")



# Create directory on scratch to save the data
datadir = '/scratch/xv83/TMIP/data'

members = ["HI-05", "HI-06", "HI-07", "HI-08"]

print("Starting client")

# This `if` statement is required in scripts (not required in Jupyter)
if __name__ == '__main__':
    client = Client(n_workers=4) #, threads_per_worker=1, memory_limit='16GB') # Note: with 1thread/worker cannot plot thetao. Maybe I need to understand why?


    for member in members:

        # print ensemble/member
        inputdir = f'/scratch/p66/pbd562/petrichor/get/{member}'
        outputdir = f'{datadir}/{model}/{member}'
        print(f"\nProcessing {inputdir}")

        # directory to save the data to (as NetCDF)
        print("Creating directory: ", outputdir)
        makedirs(outputdir, exist_ok=True)

        # subset of the files required
        # paths = [f'{inputdir}/history/ocn/ocean_month.nc-{year}1231' for year in range(year_start, year_end)]
        paths = [f'{inputdir}/history/ocn/ocean_month.nc-{year}1231' for year in range(1990, 2000)]

        # load the data
        ds = xr.open_mfdataset(
            paths,
            chunks=None,
            concat_dim=None,
            compat='no_conflicts',
            preprocess=None,
            engine='netcdf4',
            data_vars='all',
            coords='different',
            combine='by_coords',
            parallel=False,
            join='outer',
            attrs_file=None,
            combine_attrs='override',
            # compat="override",
            # data_vars="minimal",
            # coords="minimal"
            # parallel=True,
            # preprocess=combined_preprocessing,
        )



        # tx_trans
        try:
            print("Loading tx_trans and averaging tx_trans")
            tx_trans = annual_mean("tx_trans", ds)
            print("\ntx_trans: ", tx_trans)
            print("Saving tx_trans to: ", f'{outputdir}/year_tx_trans.nc')
            tx_trans.to_netcdf(f'{outputdir}/year_tx_trans.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {member} tx_trans')
            print(traceback.format_exc())

        # ty_trans
        try:
            print("Loading ty_trans and averaging ty_trans")
            ty_trans = annual_mean("ty_trans", ds)
            print("\nty_trans: ", ty_trans)
            print("Saving ty_trans to: ", f'{outputdir}/year_ty_trans.nc')
            ty_trans.to_netcdf(f'{outputdir}/year_ty_trans.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {member} ty_trans')
            print(traceback.format_exc())



        # tx_trans_gm
        try:
            print("Loading tx_trans_gm and averaging tx_trans_gm")
            tx_trans_gm = annual_mean("tx_trans_gm", ds)
            print("\ntx_trans_gm: ", tx_trans_gm)
            print("Saving tx_trans_gm to: ", f'{outputdir}/year_tx_trans_gm.nc')
            tx_trans_gm.to_netcdf(f'{outputdir}/year_tx_trans_gm.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {member} tx_trans_gm')
            print(traceback.format_exc())

        # ty_trans_gm
        try:
            print("Loading ty_trans_gm and averaging ty_trans_gm")
            ty_trans_gm = annual_mean("ty_trans_gm", ds)
            print("\nty_trans_gm: ", ty_trans_gm)
            print("Saving ty_trans_gm to: ", f'{outputdir}/year_ty_trans_gm.nc')
            ty_trans_gm.to_netcdf(f'{outputdir}/year_ty_trans_gm.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {member} ty_trans_gm')
            print(traceback.format_exc())



        # tx_trans_submeso
        try:
            print("Loading tx_trans_submeso and averaging tx_trans_submeso")
            tx_trans_submeso = annual_mean("tx_trans_submeso", ds)
            print("\ntx_trans_submeso: ", tx_trans_submeso)
            print("Saving tx_trans_submeso to: ", f'{outputdir}/year_tx_trans_submeso.nc')
            tx_trans_submeso.to_netcdf(f'{outputdir}/year_tx_trans_submeso.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {member} tx_trans_submeso')
            print(traceback.format_exc())

        # ty_trans_submeso
        try:
            print("Loading ty_trans_submeso and averaging ty_trans_submeso")
            ty_trans_submeso = annual_mean("ty_trans_submeso", ds)
            print("\nty_trans_submeso: ", ty_trans_submeso)
            print("Saving ty_trans_submeso to: ", f'{outputdir}/year_ty_trans_submeso.nc')
            ty_trans_submeso.to_netcdf(f'{outputdir}/year_ty_trans_submeso.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {member} ty_trans_submeso')
            print(traceback.format_exc())


    client.close()





