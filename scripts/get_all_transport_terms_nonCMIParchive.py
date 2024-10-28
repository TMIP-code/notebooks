

# import sys to access script arguments (experiment, ensemble, first_year, last_year)
import sys

# interactive use only
model="ACCESS-ESM1-5"
experiment="historical"
year_start=1990
num_years=10


# Model etc. defined from script input
model = sys.argv[1]
print("Model: ", model, " (type: ", type(model), ")")
experiment = sys.argv[2]
print("Experiment: ", experiment, " (type: ", type(experiment), ")")
year_start = int(sys.argv[3])
num_years = int(sys.argv[4])
print("Time window: ", year_start, " to ", year_start + num_years - 1)

# 1. Load packages

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






# Create directory on scratch to save the data
datadir = '/scratch/xv83/TMIP/data'
start_time, end_time = time_window_strings(year_start, num_years)
start_time_str = f'Jan{start_time}'
end_time_str = f'Dec{end_time}'

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
        paths = [f'{inputdir}/history/ocn/ocean_month.nc-{year}1231' for year in range(year_start, year_start + num_years)]

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
            print("Loading tx_trans")
            tx_trans_var = ds["tx_trans"]
            print("\ntx_trans: ", tx_trans_var)
            print("Averaging tx_trans")
            tx_trans = tx_trans_var.weighted(ds.time.dt.days_in_month).mean(dim="time")
            print("\ntx_trans: ", tx_trans)
            print("Saving tx_trans to: ", f'{outputdir}/tx_trans.nc')
            tx_trans.to_netcdf(f'{outputdir}/tx_trans.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {member} tx_trans')
            print(traceback.format_exc())

        # ty_trans
        try:
            print("Loading ty_trans")
            ty_trans_var = ds["ty_trans"]
            print("\nty_trans: ", ty_trans_var)
            print("Averaging ty_trans")
            ty_trans = ty_trans_var.weighted(ds.time.dt.days_in_month).mean(dim="time")
            print("\nty_trans: ", ty_trans)
            print("Saving ty_trans to: ", f'{outputdir}/ty_trans.nc')
            ty_trans.to_netcdf(f'{outputdir}/ty_trans.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {member} ty_trans')
            print(traceback.format_exc())



        # tx_trans_gm
        try:
            print("Loading tx_trans_gm")
            tx_trans_gm_var = ds["tx_trans_gm"]
            print("\ntx_trans_gm: ", tx_trans_gm_var)
            print("Averaging tx_trans_gm")
            tx_trans_gm = tx_trans_gm_var.weighted(ds.time.dt.days_in_month).mean(dim="time")
            print("\ntx_trans_gm: ", tx_trans_gm)
            print("Saving tx_trans_gm to: ", f'{outputdir}/tx_trans_gm.nc')
            tx_trans_gm.to_netcdf(f'{outputdir}/tx_trans_gm.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {member} tx_trans_gm')
            print(traceback.format_exc())

        # ty_trans_gm
        try:
            print("Loading ty_trans_gm")
            ty_trans_gm_var = ds["ty_trans_gm"]
            print("\nty_trans_gm: ", ty_trans_gm_var)
            print("Averaging ty_trans_gm")
            ty_trans_gm = ty_trans_gm_var.weighted(ds.time.dt.days_in_month).mean(dim="time")
            print("\nty_trans_gm: ", ty_trans_gm)
            print("Saving ty_trans_gm to: ", f'{outputdir}/ty_trans_gm.nc')
            ty_trans_gm.to_netcdf(f'{outputdir}/ty_trans_gm.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {member} ty_trans_gm')
            print(traceback.format_exc())



        # tx_trans_submeso
        try:
            print("Loading tx_trans_submeso")
            tx_trans_submeso_var = ds["tx_trans_submeso"]
            print("\ntx_trans_submeso: ", tx_trans_submeso_var)
            print("Averaging tx_trans_submeso")
            tx_trans_submeso = tx_trans_submeso_var.weighted(ds.time.dt.days_in_month).mean(dim="time")
            print("\ntx_trans_submeso: ", tx_trans_submeso)
            print("Saving tx_trans_submeso to: ", f'{outputdir}/tx_trans_submeso.nc')
            tx_trans_submeso.to_netcdf(f'{outputdir}/tx_trans_submeso.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {member} tx_trans_submeso')
            print(traceback.format_exc())

        # ty_trans_submeso
        try:
            print("Loading ty_trans_submeso")
            ty_trans_submeso_var = ds["ty_trans_submeso"]
            print("\nty_trans_submeso: ", ty_trans_submeso_var)
            print("Averaging ty_trans_submeso")
            ty_trans_submeso = ty_trans_submeso_var.weighted(ds.time.dt.days_in_month).mean(dim="time")
            print("\nty_trans_submeso: ", ty_trans_submeso)
            print("Saving ty_trans_submeso to: ", f'{outputdir}/ty_trans_submeso.nc')
            ty_trans_submeso.to_netcdf(f'{outputdir}/ty_trans_submeso.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {member} ty_trans_submeso')
            print(traceback.format_exc())


    client.close()





