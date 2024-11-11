

# import sys to access script arguments (experiment, ensemble, first_year, last_year)
import sys

# interactive use only
model='ACCESS-ESM1-5'
# model=ACCESS-CM2
experiment='historical'
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

# Import os for makedirs/isfile/environ
import os
os.environ["PYTHONWARNINGS"] = "ignore"

# Load dask
from dask.distributed import Client

# import glob for searching directories
from glob import glob

# Load xarray for N-dimensional arrays
import xarray as xr

# Load traceback to print exceptions
import traceback

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

def CSIRO_member(member):
    return f'HI-{member+4:02d}' # note the +4!

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

# Create directory on scratch to save the data
scratchdatadir = '/scratch/xv83/TMIP/data'
gdatadatadir = '/g/data/xv83/TMIP/data'

# Depends on time window
start_time, end_time = time_window_strings(year_start, num_years)
start_time_str = f'Jan{start_time}'
end_time_str = f'Dec{end_time}'

# decades for which files to read (saved for each decade)
decade_start = year_start - year_start % 10
decades = range(decade_start, year_start + num_years, 10)

# Members to loop through
# members = [1, 3, 4]
members = [5, 6, 7, 8]


print("Starting client")

# This `if` statement is required in scripts (not required in Jupyter)
if __name__ == '__main__':
    client = Client(n_workers=24) #, threads_per_worker=1, memory_limit='16GB') # Note: with 1thread/worker cannot plot thetao. Maybe I need to understand why?

    for member in members:

        # print ensemble/member
        print(f"\nProcessing {CSIRO_member(member)} as {CMIP6_member(member)}")

        # directory to save the data to (as NetCDF)
        inputdir = f'{gdatadatadir}/{model}/{CSIRO_member(member)}'
        outputdir = f'{scratchdatadir}/{model}/{experiment}/{CMIP6_member(member)}/{start_time_str}-{end_time_str}'
        print("Creating directory: ", outputdir)
        os.makedirs(outputdir, exist_ok=True)
        print("  averaging data from: ", inputdir)
        print("  to be saved in: ", outputdir)

        # tx_trans_gm
        paths = [f'{inputdir}/month_tx_trans_gm_{decade}s.nc' for decade in decades]
        try:
            print("    Loading tx_trans_gm")
            tx_trans_gm_ds = open_my_dataset(paths)
            print("      selecting time window")
            tx_trans_gm_sel = tx_trans_gm_ds.sel(time=slice(start_time, end_time))
            print("      averaging")
            tx_trans_gm = tx_trans_gm_sel["tx_trans_gm"].weighted(tx_trans_gm_sel.time.dt.days_in_month).mean(dim="time")
            print("\ntx_trans_gm: ", tx_trans_gm)
            print("      saving to: ", f'{outputdir}/tx_trans_gm.nc')
            tx_trans_gm.to_netcdf(f'{outputdir}/tx_trans_gm.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {CSIRO_member(member)}/{CMIP6_member(member)} tx_trans_gm')
            print(traceback.format_exc())

        # ty_trans_gm
        paths = [f'{inputdir}/month_ty_trans_gm_{decade}s.nc' for decade in decades]
        try:
            print("    Loading ty_trans_gm")
            ty_trans_gm_ds = open_my_dataset(paths)
            print("      selecting time window")
            ty_trans_gm_sel = ty_trans_gm_ds.sel(time=slice(start_time, end_time))
            print("      averaging")
            ty_trans_gm = ty_trans_gm_sel["ty_trans_gm"].weighted(ty_trans_gm_sel.time.dt.days_in_month).mean(dim="time")
            print("\nty_trans_gm: ", ty_trans_gm)
            print("      saving to: ", f'{outputdir}/ty_trans_gm.nc')
            ty_trans_gm.to_netcdf(f'{outputdir}/ty_trans_gm.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {CSIRO_member(member)}/{CMIP6_member(member)} ty_trans_gm')
            print(traceback.format_exc())

        # tx_trans_submeso
        paths = [f'{inputdir}/month_tx_trans_submeso_{decade}s.nc' for decade in decades]
        try:
            print("    Loading tx_trans_submeso")
            tx_trans_submeso_ds = open_my_dataset(paths)
            print("      selecting time window")
            tx_trans_submeso_sel = tx_trans_submeso_ds.sel(time=slice(start_time, end_time))
            print("      averaging")
            tx_trans_submeso = tx_trans_submeso_sel["tx_trans_submeso"].weighted(tx_trans_submeso_sel.time.dt.days_in_month).mean(dim="time")
            print("\ntx_trans_submeso: ", tx_trans_submeso)
            print("      saving to: ", f'{outputdir}/tx_trans_submeso.nc')
            tx_trans_submeso.to_netcdf(f'{outputdir}/tx_trans_submeso.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {CSIRO_member(member)}/{CMIP6_member(member)} tx_trans_submeso')
            print(traceback.format_exc())

        # ty_trans_submeso
        paths = [f'{inputdir}/month_ty_trans_submeso_{decade}s.nc' for decade in decades]
        try:
            print("    Loading ty_trans_submeso")
            ty_trans_submeso_ds = open_my_dataset(paths)
            print("      selecting time window")
            ty_trans_submeso_sel = ty_trans_submeso_ds.sel(time=slice(start_time, end_time))
            print("      averaging")
            ty_trans_submeso = ty_trans_submeso_sel["ty_trans_submeso"].weighted(ty_trans_submeso_sel.time.dt.days_in_month).mean(dim="time")
            print("\nty_trans_submeso: ", ty_trans_submeso)
            print("      saving to: ", f'{outputdir}/ty_trans_submeso.nc')
            ty_trans_submeso.to_netcdf(f'{outputdir}/ty_trans_submeso.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {CSIRO_member(member)}/{CMIP6_member(member)} ty_trans_submeso')
            print(traceback.format_exc())

    client.close()





