# qsub -I -P xv83 -l ncpus=48 -l mem=190GB -l jobfs=4GB -l walltime=1:00:00 -l storage=gdata/xv83+gdata/oi10+gdata/dk92+gdata/hh5+gdata/rr3+gdata/al33+gdata/fs38+gdata/xp65+gdata/p73
# qsub -I -q express -P xv83 -l ncpus=48 -l mem=190GB -l jobfs=4GB -l walltime=1:00:00 -l storage=gdata/xv83+gdata/dk92+gdata/hh5+gdata/xp65+gdata/p73

# import sys to access script arguments (experiment, ensemble, first_year, last_year)
import sys

# These are fixed (correspond to my AA simulation)
model='ACCESS-ESM1-5'
experiment='historical'
year_start=1850
num_years=10

# 1. Load packages

# Import os for makedirs/isfile/environ
import os
os.environ["PYTHONWARNINGS"] = "ignore"

# Load dask
from dask.distributed import Client

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
AAdatadir = '/scratch/xv83/bp3051/access-esm/archive/andersonacceleration_test-n10-5415f621/'
gdatadatadir = '/g/data/xv83/TMIP/data'

# Depends on time window
start_time, end_time = time_window_strings(year_start, num_years)
start_time_str = f'Jan{start_time}'
end_time_str = f'Dec{end_time}'

# Simulation years from AA output
simyears = range(10) # AA cycles of 10 years

print("Starting client")

# This `if` statement is required in scripts (not required in Jupyter)
if __name__ == '__main__':
    client = Client(n_workers=44, threads_per_worker=1) #, memory_limit='16GB') # Note: with 1thread/worker cannot plot thetao. Maybe I need to understand why?
    # print ensemble/member
    print(f"\nProcessing AA output")
    # directory to save the data to (as NetCDF)
    outputdir = f'{scratchdatadir}/{model}/{experiment}/AA/{start_time_str}-{end_time_str}'
    print("Creating directory: ", outputdir)
    os.makedirs(outputdir, exist_ok=True)
    print("  averaging data from: ", AAdatadir)
    print("  to be saved in: ", outputdir)
    # # tx_trans
    # paths = [f'{AAdatadir}/output00{simyear}/ocean/ocean-3d-tx_trans-1monthly-mean-ym_185{simyear}_01.nc' for simyear in simyears]
    # try:
    #     print("    Loading tx_trans")
    #     tx_trans_ds = open_my_dataset(paths)
    #     print("      selecting time window")
    #     tx_trans_sel = tx_trans_ds.sel(time=slice(start_time, end_time))
    #     print("      averaging")
    #     tx_trans = tx_trans_sel["tx_trans"].weighted(tx_trans_sel.time.dt.days_in_month).mean(dim="time")
    #     print("\ntx_trans: ", tx_trans)
    #     print("      saving to: ", f'{outputdir}/tx_trans.nc')
    #     tx_trans.to_netcdf(f'{outputdir}/tx_trans.nc', compute=True)
    # except Exception:
    #     print(f'Error processing tx_trans')
    #     print(traceback.format_exc())
    # # ty_trans
    # paths = [f'{AAdatadir}/output00{simyear}/ocean/ocean-3d-ty_trans-1monthly-mean-ym_185{simyear}_01.nc' for simyear in simyears]
    # try:
    #     print("    Loading ty_trans")
    #     ty_trans_ds = open_my_dataset(paths)
    #     print("      selecting time window")
    #     ty_trans_sel = ty_trans_ds.sel(time=slice(start_time, end_time))
    #     print("      averaging")
    #     ty_trans = ty_trans_sel["ty_trans"].weighted(ty_trans_sel.time.dt.days_in_month).mean(dim="time")
    #     print("\nty_trans: ", ty_trans)
    #     print("      saving to: ", f'{outputdir}/ty_trans.nc')
    #     ty_trans.to_netcdf(f'{outputdir}/ty_trans.nc', compute=True)
    # except Exception:
    #     print(f'Error processing ty_trans')
    #     print(traceback.format_exc())
    # # tx_trans_gm
    # paths = [f'{AAdatadir}/output00{simyear}/ocean/ocean-3d-tx_trans_gm-1monthly-mean-ym_185{simyear}_01.nc' for simyear in simyears]
    # try:
    #     print("    Loading tx_trans_gm")
    #     tx_trans_gm_ds = open_my_dataset(paths)
    #     print("      selecting time window")
    #     tx_trans_gm_sel = tx_trans_gm_ds.sel(time=slice(start_time, end_time))
    #     print("      averaging")
    #     tx_trans_gm = tx_trans_gm_sel["tx_trans_gm"].weighted(tx_trans_gm_sel.time.dt.days_in_month).mean(dim="time")
    #     print("\ntx_trans_gm: ", tx_trans_gm)
    #     print("      saving to: ", f'{outputdir}/tx_trans_gm.nc')
    #     tx_trans_gm.to_netcdf(f'{outputdir}/tx_trans_gm.nc', compute=True)
    # except Exception:
    #     print(f'Error processing tx_trans_gm')
    #     print(traceback.format_exc())
    # # ty_trans_gm
    # paths = [f'{AAdatadir}/output00{simyear}/ocean/ocean-3d-ty_trans_gm-1monthly-mean-ym_185{simyear}_01.nc' for simyear in simyears]
    # try:
    #     print("    Loading ty_trans_gm")
    #     ty_trans_gm_ds = open_my_dataset(paths)
    #     print("      selecting time window")
    #     ty_trans_gm_sel = ty_trans_gm_ds.sel(time=slice(start_time, end_time))
    #     print("      averaging")
    #     ty_trans_gm = ty_trans_gm_sel["ty_trans_gm"].weighted(ty_trans_gm_sel.time.dt.days_in_month).mean(dim="time")
    #     print("\nty_trans_gm: ", ty_trans_gm)
    #     print("      saving to: ", f'{outputdir}/ty_trans_gm.nc')
    #     ty_trans_gm.to_netcdf(f'{outputdir}/ty_trans_gm.nc', compute=True)
    # except Exception:
    #     print(f'Error processing ty_trans_gm')
    #     print(traceback.format_exc())
    # # tx_trans_submeso
    # paths = [f'{AAdatadir}/output00{simyear}/ocean/ocean-3d-tx_trans_submeso-1monthly-mean-ym_185{simyear}_01.nc' for simyear in simyears]
    # try:
    #     print("    Loading tx_trans_submeso")
    #     tx_trans_submeso_ds = open_my_dataset(paths)
    #     print("      selecting time window")
    #     tx_trans_submeso_sel = tx_trans_submeso_ds.sel(time=slice(start_time, end_time))
    #     print("      averaging")
    #     tx_trans_submeso = tx_trans_submeso_sel["tx_trans_submeso"].weighted(tx_trans_submeso_sel.time.dt.days_in_month).mean(dim="time")
    #     print("\ntx_trans_submeso: ", tx_trans_submeso)
    #     print("      saving to: ", f'{outputdir}/tx_trans_submeso.nc')
    #     tx_trans_submeso.to_netcdf(f'{outputdir}/tx_trans_submeso.nc', compute=True)
    # except Exception:
    #     print(f'Error processing tx_trans_submeso')
    #     print(traceback.format_exc())
    # # ty_trans_submeso
    # paths = [f'{AAdatadir}/output00{simyear}/ocean/ocean-3d-ty_trans_submeso-1monthly-mean-ym_185{simyear}_01.nc' for simyear in simyears]
    # try:
    #     print("    Loading ty_trans_submeso")
    #     ty_trans_submeso_ds = open_my_dataset(paths)
    #     print("      selecting time window")
    #     ty_trans_submeso_sel = ty_trans_submeso_ds.sel(time=slice(start_time, end_time))
    #     print("      averaging")
    #     ty_trans_submeso = ty_trans_submeso_sel["ty_trans_submeso"].weighted(ty_trans_submeso_sel.time.dt.days_in_month).mean(dim="time")
    #     print("\nty_trans_submeso: ", ty_trans_submeso)
    #     print("      saving to: ", f'{outputdir}/ty_trans_submeso.nc')
    #     ty_trans_submeso.to_netcdf(f'{outputdir}/ty_trans_submeso.nc', compute=True)
    # except Exception:
    #     print(f'Error processing ty_trans_submeso')
    #     print(traceback.format_exc())
    # MLD
    paths = [f'{AAdatadir}/output00{simyear}/ocean/ocean-2d-mld-1monthly-mean-ym_185{simyear}_01.nc' for simyear in simyears]
    try:
        print("Loading mlotst data")
        mlotst_ds = open_my_dataset(paths)
        print("\nmlotst_ds: ", mlotst_ds)
        print("Slicing mlotst for the time period")
        mlotst_ds_sel = mlotst_ds.sel(time=slice(start_time, end_time))
        print("Averaging mlotst (mean of the yearly maximum of monthly data)")
        mlotst_yearlymax = mlotst_ds_sel.groupby("time.year").max(dim="time")
        print("\nmlotst_yearlymax: ", mlotst_yearlymax)
        mlotst = mlotst_yearlymax.mean(dim="year")
        print("\nmlotst: ", mlotst)
        print("Saving mlotst to: ", f'{outputdir}/mlotst.nc')
        mlotst.to_netcdf(f'{outputdir}/mlotst.nc', compute=True)
        mlotst_max = mlotst_ds_sel.max(dim="time")
        print("\nmlotst_max: ", mlotst_max)
        print("Saving mlotst_max to: ", f'{outputdir}/mlotst_max.nc')
        mlotst_max.to_netcdf(f'{outputdir}/mlotst_max.nc', compute=True)
    except Exception:
        print(f'Error processing {model} {member} mlotst')
        print(traceback.format_exc())
    client.close()





