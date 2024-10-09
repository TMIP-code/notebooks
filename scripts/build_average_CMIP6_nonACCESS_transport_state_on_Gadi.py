#!/g/data/hh5/public/apps/cms_conda_scripts/analysis-22.10.d/bin/python3

# Prototype python script for fetching and averaging ACCESS data
# Warning: No promises made, this is work in progress!

# required variables for building the transport matrix are:
# - monthly variables:
#   - umo (or uo)
#   - vmo (or uo)
#   - mlotst (average of the yearly maximum of the mixed layer)
# - fixed variables (not need for any preprocessing, but listed here for completeness):
#   - areacello
#   - volcello
#       - lon
#       - lat
#       - depth
#       - lon_vertices
#       - lat_vertices
#
# The goal is to create NetCDF files of time-averaged ocean-transport states (`uo`, `vo`, and `mlotst`) from ACCESS-ESM1.5 runs.
#

# import sys to access script arguments (experiment, ensemble, first_year, last_year)
import sys

# For interactive debugging, you can use the following lines to set the arguments
# and skip the following lines that parse the arguments from `sys.argv`
models = [
    'CMCC-CM2-HR4', 'CMCC-CM2-SR5', 'CMCC-ESM2', 'FGOALS-f3-L',
    'FGOALS-g3', 'MPI-ESM-1-2-HAM', 'MPI-ESM1-2-HR', 'MPI-ESM1-2-LR',
    'NorCPM1', 'NorESM2-LM', 'NorESM2-MM', 'CESM2', 'CESM2-FV2',
    'CESM2-WACCM-FV2', 'TaiESM1-TIMCOM'
]
model = "BCC-CSM2-MR"
model = "CESM2"
model = models[0]
experiment = "historical"
ensemble = "r1i1p1f1" # <- note that this is not used in the script
year_start = 1990
num_years = 10

# Model etc. defined from script input
model = sys.argv[1]
print("Model: ", model, " (type: ", type(model), ")")
experiment = sys.argv[2]
print("Experiment: ", experiment, " (type: ", type(experiment), ")")
ensemble = sys.argv[3]
print("Ensemble member: ", ensemble, " (type: ", type(ensemble), ")")
year_start = int(sys.argv[4])
num_years = int(sys.argv[5])
print("Time window: ", year_start, " to ", year_start + num_years - 1)

# 1. Load packages

# Ignore warnings
from os import environ
environ["PYTHONWARNINGS"] = "ignore"

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

# Load xmip for preprocessing (trying to get consistent metadata for making matrices down the road)
from xmip.preprocessing import combined_preprocessing

# Load traceback to print exceptions
import traceback

# 2. Define some functions
# (to avoid too much boilerplate code)
print("Defining functions")

def time_window_strings(year_start, num_years, time_type = datetime.datetime):
    """
    return strings for start_time and end_time
    """
    # start_time is first second of year_start
    start_time = time_type(year_start, 1, 1, 0, 0, 0)
    # end_time is last second of last_year
    end_time = time_type(year_start + num_years - 1, 12, 31, 23, 59, 59)
    # Return the weighted average
    return start_time, end_time

def find_latest_version(cat):
    """
    find latest version of selected data
    """
    sorted_versions = cat.df.version.to_list()
    sorted_versions.sort()
    latest_version = sorted_versions[-1]
    return latest_version

def select_latest_cat(cat, **kwargs):
    """
    search latest version of selected data
    """
    selectedcat = cat.search(**kwargs)
    latestselectedcat = selectedcat.search(version=find_latest_version(selectedcat))
    return latestselectedcat

def select_latest_data(cat, xarray_open_kwargs, **kwargs):
    latestselectedcat = select_latest_cat(cat, **kwargs)
    xarray_combine_by_coords_kwargs=dict(
        compat="override",
        data_vars="minimal",
        coords="minimal"
    )
    datadask = latestselectedcat.to_dask(
        xarray_open_kwargs=xarray_open_kwargs,
        xarray_combine_by_coords_kwargs=xarray_combine_by_coords_kwargs,
        parallel=True,
        preprocess=combined_preprocessing,
    )
    return datadask



# 3. Load catalog

catalogs = intake.cat.access_nri
cat = catalogs["cmip6_oi10"]
print(cat)

# Only keep the required data
searched_cat = cat.search(
    source_id = model,
    experiment_id = experiment,
    # member_id = ensemble,
    # file_type = "l",
    variable_id = ["uo", "vo", "umo", "vmo", "mlotst", "volcello", "areacello", "agessc"],
    # variable_id = ["umo"],
    realm = 'ocean')
print(searched_cat)

members = searched_cat.df.member_id.unique()

# sort members that are formatted as "r%di%dp%df%d" where %d is a integer
import re

def extract_numbers(member):
    # Extract integers from the string using regex
    return list(map(int, re.findall(r'\d+', member)))

def sort_members(members):
    # Sort members using the custom key function
    return sorted(members, key=extract_numbers)

sorted_members = sort_members(members)
# print members on one line each
print("\n".join(sorted_members))


# Create directory on scratch to save the data
datadir = '/scratch/xv83/TMIP/data'
start_time, end_time = time_window_strings(year_start, num_years)
start_time_str = start_time.strftime("%b%Y")
end_time_str = end_time.strftime("%b%Y")



# 4. Load data, preprocess it, and save it to NetCDF

print("Starting client")

# This `if` statement is required in scripts (not required in Jupyter)
if __name__ == '__main__':
    client = Client(n_workers=4)#, threads_per_worker=1, memory_limit='16GB') # Note: with 1thread/worker cannot plot thetao. Maybe I need to understand why?

    for member in sorted_members:

        print(f"\nProcessing member: {member}")

        # Load the fixed data
        print("Loading fixed data")

        # volcello dataset
        print("Loading volcello data")
        volcello_datadask = select_latest_data(searched_cat,
            dict(
                chunks={'i': 60, 'j': 60, 'lev':50}
            ),
            variable_id = "volcello",
            member_id = member,
            table_id = "Ofx",
        )
        print("\nvolcello_datadask: ", volcello_datadask)

        # areacello dataset
        print("Loading areacello data")
        areacello_datadask = select_latest_data(searched_cat,
            dict(
                chunks={'i': 60, 'j': 60}
            ),
            variable_id = "areacello",
            member_id = member,
            table_id = "Ofx",
        )
        print("\nareacello_datadask: ", areacello_datadask)

        volcello = volcello_datadask["volcello"]
        areacello = areacello_datadask["areacello"]

        # Save the fixed data to NetCDF
        print("Saving fixed data to NetCDF")
        outputdir = f'{datadir}/{model}/{experiment}/{member}/{start_time_str}-{end_time_str}'
        print("Creating directory: ", outputdir)
        makedirs(outputdir, exist_ok=True)

        volcello_file = f'{outputdir}/volcello.nc'
        print("Saving volcello to: ", volcello_file)
        volcello_datadask.to_netcdf(volcello_file, compute=True)

        areacello_file = f'{outputdir}/areacello.nc'
        print("Saving areacello to: ", areacello_file)
        areacello_datadask.to_netcdf(areacello_file, compute=True)

        # umo
        try:
            print("Loading umo data")
            umo_datadask = select_latest_data(searched_cat,
                dict(
                    chunks={'i': 60, 'j': 60, 'time': -1, 'lev':50}
                ),
                variable_id = "umo",
                member_id = member,
                frequency = "mon",
            )
            print("\numo_datadask: ", umo_datadask)
            print("Slicing umo for the time period")
            start_time, end_time = time_window_strings(year_start, num_years, time_type = type(umo_datadask.time.values[0]))
            umo_datadask_sel = umo_datadask.sel(time=slice(start_time, end_time))
            print("Averaging umo")
            umo = umo_datadask_sel["umo"].weighted(umo_datadask_sel.time.dt.days_in_month).mean(dim="time")
            print("\numo: ", umo)
            print("Saving umo to: ", f'{outputdir}/umo.nc')
            umo.to_netcdf(f'{outputdir}/umo.nc', compute=True)
        except Exception:
            print("Error processing umo")
            print(traceback.format_exc())

        # vmo
        try:
            print("Loading vmo data")
            vmo_datadask = select_latest_data(searched_cat,
                dict(
                    chunks={'i': 60, 'j': 60, 'time': -1, 'lev':50}
                ),
                variable_id = "vmo",
                member_id = member,
                frequency = "mon",
            )
            print("\nvmo_datadask: ", vmo_datadask)
            print("Slicing vmo for the time period")
            start_time, end_time = time_window_strings(year_start, num_years, time_type = type(vmo_datadask.time.values[0]))
            vmo_datadask_sel = vmo_datadask.sel(time=slice(start_time, end_time))
            print("Averaging vmo")
            vmo = vmo_datadask_sel["vmo"].weighted(vmo_datadask_sel.time.dt.days_in_month).mean(dim="time")
            print("\nvmo: ", vmo)
            print("Saving vmo to: ", f'{outputdir}/vmo.nc')
            vmo.to_netcdf(f'{outputdir}/vmo.nc', compute=True)
        except Exception:
            print("Error processing vmo")
            print(traceback.format_exc())

        # uo
        try:
            print("Loading uo data")
            uo_datadask = select_latest_data(searched_cat,
                dict(
                    chunks={'i': 60, 'j': 60, 'time': -1, 'lev':50}
                ),
                variable_id = "uo",
                member_id = member,
                frequency = "mon",
            )
            print("\nuo_datadask: ", uo_datadask)
            print("Slicing uo for the time period")
            start_time, end_time = time_window_strings(year_start, num_years, time_type = type(uo_datadask.time.values[0]))
            uo_datadask_sel = uo_datadask.sel(time=slice(start_time, end_time))
            print("Averaging uo")
            uo = uo_datadask_sel["uo"].weighted(uo_datadask_sel.time.dt.days_in_month).mean(dim="time")
            print("\nuo: ", uo)
            print("Saving uo to: ", f'{outputdir}/uo.nc')
            uo.to_netcdf(f'{outputdir}/uo.nc', compute=True)
        except Exception:
            print("Error processing uo")
            print(traceback.format_exc())

        # vo
        try:
            print("Loading vo data")
            vo_datadask = select_latest_data(searched_cat,
                dict(
                    chunks={'i': 60, 'j': 60, 'time': -1, 'lev':50}
                ),
                variable_id = "vo",
                member_id = member,
                frequency = "mon",
            )
            print("\nvo_datadask: ", vo_datadask)
            print("Slicing vo for the time period")
            start_time, end_time = time_window_strings(year_start, num_years, time_type = type(vo_datadask.time.values[0]))
            vo_datadask_sel = vo_datadask.sel(time=slice(start_time, end_time))
            print("Averaging vo")
            vo = vo_datadask_sel["vo"].weighted(vo_datadask_sel.time.dt.days_in_month).mean(dim="time")
            print("\nvo: ", vo)
            print("Saving vo to: ", f'{outputdir}/vo.nc')
            vo.to_netcdf(f'{outputdir}/vo.nc', compute=True)
        except Exception:
            print("Error processing vo")
            print(traceback.format_exc())

        # mlotst dataset
        print("Loading mlotst data")
        mlotst_datadask = select_latest_data(searched_cat,
            dict(
                chunks={'i': 60, 'j': 60, 'time': -1, 'lev':50}
            ),
            variable_id = "mlotst",
            member_id = member,
            frequency = "mon",
        )
        print("\nmlotst_datadask: ", mlotst_datadask)
        print("Slicing mlotst for the time period")
        start_time, end_time = time_window_strings(year_start, num_years, time_type = type(mlotst_datadask.time.values[0]))
        mlotst_datadask_sel = mlotst_datadask.sel(time=slice(start_time, end_time))
        print("Averaging mlotst (mean of the yearly maximum of monthly data)")
        mlotst_yearlymax = mlotst_datadask_sel.groupby("time.year").max(dim="time")
        print("\nmlotst_yearlymax: ", mlotst_yearlymax)
        mlotst = mlotst_yearlymax.mean(dim="year")
        print("\nmlotst: ", mlotst)
        print("Saving mlotst to: ", f'{outputdir}/mlotst.nc')
        mlotst.to_netcdf(f'{outputdir}/mlotst.nc', compute=True)



        # agessc dataset
        try:
            print("Loading agessc data")
            agessc_datadask = select_latest_data(searched_cat,
                dict(
                    chunks={'i': 60, 'j': 60, 'time': -1, 'lev':50}
                ),
                variable_id = "agessc",
                member_id = member,
                frequency = "mon",
            )
            print("\nagessc_datadask: ", agessc_datadask)
            print("Slicing agessc for the time period")
            start_time, end_time = time_window_strings(year_start, num_years, time_type = type(agessc_datadask.time.values[0]))
            agessc_datadask_sel = agessc_datadask.sel(time=slice(start_time, end_time))
            print("Averaging agessc")
            agessc = agessc_datadask_sel["agessc"].weighted(agessc_datadask_sel.time.dt.days_in_month).mean(dim="time")
            print("\nagessc: ", agessc)
            print("Saving agessc to: ", f'{outputdir}/agessc.nc')
            agessc.to_netcdf(f'{outputdir}/agessc.nc', compute=True)
        except Exception:
            print("Error processing agessc")
            print(traceback.format_exc())



    client.close()




