# Prototype python script for fetching and averaging ACCESS data
# Warning: No promises made, this is work in progress!

# required variables for building the transport matrix are:
# - monthly variables:
#   - umo
#   - vmo
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
# The goal is to create NetCDF files of time-averaged ocean-transport states (`umo`, `vmo`, and `mlotst`) from ACCESS-ESM1.5 runs.
#

# import sys to access script arguments (experiment, ensemble, first_year, last_year)
import sys

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

cat_str = "/g/data/dk92/catalog/v2/esm/cmip6-fs38/catalog.json" # <- this is the catalog for ACCESS CMIP6 output at NCI
cat = intake.open_esm_datastore(cat_str)
print(cat)

# Only keep the required data
searched_cat = cat.search(
    source_id = model,
    experiment_id = experiment,
    # member_id = ensemble,
    variable_id = ["umo", "vmo", "mlotst", "volcello", "areacello", "agessc"],
    realm = 'ocean')

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

        volcello = volcello_datadask["volcello"]
        areacello = areacello_datadask["areacello"]

        # Save the fixed data to NetCDF
        print("Saving fixed data to NetCDF")
        outputdir = f'{datadir}/{model}/{experiment}/{member}/{start_time_str}-{end_time_str}'
        print("Creating directory: ", outputdir)
        makedirs(outputdir, exist_ok=True)
        volcello.to_netcdf(f'{outputdir}/volcello.nc', compute=True)
        areacello.to_netcdf(f'{outputdir}/areacello.nc', compute=True)


        # umo dataset
        print("Loading umo data")
        umo_datadask = select_latest_data(searched_cat,
            dict(
                chunks={'i': 60, 'j': 60, 'time': -1, 'lev':50}
            ),
            variable_id = "umo",
            member_id = member,
            frequency = "mon",
        )
        # print("\nuo_datadask: ", umo_datadask)

        # vmo dataset
        print("Loading vmo data")
        vmo_datadask = select_latest_data(searched_cat,
            dict(
                chunks={'i': 60, 'j': 60, 'time': -1, 'lev':50}
            ),
            variable_id = "vmo",
            member_id = member,
            frequency = "mon",
        )
        # print("\nvo_datadask: ", vmo_datadask)

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
        # print("\nmlotst_datadask: ", mlotst_datadask)

        # agessc dataset
        print("Loading agessc data")
        agessc_datadask = select_latest_data(searched_cat,
            dict(
                chunks={'i': 60, 'j': 60, 'time': -1, 'lev':50}
            ),
            variable_id = "agessc",
            member_id = member,
            frequency = "mon",
        )
        # print("\nagessc_datadask: ", agessc_datadask)

        # Average the data
        print("Average the data over the time period")

        # Slice umo dataset for the time period
        umo_datadask_sel = umo_datadask.sel(time=slice(start_time, end_time))
        # Take the time average of the monthly evaporation (using month length as weights)
        umo = umo_datadask_sel["umo"].weighted(umo_datadask_sel.time.dt.days_in_month).mean(dim="time")
        # print("\nuo: ", umo)

        # Slice vmo dataset for the time period
        vmo_datadask_sel = vmo_datadask.sel(time=slice(start_time, end_time))
        # Take the time average of the monthly evaporation (using month length as weights)
        vmo = vmo_datadask_sel["vmo"].weighted(vmo_datadask_sel.time.dt.days_in_month).mean(dim="time")
        # print("\nvo: ", vmo)

        # Slice agessc dataset for the time period
        agessc_datadask_sel = agessc_datadask.sel(time=slice(start_time, end_time))
        # Take the time average of the monthly evaporation (using month length as weights)
        agessc = agessc_datadask_sel["agessc"].weighted(agessc_datadask_sel.time.dt.days_in_month).mean(dim="time")
        # print("\nagessc: ", agessc)

        # Slice mlotst dataset for the time period
        mlotst_datadask_sel = mlotst_datadask.sel(time=slice(start_time, end_time))
        # Take the time mean of the yearly maximum of mlotst
        mlotst_yearlymax = mlotst_datadask_sel.groupby("time.year").max(dim="time")
        # print("\nmlotst_yearlymax: ", mlotst_yearlymax)

        mlotst = mlotst_yearlymax.mean(dim="year")
        # print("\nmlotst: ", mlotst)


        # Save the averaged data to NetCDF
        print("Saving averaged data to NetCDF")
        umo.to_netcdf(f'{outputdir}/umo.nc', compute=True)
        vmo.to_netcdf(f'{outputdir}/vmo.nc', compute=True)
        agessc.to_netcdf(f'{outputdir}/agessc.nc', compute=True)
        mlotst.to_netcdf(f'{outputdir}/mlotst.nc', compute=True)


    client.close()




