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
print("Ensemble ensemble: ", ensemble, " (type: ", type(ensemble), ")")
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
    start_time = f'{year_start}'
    # end_time is last second of last_year
    end_time = f'{year_start + num_years - 1}'
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
    # if dataframe is empty, error
    if selectedcat.df.empty:
        raise ValueError(f"No data found for {kwargs}")

    latestselectedcat = selectedcat.search(version=find_latest_version(selectedcat))
    return latestselectedcat

def select_latest_data(cat, xarray_open_kwargs, **kwargs):
    latestselectedcat = select_latest_cat(cat, **kwargs)
    print("\nlatestselectedcat: ", latestselectedcat)
    xarray_combine_by_coords_kwargs=dict(
        compat="override",
        data_vars="minimal",
        coords="minimal"
    )
    datadask = latestselectedcat.to_dask(
        xarray_open_kwargs=xarray_open_kwargs,
        xarray_combine_by_coords_kwargs=xarray_combine_by_coords_kwargs,
        parallel=True,
        # preprocess=combined_preprocessing, <- xmip does not work for CMIP5 data ATM
    )
    return datadask



# 3. Load catalog

catalogs = intake.cat.access_nri
cat = catalogs["cmip5_rr3"]
print(cat)

# Only keep the required data
searched_cat = cat.search(
    model = model,
    experiment = experiment,
    # ensemble = ensemble,
    variable = ["uo", "vo", "umo", "vmo", "mlotst", "volcello", "areacello", "agessc", "thetao", "so"],
    realm = 'ocean')
print(searched_cat)

cmip_version = "CMIP5"
# Find ensembles that have all the required data
def summary_variable_availability(df, cmip_version):
    if cmip_version == "CMIP6":
        variable_id = 'variable_id'
        experiment_id = 'experiment_id'
        source_id = 'source_id'
        member_id = 'member_id'
        list1 = ['umo', 'vmo', 'mlotst', 'volcello', 'areacello', 'thetao', 'so']
        list2 = ['uo', 'vo', 'mlotst', 'volcello', 'areacello', 'thetao', 'so']
    elif cmip_version == "CMIP5":
        variable_id = 'variable'
        experiment_id = 'experiment'
        source_id = 'model'
        member_id = 'ensemble'
        # (Note Removed volcello and areacello because could be in different member: r0i0p0)
        list1 = ['umo', 'vmo', 'mlotst', 'thetao', 'so']
        list2 = ['uo', 'vo', 'mlotst', 'thetao', 'so']
    # Step 1: Filter the dataframe to include only the specified variables
    filtered_df_1 = df[df[variable_id].isin(list1)]
    filtered_df_2 = df[df[variable_id].isin(list2)]
    # Step 2: Group by 'source_id' and 'member_id'
    grouped_1 = filtered_df_1.groupby([experiment_id, source_id, member_id])
    grouped_2 = filtered_df_2.groupby([experiment_id, source_id, member_id])
    # Step 3: Find groups that contain all the variables in each set
    valid_groups_1 = grouped_1.filter(lambda x: set(list1).issubset(set(x[variable_id])))
    valid_groups_2 = grouped_2.filter(lambda x: set(list2).issubset(set(x[variable_id])))
    # Step 4: Get the list of source_id and their member_id for each set
    result_1 = valid_groups_1[[experiment_id, source_id, member_id]].drop_duplicates().reset_index(drop=True)
    result_2 = valid_groups_2[[experiment_id, source_id, member_id]].drop_duplicates().reset_index(drop=True)
    # Step 5: Group by 'source_id' and aggregate member_id into a list for each set
    final_result_1 = result_1.groupby([experiment_id, source_id])[member_id].apply(list).reset_index()
    final_result_2 = result_2.groupby([experiment_id, source_id])[member_id].apply(list).reset_index()
    # Step 6: Merge the results into a single dataframe
    merged_result = pd.merge(final_result_1, final_result_2, on=[experiment_id, source_id], how='outer', suffixes=('_umo_vmo', '_uo_vo'))
    return merged_result

# Find ensembles that have all the required data (umo+vmo or uo+vo + all the rest)
availability_df = summary_variable_availability(cat.df, cmip_version)
# grab ensembles to loop over
availability_df = availability_df[(availability_df.model == model) & (availability_df.experiment == experiment)]
[ensembles1] = availability_df.ensemble_umo_vmo
[ensembles2] = availability_df.ensemble_uo_vo
ensembles = list(set(ensembles1) & set(ensembles2))

# sort ensembles that are formatted as "r%di%dp%df%d" where %d is a integer
import re

def extract_numbers(ensemble):
    # Extract integers from the string using regex
    return list(map(int, re.findall(r'\d+', ensemble)))

def sort_ensembles(ensembles):
    # Sort ensembles using the custom key function
    return sorted(ensembles, key=extract_numbers)

sorted_ensembles = sort_ensembles(ensembles)
# print ensembles on one line each
print("\n".join(sorted_ensembles))


# Create directory on scratch to save the data
datadir = '/scratch/xv83/TMIP/data'
start_time, end_time = time_window_strings(year_start, num_years)
start_time_str = f'Jan{start_time}'
end_time_str = f'Dec{end_time}'



# 4. Load data, preprocess it, and save it to NetCDF

print("Starting client")

# This `if` statement is required in scripts (not required in Jupyter)
if __name__ == '__main__':
    client = Client(n_workers=4)#, threads_per_worker=1, memory_limit='16GB') # Note: with 1thread/worker cannot plot thetao. Maybe I need to understand why?

    for ensemble in sorted_ensembles:

        # skip if r0i0p0
        if ensemble == "r0i0p0":
            continue

        # print ensemble/member
        print(f"\nProcessing ensemble: {ensemble}")

        # directory to save the data to (as NetCDF)
        outputdir = f'{datadir}/{model}/{experiment}/{ensemble}/{start_time_str}-{end_time_str}'
        print("Creating directory: ", outputdir)
        makedirs(outputdir, exist_ok=True)

        # volcello
        try:
            print("Loading volcello data")
            volcello_datadask = select_latest_data(searched_cat,
                dict(
                    chunks={'time': -1, 'lev':-1}
                ),
                variable = "volcello",
                ensemble = "r0i0p0", # <- in the CMIP5 ACCESS catalog, the fixed data is in ensemble r0i0p0 (not in any other ensemble)
                table = "fx", # <- in the CMIP5 ACCESS catalog, the fixed data table is "fx" (not "Ofx")
            )
            print("\nvolcello_datadask: ", volcello_datadask)
            volcello_file = f'{outputdir}/volcello.nc'
            print("Saving volcello to: ", volcello_file)
            volcello_datadask.to_netcdf(volcello_file, compute=True)
        except Exception:
            print(f'Error processing {model} {ensemble} volcello')
            print(traceback.format_exc())


        # areacello
        try:
            print("Loading areacello data")
            areacello_datadask = select_latest_data(searched_cat,
                dict(
                    chunks={'time': -1, 'lev':-1}
                ),
                variable = "areacello",
                ensemble = "r0i0p0", # <- in the CMIP5 ACCESS catalog, the fixed data is in ensemble r0i0p0 (not in any other ensemble)
                table = "fx", # <- in the CMIP5 ACCESS catalog, the fixed data table is "fx" (not "Ofx")
            )
            print("\nareacello_datadask: ", areacello_datadask)
            areacello_file = f'{outputdir}/areacello.nc'
            print("Saving areacello to: ", areacello_file)
            areacello_datadask.to_netcdf(areacello_file, compute=True)
        except Exception:
            print(f'Error processing {model} {ensemble} areacello')
            print(traceback.format_exc())


        # umo
        try:
            print("Loading umo data")
            umo_datadask = select_latest_data(searched_cat,
                dict(
                    chunks={'time': -1, 'lev':-1}
                ),
                variable = "umo",
                ensemble = ensemble,
                frequency = "mon",
            )
            print("\numo_datadask: ", umo_datadask)
            print("Slicing umo for the time period")
            umo_datadask_sel = umo_datadask.sel(time=slice(start_time, end_time))
            print("Averaging umo")
            umo = umo_datadask_sel["umo"].weighted(umo_datadask_sel.time.dt.days_in_month).mean(dim="time")
            print("\numo: ", umo)
            print("Saving umo to: ", f'{outputdir}/umo.nc')
            umo.to_netcdf(f'{outputdir}/umo.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {ensemble} umo')
            print(traceback.format_exc())

        # vmo
        try:
            print("Loading vmo data")
            vmo_datadask = select_latest_data(searched_cat,
                dict(
                    chunks={'time': -1, 'lev':-1}
                ),
                variable = "vmo",
                ensemble = ensemble,
                frequency = "mon",
            )
            print("\nvmo_datadask: ", vmo_datadask)
            print("Slicing vmo for the time period")
            vmo_datadask_sel = vmo_datadask.sel(time=slice(start_time, end_time))
            print("Averaging vmo")
            vmo = vmo_datadask_sel["vmo"].weighted(vmo_datadask_sel.time.dt.days_in_month).mean(dim="time")
            print("\nvmo: ", vmo)
            print("Saving vmo to: ", f'{outputdir}/vmo.nc')
            vmo.to_netcdf(f'{outputdir}/vmo.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {ensemble} vmo')
            print(traceback.format_exc())

        # uo
        try:
            print("Loading uo data")
            uo_datadask = select_latest_data(searched_cat,
                dict(
                    chunks={'time': -1, 'lev':-1}
                ),
                variable = "uo",
                ensemble = ensemble,
                frequency = "mon",
            )
            print("\nuo_datadask: ", uo_datadask)
            print("Slicing uo for the time period")
            uo_datadask_sel = uo_datadask.sel(time=slice(start_time, end_time))
            print("Averaging uo")
            uo = uo_datadask_sel["uo"].weighted(uo_datadask_sel.time.dt.days_in_month).mean(dim="time")
            print("\nuo: ", uo)
            print("Saving uo to: ", f'{outputdir}/uo.nc')
            uo.to_netcdf(f'{outputdir}/uo.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {ensemble} uo')
            print(traceback.format_exc())

        # vo
        try:
            print("Loading vo data")
            vo_datadask = select_latest_data(searched_cat,
                dict(
                    chunks={'time': -1, 'lev':-1}
                ),
                variable = "vo",
                ensemble = ensemble,
                frequency = "mon",
            )
            print("\nvo_datadask: ", vo_datadask)
            print("Slicing vo for the time period")
            vo_datadask_sel = vo_datadask.sel(time=slice(start_time, end_time))
            print("Averaging vo")
            vo = vo_datadask_sel["vo"].weighted(vo_datadask_sel.time.dt.days_in_month).mean(dim="time")
            print("\nvo: ", vo)
            print("Saving vo to: ", f'{outputdir}/vo.nc')
            vo.to_netcdf(f'{outputdir}/vo.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {ensemble} vo')
            print(traceback.format_exc())

        # mlotst dataset
        try:
            print("Loading mlotst data")
            mlotst_datadask = select_latest_data(searched_cat,
                dict(
                    chunks={'time': -1, 'lev':-1}
                ),
                variable = "mlotst",
                ensemble = ensemble,
                frequency = "mon",
            )
            print("\nmlotst_datadask: ", mlotst_datadask)
            print("Slicing mlotst for the time period")
            mlotst_datadask_sel = mlotst_datadask.sel(time=slice(start_time, end_time))
            print("Averaging mlotst (mean of the yearly maximum of monthly data)")
            mlotst_yearlymax = mlotst_datadask_sel.groupby("time.year").max(dim="time")
            print("\nmlotst_yearlymax: ", mlotst_yearlymax)
            mlotst = mlotst_yearlymax.mean(dim="year")
            print("\nmlotst: ", mlotst)
            print("Saving mlotst to: ", f'{outputdir}/mlotst.nc')
            mlotst.to_netcdf(f'{outputdir}/mlotst.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {ensemble} mlotst')
            print(traceback.format_exc())

        # thetao dataset
        try:
            print("Loading thetao data")
            thetao_datadask = select_latest_data(searched_cat,
                dict(
                    chunks={'time': -1, 'lev':-1}
                ),
                variable = "thetao",
                ensemble = ensemble,
                frequency = "mon",
            )
            print("\nthetao_datadask: ", thetao_datadask)
            print("Slicing thetao for the time period")
            thetao_datadask_sel = thetao_datadask.sel(time=slice(start_time, end_time))
            print("Averaging thetao")
            thetao = thetao_datadask_sel.weighted(thetao_datadask_sel.time.dt.days_in_month).mean(dim="time")
            print("\nthetao: ", thetao)
            print("Saving thetao to: ", f'{outputdir}/thetao.nc')
            thetao.to_netcdf(f'{outputdir}/thetao.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {ensemble} thetao')
            print(traceback.format_exc())

        # so dataset
        try:
            print("Loading so data")
            so_datadask = select_latest_data(searched_cat,
                dict(
                    chunks={'time': -1, 'lev':-1}
                ),
                variable = "so",
                ensemble = ensemble,
                frequency = "mon",
            )
            print("\nso_datadask: ", so_datadask)
            print("Slicing so for the time period")
            so_datadask_sel = so_datadask.sel(time=slice(start_time, end_time))
            print("Averaging so")
            so = so_datadask_sel.weighted(so_datadask_sel.time.dt.days_in_month).mean(dim="time")
            print("\nso: ", so)
            print("Saving so to: ", f'{outputdir}/so.nc')
            so.to_netcdf(f'{outputdir}/so.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {ensemble} so')
            print(traceback.format_exc())

        # agessc dataset
        try:
            print("Loading agessc data")
            agessc_datadask = select_latest_data(searched_cat,
                dict(
                    chunks={'time': -1, 'lev':-1}
                ),
                variable = "agessc",
                ensemble = ensemble,
                frequency = "mon",
            )
            print("\nagessc_datadask: ", agessc_datadask)
            print("Slicing agessc for the time period")
            agessc_datadask_sel = agessc_datadask.sel(time=slice(start_time, end_time))
            print("Averaging agessc")
            agessc = agessc_datadask_sel["agessc"].weighted(agessc_datadask_sel.time.dt.days_in_month).mean(dim="time")
            print("\nagessc: ", agessc)
            print("Saving agessc to: ", f'{outputdir}/agessc.nc')
            agessc.to_netcdf(f'{outputdir}/agessc.nc', compute=True)
        except Exception:
            print(f'Error processing {model} {ensemble} agessc')
            print(traceback.format_exc())



    client.close()




