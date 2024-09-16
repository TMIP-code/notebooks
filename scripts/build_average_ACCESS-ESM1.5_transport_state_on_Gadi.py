#!/usr/bin/env python
# coding: utf-8

# # Prototype python script for fetching and averaging ACCESS data
# 
# The goal is to be able to create NetCDF files of time-averaged ocean-transport states (`umo`, `vmo`, `uo`, `vo`, and `mlotst`) from ACCESS runs.
# 
# Eventually, this notebook will be turned into a function with these inputs:
# - model
# - experiment
# - ensemble member
# - time period
# that a script can put in a loop to generate lots of datasets.
# 
# Down the road, a Julia script using [OceanTransportMatrixBuilder.jl](https://github.com/TMIP-code/OceanTransportMatrixBuilder.jl) can grab the time-averaged data from these files and generate the transport matrices.
# 
# Note that this notebook is specific to Gadi at NCI. It may require access to at least one of the following projects (so make sure you add them all in your interactive ARE job submission):
# ```
# gdata/gh0+gdata/xv83+gdata/oi10+gdata/dk92+gdata/hh5+gdata/rr3+gdata/al33+gdata/fs38
# ```
# 
# Warning: No promises made, this is work in progress!

# ## 1. Load packages

# In[1]:


# Ignore warnings
from os import environ
environ["PYTHONWARNINGS"] = "ignore"


# In[2]:


# Import makedirs to create directories where I write new files
from os import makedirs


# In[3]:


# Load dask
from dask.distributed import Client

# Load intake and cosima cookbook
import intake
import cosima_cookbook as cc

# Load xarray for N-dimensional arrays
import xarray as xr

# Load xesmf for regridding
import xesmf as xe

# Load datetime to deal with time formats
import datetime

# Load numpy for numbers!
import numpy as np

# Load xmip for preprocessing (trying to get consistent metadata for making matrices down the road)
from xmip.preprocessing import combined_preprocessing

# Load pandas for DataFrame manipulations
import pandas as pd


# ## 2. Define some functions
# 
# (to avoid too much boilerplate code)

# In[4]:


########## functions ##########
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
        parallel=True
    )
    return datadask



# ## 3. Select the model, experiment, ensemble, and time window
# 
# On NCI, the catalog depends not only on the CMIP version but also on the model: For some reason that eludes me, some of the Australian data (including ACCESS1.3, ACCESS-ESM1.5, and ACCESS-CM2) lives in its own ***separate*** catalog. So I need to define these first 

# In[ ]:





# In[5]:


# Comment/uncomment
model = "ACCESS-ESM1-5"


# In[6]:


# Load catalog
cat_str = "/g/data/dk92/catalog/v2/esm/cmip6-fs38/catalog.json"


# In[7]:


# The catalog
cat = intake.open_esm_datastore(cat_str)
cat


# A little detour to list all the models in the catalog:

# In[8]:


models = np.sort(cat.search(realm = 'ocean').df.source_id.unique())
print(*models, sep = "\n")


# The creation of a matrix can only work if the following set of variables is available:
# - mass transports (`umo` and `vmo`)
# - mixed-layer depth (`mlotst`)
# 
# Alternatively, we can use `umo` and `vmo` (in kg/s) can be replaced by `uo` and `vo` (m/s). However, the conversion to mass transport requires the grid-cell volume (`volcello`), grid-cell areas (from vertices and `thkcello`), and density (no variable so I guess constant will do).
# 
# So the notebook here will create all the files for transport, if available: `umo`, `vmo`, `uo`, `vo`, `mlotst`.

# In[9]:


experiment = "historical"


# List of members

# In[10]:


members = np.sort(cat.search(source_id = model, realm = 'ocean').df.member_id.unique())
print(*members, sep = "\n")


# In[ ]:





# In[ ]:





# In[ ]:





# In[11]:


def list_models_and_members_that_have(cat, variables):
    """
    find the list of models and their members that have all the variables: 'uo', 'vo', and 'mlotst'.
    """
    # Step 1: Filter the dataframe to include only the specified variables
    filtered_df = cat.search(variable_id = variables).df
    
    # Step 2: Group by 'model' and 'member'
    grouped = filtered_df.groupby(['source_id', 'member_id'])
    
    # Step 3: Find groups that contain all three variables
    valid_groups = grouped.filter(lambda x: set(variables).issubset(set(x['variable_id'])))
    
    # Step 4: Get the list of models and their members
    result = valid_groups[['source_id', 'member_id']].drop_duplicates().reset_index(drop=True)
    
    # Step 5: Sort the result by model
    result_sorted = result.sort_values(by='source_id')

    # Setp 6: Regroup by model
    grouped_result_sorted = result_sorted.groupby('source_id')
    
    return grouped_result_sorted.apply(display)


# In[12]:


def summary_variable_availability(df):

    # Step 1: Filter the dataframe to include only the specified variables
    filtered_df_1 = df[df['variable_id'].isin(['umo', 'vmo'])]
    filtered_df_2 = df[df['variable_id'].isin(['uo', 'vo'])]
    filtered_df_3 = df[df['variable_id'].isin(['mlotst'])]
    filtered_df_4 = df[df['variable_id'].isin(['mlotstmax'])]
    
    # Step 2: Group by 'source_id' and 'member_id'
    grouped_1 = filtered_df_1.groupby(['experiment_id', 'source_id', 'member_id'])
    grouped_2 = filtered_df_2.groupby(['experiment_id', 'source_id', 'member_id'])
    grouped_3 = filtered_df_3.groupby(['experiment_id', 'source_id', 'member_id'])
    grouped_4 = filtered_df_4.groupby(['experiment_id', 'source_id', 'member_id'])
    
    # Step 3: Find groups that contain all the variables in each set
    valid_groups_1 = grouped_1.filter(lambda x: set(['umo', 'vmo']).issubset(set(x['variable_id'])))
    valid_groups_2 = grouped_2.filter(lambda x: set(['uo', 'vo']).issubset(set(x['variable_id'])))
    valid_groups_3 = grouped_3.filter(lambda x: set(['mlotst']).issubset(set(x['variable_id'])))
    valid_groups_4 = grouped_4.filter(lambda x: set(['mlotstmax']).issubset(set(x['variable_id'])))
    
    # Step 4: Get the list of source_id and their member_id for each set
    result_1 = valid_groups_1[['experiment_id', 'source_id', 'member_id']].drop_duplicates().reset_index(drop=True)
    result_2 = valid_groups_2[['experiment_id', 'source_id', 'member_id']].drop_duplicates().reset_index(drop=True)
    result_3 = valid_groups_3[['experiment_id', 'source_id', 'member_id']].drop_duplicates().reset_index(drop=True)
    result_4 = valid_groups_4[['experiment_id', 'source_id', 'member_id']].drop_duplicates().reset_index(drop=True)
    
    # Step 5: Group by 'source_id' and aggregate member_id into a list for each set
    final_result_1 = result_1.groupby(['experiment_id', 'source_id'])['member_id'].apply(list).reset_index()
    final_result_2 = result_2.groupby(['experiment_id', 'source_id'])['member_id'].apply(list).reset_index()
    final_result_3 = result_3.groupby(['experiment_id', 'source_id'])['member_id'].apply(list).reset_index()
    final_result_4 = result_4.groupby(['experiment_id', 'source_id'])['member_id'].apply(list).reset_index()
    
    # Step 6: Merge the results into a single dataframe
    merged_result_1 = pd.merge(final_result_1, final_result_2, on=['experiment_id', 'source_id'], how='outer', suffixes=('_umo_vmo', '_uo_vo'))
    merged_result_2 = pd.merge(merged_result_1, final_result_3, on=['experiment_id', 'source_id'], how='outer', suffixes=('', '_mlotst'))
    merged_result_3 = pd.merge(merged_result_2, final_result_4, on=['experiment_id', 'source_id'], how='outer', suffixes=('', '_mlotstmax'))

    final_restult = merged_result_3.sort_values(by='source_id')
    
    return final_restult


# In[13]:


summary_variable_availability(cat.df)


# In[14]:


model


# In[ ]:





# In[15]:


list_models_and_members_that_have(cat, ['umo', 'vmo', 'mlotst'])


# In[ ]:





# In[16]:


ensemble = "r1i1p1f1"


# In[ ]:





# In[17]:


year_start = 1990
num_years = 10


# In[ ]:





# In[ ]:





# In[18]:


def variable_availability_check(cat, **kwargs):
    """
    List the relevant variables available and if more is needed.
    """
    searched_cat = cat.search(**kwargs)
    umo_cat = searched_cat.search(variable_id = "umo")
    vmo_cat = searched_cat.search(variable_id = "vmo")
    mlotstmo_cat = searched_cat.search(variable_id = "mlotst")

    print("\numo:\n")
    print(available_time_window(umo_cat))
    print("\nvmo:\n")
    print(available_time_window(vmo_cat))
    print("\nmlotst:\n")
    print(available_time_window(mlotstmo_cat))

    return


def available_time_window(cat):
    time_ranges = cat.df.time_range.unique()
    idx = np.argsort([int(foo[0:4]) for foo in time_ranges if foo != 'na'])
    # return time_ranges[idx]
    return time_ranges[idx]


# In[19]:


model, experiment, ensemble


# In[20]:


variable_availability_check(cat,
    source_id = model,
    experiment_id = experiment,
    member_id = ensemble,    
    realm = 'ocean'
)                            


# In[21]:


# Check that catalog contains the data requested before creating empty directories
searched_cat = cat.search(
    source_id = model,
    experiment_id = experiment,
    member_id = ensemble,
    variable_id = ["umo", "vmo", "mlotst"],
    realm = 'ocean')
np.sort(searched_cat.df.source_id.unique())


# In[22]:


searched_searched_cat = searched_cat.search(variable_id = "uo")
searched_searched_cat.df.variable_id.unique()


# In[23]:


np.sort([int(foo[0:4]) for foo in searched_cat.df.time_range.unique() if foo != 'na'])


# In[28]:


# Create directory on gdata
datadir = '/scratch/xv83/TMIP/data'
start_time, end_time = time_window_strings(year_start, num_years)
start_time_str = start_time.strftime("%b%Y")
end_time_str = end_time.strftime("%b%Y")

outputdir = f'{datadir}/{model}/{experiment}/{ensemble}/{start_time_str}-{end_time_str}'
print(outputdir)


# In[29]:


makedirs(outputdir, exist_ok=True)


# In[30]:


########## Start the client and make the `.nc` files ##########
print("Starting client")
client = Client(n_workers=4)#, threads_per_worker=1, memory_limit='16GB') # Note: with 1thread/worker cannot plot thetao. Maybe I need to understand why?
client


# In[31]:


# umo dataset
print("Loading umo data")
umo_datadask = select_latest_data(searched_cat,
    dict(
        chunks={'i': 60, 'j': 60, 'time': -1, 'lev':50}
    ),
    variable_id = "umo",
    frequency = "mon",
)
print("\numo_datadask: ", umo_datadask)


# In[32]:


# vmo dataset
print("Loading vmo data")
vmo_datadask = select_latest_data(searched_cat,
    dict(
        chunks={'i': 60, 'j': 60, 'time': -1, 'lev':50}
    ),
    variable_id = "vmo",
    frequency = "mon",
)
print("\nvmo_datadask: ", vmo_datadask)


# In[33]:


# mlotst dataset
print("Loading mlotst data")
mlotst_datadask = select_latest_data(searched_cat,
    dict(
        chunks={'i': 60, 'j': 60, 'time': -1, 'lev':50}
    ),
    variable_id = "mlotst",
    frequency = "mon",
)
print("\nmlotst_datadask: ", mlotst_datadask)


# In[34]:


# Deal with thkcello for a different script,
# given that its location (fixed or time-dependent) depends on the model and/or project
# # thkcello dataset
# print("Loading thkcello data")
# thkcello_datadask = select_latest_data(searched_cat,
#     dict(
#         chunks={'i': 60, 'j': 60, 'time': -1, 'lev':50}
#     ),
#     variable_id = "thkcello",
#     frequency = "mon",
# )
# print("\nthkcello_datadask: ", thkcello_datadask)


# In[35]:


# Slice umo dataset for the time period
umo_datadask_sel = umo_datadask.sel(time=slice(start_time, end_time))
# Take the time average of the monthly evaporation (using month length as weights)
umo = umo_datadask_sel["umo"].weighted(umo_datadask_sel.time.dt.days_in_month).mean(dim="time")
umo


# In[36]:


# Slice vmo dataset for the time period
vmo_datadask_sel = vmo_datadask.sel(time=slice(start_time, end_time))
# Take the time average of the monthly evaporation (using month length as weights)
vmo = vmo_datadask_sel["vmo"].weighted(vmo_datadask_sel.time.dt.days_in_month).mean(dim="time")
vmo


# In[37]:


# Slice mlotst dataset for the time period
mlotst_datadask_sel = mlotst_datadask.sel(time=slice(start_time, end_time))
# Take the time mean of the yearly maximum of mlotst
mlotst_yearlymax = mlotst_datadask_sel.groupby("time.year").max(dim="time")
mlotst_yearlymax


# In[38]:


mlotst = mlotst_yearlymax.mean(dim="year")
mlotst


# In[39]:


# # Slice thkcello dataset for the time period
# thkcello_datadask_sel = thkcello_datadask.sel(time=slice(start_time, end_time))
# # Take the time average of the monthly evaporation (using month length as weights)
# thkcello = thkcello_datadask_sel["thkcello"].weighted(thkcello_datadask_sel.time.dt.days_in_month).mean(dim="time")


# In[40]:


# Save to netcdfs (and compute!)
umo.to_netcdf(f'{outputdir}/umo.nc', compute=True)
vmo.to_netcdf(f'{outputdir}/vmo.nc', compute=True)
mlotst.to_netcdf(f'{outputdir}/mlotst.nc', compute=True)
# thkcello.to_netcdf(f'{outputdir}/thkcello.nc', compute=True)


# In[41]:


client.close()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




