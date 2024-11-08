

# import sys to access script arguments (experiment, ensemble, first_year, last_year)
import sys

# interactive use only
model="ACCESS-ESM1-5"
experiment="historical"
# year_start = 1850
year_start = 1940
# year_end = 2015
year_end = 1950


# Model etc. defined from script input
model = sys.argv[1]
print("Model: ", model, " (type: ", type(model), ")")
experiment = sys.argv[2]
print("Experiment: ", experiment, " (type: ", type(experiment), ")")
members = sys.argv[3].split(',')
print("members: ", members, " (type: ", type(members), ")")
year_start = int(sys.argv[4])
print("year_start: ", year_start, " (type: ", type(year_start), ")")
year_end = int(sys.argv[5])
print("year_end: ", year_end, " (type: ", type(year_end), ")")

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





# Create directory on scratch to save the data
scratchdatadir = '/scratch/xv83/TMIP/data'
gdatadatadir = '/g/data/xv83/TMIP/data'

# members = ["HI-05", "HI-06", "HI-07", "HI-08"]
# members = ["HI-09", "HI-10", "HI-11", "HI-12"]
# members = ["HI-05"]



print("Starting client")

# This `if` statement is required in scripts (not required in Jupyter)
if __name__ == '__main__':
    client = Client(n_workers=24, threads_per_worker=1)
    #, threads_per_worker=1, memory_limit='16GB') # Note: with 1thread/worker cannot plot thetao. Maybe I need to understand why?
    # added threads_per_worker=1 back again because I possibly hitting some random unsafe multithreading issue:
    # https://forum.access-hive.org.au/t/netcdf-not-a-valid-id-errors/389


    for member in members:

        # print ensemble/member
        inputdir = f'/scratch/p66/pbd562/petrichor/get/{member}/history/ocn'
        outputdir = f'{gdatadatadir}/{model}/{member}'
        print(f"\n{member}")

        for decade in range(year_start, year_end, 10):

            print(f'  {decade}s')

            # subset of the files required
            # paths = [f'{inputdir}/history/ocn/ocean_month.nc-{year}1231' for year in range(year_start, year_end)]
            # paths = [f'{inputdir}/history/ocn/ocean_month.nc-{year}1231' for year in range(1990, 2000)]
            if decade == 2010:
                num_years = 5
            else:
                num_years = 10

            paths = [f'{inputdir}/ocean_month.nc-{year}1231' for year in range(decade, decade + num_years)]

            # Exit early if cannot find all files
            if not all([os.path.isfile(fname) for fname in paths]):
                print(f'Missing files for {member} {decade}-{decade + num_years}')
                continue

            # load the data
            try:
                ds = xr.open_mfdataset(
                    paths,
                    chunks={'time':-1, 'st_ocean':-1},
                    concat_dim="time",
                    compat='override',
                    preprocess=None,
                    # preprocess=combined_preprocessing,
                    engine='netcdf4',
                    data_vars='minimal',
                    coords='minimal',
                    combine='nested',
                    parallel=True,
                    join='outer',
                    attrs_file=None,
                    combine_attrs='override',
                    drop_variables=[ # Drop all the variables I don't need. Can I use a `keep_variables` instead?
                        'ht', 'hu', 'kmt', 'kmu', 'pbot_t', 'patm_t', 'sea_level', 'sea_level_sq',
                        'rho', 'rho_dzt', 'dht', 'pot_temp', 'sst', 'sst_sq', 'salt', 'sss', 'pot_rho_0',
                        'age_global', 'psiu', 'mld', 'mld_max', 'mld_min', 'mld_sq', 'hblt_max',
                        'u', 'v', 'tz_trans', 'tz_trans_sq', 'tx_trans_rho', 'ty_trans_rho',
                        'tx_trans_rho_gm', 'ty_trans_rho_gm', 'temp_xflux_ndiffuse_int_z', 'temp_yflux_ndiffuse_int_z',
                        'temp_xflux_sigma', 'temp_yflux_sigma', 'temp_yflux_submeso_int_z', 'temp_xflux_submeso_int_z',
                        'temp_merid_flux_advect_global', 'temp_merid_flux_over_global', 'temp_merid_flux_gyre_global',
                        'salt_merid_flux_advect_global', 'salt_merid_flux_over_global', 'salt_merid_flux_gyre_global',
                        'temp_merid_flux_advect_southern', 'temp_merid_flux_over_southern', 'temp_merid_flux_gyre_southern',
                        'salt_merid_flux_advect_southern', 'salt_merid_flux_over_southern', 'salt_merid_flux_gyre_southern',
                        'temp_merid_flux_advect_atlantic', 'temp_merid_flux_over_atlantic', 'temp_merid_flux_gyre_atlantic',
                        'salt_merid_flux_advect_atlantic', 'salt_merid_flux_over_atlantic', 'salt_merid_flux_gyre_atlantic',
                        'temp_merid_flux_advect_pacific', 'temp_merid_flux_over_pacific', 'temp_merid_flux_gyre_pacific',
                        'salt_merid_flux_advect_pacific', 'salt_merid_flux_over_pacific', 'salt_merid_flux_gyre_pacific',
                        'temp_merid_flux_advect_arctic', 'temp_merid_flux_over_arctic', 'temp_merid_flux_gyre_arctic',
                        'salt_merid_flux_advect_arctic', 'salt_merid_flux_over_arctic', 'salt_merid_flux_gyre_arctic',
                        'temp_merid_flux_advect_indian', 'temp_merid_flux_over_indian', 'temp_merid_flux_gyre_indian',
                        'salt_merid_flux_advect_indian', 'salt_merid_flux_over_indian', 'salt_merid_flux_gyre_indian',
                        'lprec', 'fprec', 'evap', 'runoff', 'ice_calving', 'melt', 'pme_river', 'sfc_salt_flux_ice',
                        'sfc_salt_flux_runoff', 'sfc_hflux_from_water_prec', 'sfc_hflux_from_water_evap',
                        'sfc_hflux_from_runoff', 'sfc_hflux_from_calving', 'fprec_melt_heat', 'calving_melt_heat',
                        'lw_heat', 'evap_heat', 'sens_heat', 'swflx', 'sw_heat', 'tau_x', 'tau_y', 'frazil_2d',
                        'pme', 'pme_mass', 'river', 'swflx_vis', 'sw_frac', 'sfc_hflux_coupler', 'sfc_hflux_pme',
                        'tau_curl', 'ekman_we', 'salt_calvingmix', 'temp_calvingmix', 'temp_runoffmix', 'temp_rivermix',
                        'temp_runoff', 'temp_calving', 'wfimelt', 'wfiform', 'pbot0', 'anompb', 'eta_t', 'eta_u',
                        'rhobarz', 'conv_rho_ud_t', 'urhod', 'vrhod', 'u_surf', 'v_surf', 'u_bott', 'v_bott', 'bottom_temp',
                        'bottom_salt', 'bottom_age_global', 'temp', 'temp_xflux_adv', 'temp_yflux_adv', 'temp_zflux_adv',
                        'salt_xflux_adv', 'salt_yflux_adv', 'salt_zflux_adv', 'temp_vdiffuse_impl', 'salt_vdiffuse_impl',
                        'temp_tendency', 'salt_tendency', 'temp_tendency_expl', 'salt_tendency_expl', 'temp_submeso',
                        'salt_submeso', 'neutral_rho', 'pot_rho_2', 'potrho_mix_base', 'potrho_mix_depth', 'press',
                        'wt', 'wrhot', 'drhodtheta', 'drhodsalinity', 'cabbeling', 'thermobaricity',
                        'salt_xflux_ndiffuse_int_z', 'salt_yflux_ndiffuse_int_z', 'psiv', 'temp_sigma',
                        'mixdownslope_temp', 'mixdownslope_salt', 'eddy_depth', 'agm_grid_scaling',
                        'bv_freq', 'rossby', 'rossby_radius', 'buoy_freq_ave_submeso', 'hblt_submeso',
                        'viscosity_scaling', 'visc_crit_bih', 'lap_fric_u', 'lap_fric_v', 'diff_cbt_wave',
                        'diff_cbt_drag', 'bvfreq_bottom', 'mix_efficiency', 'power_waves', 'power_diss_wave',
                        'power_diss_drag', 'energy_flux', 'diff_cbt_kpp_t', 'diff_cbt_kpp_s', 'tide_speed_wave',
                        'tide_speed_drag', 'tide_speed_mask', 'roughness_length', 'roughness_amp', 'langmuirfactor',
                        'bmf_u', 'bmf_v', 'bottom_power_u', 'bottom_power_v', 'wind_power_u', 'wind_power_v',
                        'average_T1', 'average_T2', 'average_DT', 'nv', 'potrho', 'potrho_edges'
                    ]
                )
            except Exception:
                print(f'Error processing {model} {member} data')
                print(traceback.format_exc())
                continue


            # tx_trans_gm
            try:
                tx_trans_gm = ds["tx_trans_gm"].chunk({'time':12})
                tx_trans_gm2 = xr.open_dataset(f'{outputdir}/month_tx_trans_gm_{decade}s.nc').tx_trans_gm.chunk({'time':12})
            except Exception:
                print(f'Error processing {model} {member} {decade}s tx_trans_gm')
                print(traceback.format_exc())

            if not tx_trans_gm.equals(tx_trans_gm2):
                print(f'    tx_trans_gm does not match')

            # ty_trans_gm
            try:
                ty_trans_gm = ds["ty_trans_gm"].chunk({'time':12})
                ty_trans_gm2 = xr.open_dataset(f'{outputdir}/month_ty_trans_gm_{decade}s.nc').ty_trans_gm.chunk({'time':12})
            except Exception:
                print(f'Error processing {model} {member} {decade}s ty_trans_gm')
                print(traceback.format_exc())

            if not ty_trans_gm.equals(ty_trans_gm2):
                print(f'    ty_trans_gm does not match')

            # tx_trans_submeso
            try:
                tx_trans_submeso = ds["tx_trans_submeso"].chunk({'time':12})
                tx_trans_submeso2 = xr.open_dataset(f'{outputdir}/month_tx_trans_submeso_{decade}s.nc').tx_trans_submeso.chunk({'time':12})
            except Exception:
                print(f'Error processing {model} {member} {decade}s tx_trans_submeso')
                print(traceback.format_exc())

            if not tx_trans_submeso.equals(tx_trans_submeso2):
                print(f'    tx_trans_submeso does not match')

            # ty_trans_submeso
            try:
                ty_trans_submeso = ds["ty_trans_submeso"].chunk({'time':12})
                ty_trans_submeso2 = xr.open_dataset(f'{outputdir}/month_ty_trans_submeso_{decade}s.nc').ty_trans_submeso.chunk({'time':12})
            except Exception:
                print(f'Error processing {model} {member} {decade}s ty_trans_submeso')
                print(traceback.format_exc())

            if not ty_trans_submeso.equals(ty_trans_submeso2):
                print(f'    ty_trans_submeso does not match')



    client.close()




