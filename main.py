
## import statements
from local_functions import id_unique_dates, extract_multiple_tifs
from datetime import datetime

import gc
import os
import re
import sys
import rasterio
import gdal

import pandas as pd

print("Python version:", sys.version)
print("Rasterio version:", rasterio.__version__)
print("GDAL version:", gdal.VersionInfo())

## set DEBUG variables
DBG = True
DBG_SUBS = True
DBG_EXTRA = False

## set local path variables
subsample_pts_pth = 'F:/PFET/mid/subsample_pts.h5'
hls_path = 'F:/PFET/HLS/hlsl/'
results_path = 'F:/PFET/results/hlsl/'

## set regex pattern variables
hls_date_pattern = re.compile(r'_doy(\d+)_') # pattern to match datestring
hls_var_pattern = re.compile(r'_(\w+)_doy') # pattern to match variable name

# ## set problem dtimes
# problem_dtime_str = []

## subset subsample points to required columns
subsample_pts = pd.read_hdf(subsample_pts_pth, key='data')
extr_df = subsample_pts[['utmStrIDX', 'wgs84StrIDX', 'type', 'longitude', 'latitude']].copy().reset_index(drop=True)
del subsample_pts
gc.collect()

# list HLS folders
hls_folds = sorted(os.listdir(hls_path))

## main loop
for fold_tmp in hls_folds:
    ## initialize storage df and save path
    df_store = pd.DataFrame()
    save_pth_tmp = results_path + f'extr_{fold_tmp}.h5'
    if DBG: print(f'working on HLS folder: {fold_tmp}, save path: {save_pth_tmp}')

    ## list files and filter to .tif files
    fold_path = hls_path + fold_tmp + '/'
    hls_fls = os.listdir(fold_path) # list all files in directory
    hls_fls = [f for f in hls_fls if f.endswith('.tif')] # filter to .tif images

    ## get unique dates
    uq_dates = id_unique_dates(hls_fls, hls_date_pattern)
    len_uq_dates = len(uq_dates)
    if DBG: print(f'\tfound {len_uq_dates} unique date strings, head: {uq_dates[0:5]}')

    # loop over all unique dtime strings
    #
    #       for each:
    #           1) ID matching files
    #           2) loop over those files:
    #               A) ID variable
    #               B) extract data to our coords of interest
    for idy, dtime_str_tmp in enumerate(uq_dates):

        # if dtime_str_tmp in problem_dtime_str:
        #     if DBG: print('\n\t\tPROBLEM DTIME, CONTINUING\n\n')
        #     continue

        dtime_tmp = datetime.strptime(dtime_str_tmp, '%Y%j') # datetime string to python datetime
        if DBG: print(f'\n\tworking on {idy} of {len_uq_dates} with dtime_str: {dtime_str_tmp}, converted to dtime: {dtime_tmp}')

        ## subset to matching files
        match_str = '_doy' + dtime_str_tmp
        dtime_fls = sorted([f for f in hls_fls if match_str in f])
        if DBG: print(f'\t\tfound {len(dtime_fls)} matching files: {dtime_fls}')

        ## if >= 30 files present, need to divide into 2 areas
        if len(dtime_fls) >= 30:
            for area_str in ['_12N', '_13N']:
                dtime_fls_tmp = sorted([f for f in dtime_fls if area_str in f])
                if DBG: print(f'\t\t\tworking on area: {area_str}, with {len(dtime_fls_tmp)} files: {dtime_fls_tmp}')
                df_tmp = extract_multiple_tifs(fold_path, dtime_fls_tmp, extr_df, hls_var_pattern, dtime_str_tmp, dtime_tmp)
                if DBG: print(f'\t\t\t\tlen(df_tmp): {len(df_tmp)}')
                if df_tmp is None:
                    continue
                df_store = pd.concat([df_store, df_tmp], ignore_index=True)
                if DBG: print(f'\t\t\tnow len(df_store) = {len(df_store)}')
                df_store.reset_index(drop=True, inplace=True)
                del df_tmp
                gc.collect()
        else:
            df_tmp = extract_multiple_tifs(fold_path, dtime_fls, extr_df, hls_var_pattern, dtime_str_tmp, dtime_tmp)
            if df_tmp is None:
                continue
            if DBG: print(f'\t\t\t\tlen(df_tmp): {len(df_tmp)}')
            df_store = pd.concat([df_store, df_tmp], ignore_index=True)
            if DBG: print(f'\t\t\tnow len(df_store) = {len(df_store)}')
            df_store.reset_index(drop=True, inplace=True)
            del df_tmp
            gc.collect()

    ## wrap up
    if DBG: print('saving...')
    df_store.to_hdf(save_pth_tmp, key='data', mode='w')
    if DBG: print('\n\nDONE WITH YEAR\n\n')
    del df_store
    gc.collect()

## join HLSL
print("\n\n\n\nJoining individual DFs...")

hlsl_dfs = os.listdir(results_path)

hlsl_df_all = pd.DataFrame()
for fl in hlsl_dfs:
    hlss_df_all = pd.concat([hlsl_df_all, pd.read_hdf(results_path + fl, key='data')], ignore_index=True)

print(f"\tBEFORE FILTERING, len: {len(hlsl_df_all)}")

hlsl_df_filt = hlsl_df_all[hlsl_df_all['B01'] != -9999].copy().reset_index(drop=True)

hlsl_df_save_pth = 'F:/PFET/results/hlsl/hlsl_df.h5'
hlsl_df_filt.to_hdf(hlsl_df_save_pth, key='data', mode='w')

print(f"\tDONE JOINING, len: {len(hlsl_df_filt)}")


## join HLSS
# print("\n\n\n\nJoining individual DFs...")
#
# hlss_dfs = os.listdir(results_path)
#
# hlss_df_all = pd.DataFrame()
# for fl in hlss_dfs:
#     hlss_df_all = pd.concat([hlss_df_all, pd.read_hdf(results_path + fl, key='data')], ignore_index=True)
#
# print(f"\tBEFORE FILTERING, len: {len(hlss_df_all)}")
#
# hlss_df_filt = hlss_df_all[hlss_df_all['B01'] != -9999].copy().reset_index(drop=True)
#
# hlss_df_save_pth = 'F:/PFET/results/hlss_df.h5'
# hlss_df_filt.to_hdf(hlss_df_save_pth, key='data', mode='w')
#
# print(f"\tDONE JOINING, len: {len(hlss_df_filt)}")