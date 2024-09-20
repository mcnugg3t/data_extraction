###NOTES###
# check that variable names remain consistent across ECOSTRESS v001 and v002
# check line `dtime_tmp = ...`
# wrap joining of individual df's in a function - callable with the results folder path
# review behavior of extract_multiple_tifs
###########

## import statements
from local_functions import id_unique_dates, extract_multiple_tifs
from datetime import datetime

import gc
import os
import re
import sys
import rasterio
#import gdal
import pyarrow as pa
import pyarrow.parquet as pq
#import fastparquet

import pandas as pd

print("Python version:", sys.version)
print("Rasterio version:", rasterio.__version__)
#print("GDAL version:", gdal.VersionInfo())

## set DEBUG variables
DBG = True
DBG_SUBS = True
DBG_EXTRA = True

## set local path variables
subsample_pts_pth = 'F:/PFET/mid/eco_extr_points.parquet'
dat_fold_path = 'F:/PFET/ECOSTRESS/emissivity/lste002/'
results_path = 'F:/PFET/results/emissivity/lste002/'

## set regex pattern variables and dtime format variable
date_reg_pattern = re.compile(r'_doy(\d+)_aid') # pattern to match date string
var_reg_pattern = re.compile(r'LSTE.002_(\w+)_doy') # pattern to match variable name
dtime_format = '%Y%j%H%M%S' # format of date string in filenames

## set check variable (e.g. Emis2)
CHECK_VAR = 'Emis2'

## define save schema
SAVE_SCHEMA = pa.schema([
    ('utmStrIDX', pa.string()),
    ('longitude', pa.float64()),
    ('latitude', pa.float64()),
    ('cloud_mask', pa.int32()),
    ('Emis2', pa.float64()),
    ('Emis4', pa.float64()),
    ('Emis5', pa.float64()),
    ('QC', pa.int32()),
    ('water_mask', pa.int32()),
    ('dtime_str', pa.string())
])

DF_TYPES = {
    'utmStrIDX': 'string',
    'longitude': 'float64',
    'latitude': 'float64',
    'cloud_mask': 'int32',
    'Emis2': 'float64',
    'Emis4': 'float64',
    'Emis5': 'float64',
    'QC': 'int32',
    'water_mask': 'int32',
    'dtime_str': 'string'
}

## read subsample points and subset to required columns
subsample_pts = pd.read_parquet(subsample_pts_pth)
extr_df = subsample_pts[['utmStrIDX', 'longitude', 'latitude']].copy().reset_index(drop=True)
del subsample_pts
gc.collect()

# list data folders
dat_folds = sorted(os.listdir(dat_fold_path))

## main loop - for each data folder...
for fold_tmp in dat_folds:
    ## initialize storage df and save path
    df_store = pd.DataFrame()
    save_pth_tmp = results_path + f'extr_{fold_tmp}.parquet'
    writer = pq.ParquetWriter(save_pth_tmp, SAVE_SCHEMA)
    if DBG: print(f'working on ecostress folder: {fold_tmp}, save path: {save_pth_tmp}')

    ## list files and filter to .tif files
    fold_path = dat_fold_path + fold_tmp + '/'
    folder_fls = os.listdir(fold_path) # list all files in directory
    folder_fls = [f for f in folder_fls if f.endswith('.tif')] # filter to .tif images

    ## get unique dates
    uq_dates = id_unique_dates(folder_fls, date_reg_pattern)
    len_uq_dates = len(uq_dates)
    if DBG: print(f'\tfound {len_uq_dates} unique date strings, head: {uq_dates[0:5]}')

    # loop over all unique dtime strings
    #       for each:
    #           1) ID matching files
    #           2) loop over those files:
    #               A) ID variable
    #               B) extract data to our coords of interest
    for idy, dtime_str_tmp in enumerate(uq_dates):
        # if dtime_str_tmp in problem_dtime_str:
        #     if DBG: print('\n\t\tPROBLEM DTIME, CONTINUING\n\n')
        #     continue

        #dtime_tmp = datetime.strptime(dtime_str_tmp, dtime_format) # datetime string to python datetime
        if DBG: print(f'\n\tworking on {idy} of {len_uq_dates} with dtime_str: {dtime_str_tmp}')

        ## subset to matching files
        match_str = '_doy' + dtime_str_tmp ## CHECK !!
        dtime_fls = sorted([f for f in folder_fls if match_str in f])
        if DBG: print(f'\t\tfound {len(dtime_fls)} matching files: {dtime_fls}')

        # if less than 5 files, continue (incomplete set)
        if len(dtime_fls) < 5:
            continue
        ## if >= 30 files present, need to divide into 2 areas (only applicable to HLS data)
        elif len(dtime_fls) >= 30:
            for area_str in ['_12N', '_13N']:
                dtime_fls_tmp = sorted([f for f in dtime_fls if area_str in f])
                if DBG: print(f'\t\t\tworking on area: {area_str}, with {len(dtime_fls_tmp)} files: {dtime_fls_tmp}')
                df_tmp = extract_multiple_tifs(fold_path, dtime_fls_tmp, extr_df, var_reg_pattern, dtime_str_tmp)


                if ((df_tmp is None) | (len(df_tmp) == 0)):
                    if DBG: print('\t\t\tdf_tmp is null/empty, continuing...')
                    continue

                if DBG: print(f'\t\t\t\tlen(df_tmp): {len(df_tmp)}')

                # if df_tmp has content
                if ((df_tmp is not None) & (len(df_tmp) > 0)):
                    # filter df_tmp to meaningful rows
                    df_tmp = df_tmp[df_tmp[CHECK_VAR].notnull() & df_tmp[CHECK_VAR] != 0].copy().reset_index(drop=True)
                    # debug print after filtering
                    if DBG_EXTRA:
                        print(df_tmp.info())
                    # check types
                    df_tmp = df_tmp.astype(DF_TYPES)
                    # create table for writing
                    table_write = pa.Table.from_pandas(df_tmp, schema=SAVE_SCHEMA, preserve_index=False)
                    # write to save file
                    if DBG: print(f'\t\t\twriting df_tmp to save file')
                    writer.write_table(table_write)
                    if DBG: print(f'\t\t\tdone writing...')

                del df_tmp
                gc.collect()
        # otherwise (normal case)
        else:
            df_tmp = extract_multiple_tifs(fold_path, dtime_fls, extr_df, var_reg_pattern, dtime_str_tmp)

            if ( (df_tmp is None) | (len(df_tmp) == 0) ):
                if DBG: print('\t\t\tdf_tmp is null/empty, continuing...')
                continue

            if ((df_tmp is not None) & (len(df_tmp) > 0)):
                # filter df_tmp to meaningful rows
                df_tmp = df_tmp[df_tmp[CHECK_VAR].notnull() & df_tmp[CHECK_VAR] != 0].copy().reset_index(drop=True)
                # debug print after filtering
                if DBG_EXTRA:
                    print(df_tmp.info())
                # check types
                df_tmp = df_tmp.astype(DF_TYPES)
                # create table for writing
                table_write = pa.Table.from_pandas(df_tmp, schema=SAVE_SCHEMA, preserve_index=False)
                # write to save file
                if DBG: print(f'\t\t\twriting df_tmp to save file')
                writer.write_table(table_write)
                if DBG: print(f'\t\t\tdone writing...')

            del df_tmp
            gc.collect()

    ## wrap up
    writer.close()
    if DBG: print('\n\nDONE WITH FOLDER, CONTINUING...\n\n')
    gc.collect()

## join individual result dfs into one df for each dataset 

#print("\n\n\n\nJoining individual DFs...")

#hlss_dfs = os.listdir(results_path)

#hlss_df_all = pd.DataFrame()
#for fl in hlss_dfs:
#    hlss_df_all = pd.concat([hlss_df_all, pd.read_hdf(results_path + fl, key='data')], ignore_index=True)

#print(f"\tBEFORE FILTERING, len: {len(hlss_df_all)}")

#hlss_df_filt = hlss_df_all[hlss_df_all['B01'] != -9999].copy().reset_index(drop=True)

#hlss_df_save_pth = 'F:/PFET/results/hlss_df.h5'
#hlss_df_filt.to_hdf(hlss_df_save_pth, key='data', mode='w')

#print(f"\tDONE JOINING, len: {len(hlss_df_filt)}")