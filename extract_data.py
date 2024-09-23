###NOTES###


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
dat_fold_path = 'F:/PFET/HLS/hlsl/'
results_path = 'F:/PFET/results/HLSL/'
save_pth_tmp = results_path + 'hlsl.parquet'

## set regex pattern variables and dtime format variable
date_reg_pattern = re.compile(r'_doy(\d+)_aid') # pattern to match date string
var_reg_pattern = re.compile(r'HLSL30.020_(\w+)_doy') # pattern to match variable name
dtime_format = '%Y%j' # format of date string in filenames
# dtime_format = '%Y%j%H%M%S' # format of date string in filenames

## filter out
filter_out = ['SZA', 'SAA', 'VAA', 'VZA']

## set check variable (e.g. Emis2) and minimum number of files (e.g. 2 for geolocation, 5 for LSTE, etc.)
CHECK_VAR = 'B01'
MIN_FILES = 11

# B01
# B02
# B03
# B04
# B05
# B06
# B07
# B09
# B10
# B11
# Fmask

## define save schema
SAVE_SCHEMA = pa.schema([
    ('utmStrIDX', pa.string()),
    ('longitude', pa.float64()),
    ('latitude', pa.float64()),
    # ('CloudMask', pa.int32()),
    # ('Cloud_final', pa.int32()),
    # ('Cloud_confidence', pa.int32()),
    #('view_azimuth', pa.float64()),
    #('view_zenith', pa.float64()),
    # ('cloud_mask', pa.int32()),
    # ('Emis2', pa.float64()),
    # ('Emis4', pa.float64()),
    # ('Emis5', pa.float64()),
    # ('QC', pa.int32()),
    # ('water_mask', pa.int32()),
    ('B01', pa.float64()),
    ('B02', pa.float64()),
    ('B03', pa.float64()),
    ('B04', pa.float64()),
    ('B05', pa.float64()),
    ('B06', pa.float64()),
    ('B07', pa.float64()),
    ('B09', pa.float64()),
    ('B10', pa.float64()),
    ('B11', pa.float64()),
    ('Fmask', pa.int32()),
    ('dtime_str', pa.string())
])

DF_TYPES = {
    'utmStrIDX': 'string',
    'longitude': 'float64',
    'latitude': 'float64',
    # 'CloudMask': 'int32',
    # 'Cloud_final': 'int32',
    # 'Cloud_confidence': 'int32',
    #'view_azimuth': 'float64',
    #'view_zenith': 'float64',
    # 'cloud_mask': 'int32',
    # 'Emis2': 'float64',
    # 'Emis4': 'float64',
    # 'Emis5': 'float64',
    # 'QC': 'int32',
    # 'water_mask': 'int32',
    'B01': 'float64',
    'B02': 'float64',
    'B03': 'float64',
    'B04': 'float64',
    'B05': 'float64',
    'B06': 'float64',
    'B07': 'float64',
    'B09': 'float64',
    'B10': 'float64',
    'B11': 'float64',
    'Fmask': 'int32',
    'dtime_str': 'string'
}

## read subsample points and subset to required columns
subsample_pts = pd.read_parquet(subsample_pts_pth)
extr_df = subsample_pts[['utmStrIDX', 'longitude', 'latitude']].copy().reset_index(drop=True)
del subsample_pts
gc.collect()

# list data folders
dat_folds = sorted(os.listdir(dat_fold_path))

# init parquet writer
writer = pq.ParquetWriter(save_pth_tmp, SAVE_SCHEMA)

## main loop - for each data folder...
for fold_tmp in dat_folds:
    ## initialize storage df and save path
    #df_store = pd.DataFrame()
    if DBG: print(f'working on data folder: {fold_tmp}, save path: {save_pth_tmp}')

    ## list files and filter to .tif files
    fold_path = dat_fold_path + fold_tmp + '/'
    folder_fls = os.listdir(fold_path) # list all files in directory
    folder_fls = [f for f in folder_fls if f.endswith('.tif')] # filter to .tif images
    folder_fls = [f for f in folder_fls if not any(kw in f for kw in filter_out)] # filter out keywords

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
        ## convert string to datetime to check month
        dtime_tmp = datetime.strptime(dtime_str_tmp, dtime_format)

        if dtime_tmp.month not in [3, 4, 5, 6, 7, 8, 9, 10]:
             if DBG: print('\n\t\tDTIME OUT OF MONTH BOUNDS, CONTINUING\n\n')
             continue

        #dtime_tmp = datetime.strptime(dtime_str_tmp, dtime_format) # datetime string to python datetime
        if DBG: print(f'\n\tworking on {idy} of {len_uq_dates} with dtime_str: {dtime_str_tmp}, dtime_tmp: {dtime_tmp}')

        ## subset to matching files
        match_str = '_doy' + dtime_str_tmp ## CHECK !!
        dtime_fls = sorted([f for f in folder_fls if match_str in f])
        if DBG: print(f'\t\tfound {len(dtime_fls)} matching files: {dtime_fls}')

        ## if >= 22 files present, need to divide into 2 areas (only applicable to HLS data)
        if len(dtime_fls) >= 22:
            for area_str in ['_12N', '_13N']:
                dtime_fls_tmp = sorted([f for f in dtime_fls if area_str in f])
                if DBG: print(f'\t\t\tworking on area: {area_str}, with {len(dtime_fls_tmp)} files: {dtime_fls_tmp}')
                # if less than MIN_FILES files, continue (incomplete set of files for dtime_str_tmp)
                if len(dtime_fls_tmp) < MIN_FILES:
                    continue

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
            # if less than MIN_FILES files, continue (incomplete set of files for dtime_str_tmp)
            if len(dtime_fls) < MIN_FILES:
                continue

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
    if DBG: print('\n\nDONE WITH FOLDER, CONTINUING...\n\n')

## wrap up
writer.close()
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