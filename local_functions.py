## import statements
import rasterio
import re
import numpy as np
import pandas as pd

## function to identify a list of unique dates with inputs: 1) filelist 2) regex pattern which subsets date from each filename
def id_unique_dates(file_list_in, date_regex_pattern, DBG=False, DBG_SUBS=False):
    if DBG: print('\ncall to id_unique_dates\n')

    uq_dates = list()

    for fl in file_list_in:
        if DBG_SUBS: print(f'\tworking on file: {fl}')

        match = date_regex_pattern.search(fl)
        if match:
            date_str = match.group(1)
            if DBG_SUBS: print(f'\t\tdate match found: {date_str}')
            if not date_str in uq_dates:
                if DBG_SUBS: print(f'\t\t\tdate not in uq_dates, adding')
                uq_dates.append(date_str)

    return uq_dates


## function to extract values of .tif file at coords (df), has features: 1) first subsets df to coords within bounds of .tif file, 2) removes NA values
def extract_tif_to_coords(tif_file_path, coords, return_df=False, var_name=None, DBG=False, DBG_SUBS=False):
    if DBG_SUBS: print(f"\ncall to extract_tif_to_coords\n")
    return_series = None

    with rasterio.open(tif_file_path) as src:

        ## get bounds of tif file
        bounds = src.bounds # Get the spatial extent of bounds file
        minLon = bounds[0]
        minLat = bounds[1]
        maxLon = bounds[2]
        maxLat = bounds[3]
        if DBG_SUBS: print(f'\tminLon: {minLon}, minLat: {minLat}, maxLon: {maxLon}, maxLat: {maxLat}')

        ## filter coords to within bounds -> coords_filt
        coords_filt = coords[(coords['latitude'] >= minLat) &
                            (coords['latitude'] <= maxLat) &
                            (coords['longitude'] >= minLon) &
                            (coords['longitude'] <= maxLon)].copy().reset_index(drop=True)

        ## handle case when no coords within bounds
        if len(coords_filt) == 0:
            print('\t\tno overlap, returning None')
            return return_series

        if DBG_SUBS: print(f'\tlen(coords_filt): {len(coords_filt)}')

        ## extract lat and lon from coords_filt, cast to np arrays
        lats_tmp = np.array(coords_filt['latitude'])
        lons_tmp = np.array(coords_filt['longitude'])

        ## read raster data and extract at points
        raster_data_tmp = src.read(1)
        transf_tmp = src.transform
        rows, cols = rasterio.transform.rowcol(transf_tmp, lons_tmp, lats_tmp)
        data_extr = raster_data_tmp[rows, cols]
        if DBG: print(f'data_extr.shape: {data_extr.shape}')

        return_series = pd.Series(data_extr)

    if return_df:
        df_to_return = pd.DataFrame({
            'longitude': coords_filt['longitude'],
            'latitude': coords_filt['latitude'],
            var_name: return_series
        })
        return df_to_return
    else:
        return return_series

## function to wrap around extract_tif_to_coords for a list of files - returns df
def extract_multiple_tifs(fold_path_in, file_list_in, coords_in, var_name_regex_pattern, dtime_str, dtime_in, DBG=True, DBG_SUBS=False):
    ## initialize
    if DBG_SUBS: print('\ncall to extract_multiple_tifs\n')
    df_return = pd.DataFrame()

    ## loop over file_list_in
    for idx, fl_tmp in enumerate(file_list_in):
        match = re.search(var_name_regex_pattern, fl_tmp) # match variable name
        if match: # if varname found in filename
            var_name_tmp = match.group(1)
            file_pth_tmp = fold_path_in + fl_tmp
            if DBG: print(f'\t\t\t\t\tworking on file: {fl_tmp}, var_name_tmp: {var_name_tmp}, path: {file_pth_tmp}')
            if idx == 0: # if first index, then create df_tmp from
                extract_result = extract_tif_to_coords(file_pth_tmp, coords_in, True, var_name_tmp)
                if extract_result is None:
                    print('\t\tbreaking...')
                    break
                df_return = extract_result
            else:
                extract_result = extract_tif_to_coords(file_pth_tmp, coords_in, return_df=False)
                if extract_result is None:
                    print('\t\tbreak')
                    break
                df_return[var_name_tmp] = extract_result
        else:
            print(f'\n\n!! no varname match for {fl_tmp} !!\n')
            continue

    ## add dtime column
    df_return['dtime_str'] = dtime_str
    df_return['dtime'] = dtime_in
    return df_return