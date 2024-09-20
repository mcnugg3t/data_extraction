
## import
import rasterio
import re
import os
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from local_functions import id_unique_dates, extract_multiple_tifs, extract_tif_to_coords

import pyarrow
import fastparquet

# get variables for lste002
tst_pth = "F:/PFET/ECOSTRESS/emissivity/lste002/2023_1"
tst_fls = os.listdir(tst_pth)
date_reg_tst = re.compile(r'_doy(\d+)_aid')
dates_tst = id_unique_dates(tst_fls, date_reg_tst, DBG=False, DBG_SUBS=False)
print(f'dates_tst 0: {dates_tst[0]}')
subs_str = '_doy' + dates_tst[0]
print(f'subs_str : {subs_str}')
tst_fls_subs = [fl for fl in tst_fls if subs_str in fl]
print(f'{tst_fls_subs}\n\n')


# test regex pattern
pattern = r'LSTE.002_(\w+)_doy'
string = 'LSTE.002_cloud_mask_doy'
match = re.search(pattern, string)
if match:
    print("Matched:", match.group(0))
    print("Captured group:", match.group(1))

# ## test extraction function
# print(f"import pyarrow version {pyarrow.__version__}")
#
# # test the performance of extract_tif_to_coords using subsample_pts
# subsample_pts_pth = 'F:/PFET/mid/eco_extr_points.parquet'
# subsample_pts = pd.read_parquet(subsample_pts_pth)
# print(subsample_pts.head())
#
# tst_tif_pth = "F:/PFET/ECOSTRESS/emissivity/lste001/2021_1/ECO2LSTE.001_SDS_Emis2_doy2021001091738_aid0001.tif"
# tst_extract = extract_tif_to_coords(tst_tif_pth, subsample_pts, return_df=True, var_name='Emis2', DBG=True, DBG_SUBS=True)
#
# tst_extract['geometry'] = tst_extract.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)
#
# tst_extract = tst_extract[tst_extract['Emis2'].notnull() & tst_extract['Emis2'] != 0].copy().reset_index(drop=True)
#
# tst_extract = tst_extract.sample(frac=0.1, random_state=42).copy().reset_index()
# tst_extract['Emis2'] = 0.49 + tst_extract['Emis2'] * 0.002
#
# print(f"len(tst_extract) after subsampling: {len(tst_extract)}")
#
# gdf = gpd.GeoDataFrame(tst_extract, geometry='geometry')
# gdf.set_crs(epsg=4326, inplace=True)
# gdf.to_file("F:/PFET/tst_extract/tst_extract.shp", driver='ESRI Shapefile')